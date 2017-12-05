using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Publishing.DataPromotion;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.Locators;
using Sitecore.Framework.Publishing.Manifest;

namespace Sitecore.Support.Framework.Publishing.DataPromotion
{
  public class
    DefaultItemCloneManifestPromoter : Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter
  {
    private static readonly IItemVariantIdentifierComparer VariantIdentifierComparer =
      new IItemVariantIdentifierComparer();

    private readonly PromoterOptions _options;

    public DefaultItemCloneManifestPromoter(
      ILogger<DefaultItemCloneManifestPromoter> logger,
      PromoterOptions options = null) : base(logger, options)
    {
      _options = options ?? new PromoterOptions();
    }

    public DefaultItemCloneManifestPromoter(
      ILogger<DefaultItemCloneManifestPromoter> logger,
      IConfiguration config) : base(logger, config)
    {
    }

    public override async Task Promote(
      TargetPromoteContext targetContext,
      IManifestRepository manifestRepository,
      IItemReadRepository sourceItemRepository,
      IItemRelationshipRepository relationshipRepository,
      IItemWriteRepository targetItemRepository,
      FieldReportSpecification fieldsToReport,
      CancellationTokenSource cancelTokenSource)
    {
      await base.Promote(async () =>
        {
          var itemWorker = CreatePromoteWorker(manifestRepository, targetItemRepository,
            targetContext.Manifest.ManifestId, targetContext.CalculateResults, fieldsToReport);

          await ProcessManifestInBatches(
            manifestRepository,
            targetContext.Manifest.ManifestId,
            ManifestStepAction.PromoteCloneVariant,
            async (ItemVariantLocator[] batchUris) =>
            {
              return await DecloneVariants(targetContext, sourceItemRepository, relationshipRepository, batchUris)
                .ConfigureAwait(false);
            },
            async declonedData =>
            {
              if (!declonedData.Any()) return;

              await Task.WhenAll(
                itemWorker.SaveVariants(declonedData.Select(d => d.Item1).ToArray()),
                relationshipRepository.Save(targetContext.TargetStore.Name,
                  declonedData.ToDictionary(d => (IItemVariantIdentifier) d.Item1,
                    d => (IReadOnlyCollection<IItemRelationship>) d.Item2)));
            },
            _options.BatchSize,
            cancelTokenSource);
        },
        cancelTokenSource);
    }

    /// <summary>
    ///   processes the locators to build the decloned variants
    /// </summary>
    protected override async Task<IEnumerable<Tuple<IItemVariant, IItemRelationship[]>>> DecloneVariants(
      TargetPromoteContext targetContext,
      IItemReadRepository itemRepository,
      IItemRelationshipRepository relationshipRepository,
      IItemVariantLocator[] cloneLocators)
    {
      // get the clones..
      var cloneVariantsTask = itemRepository.GetVariants(cloneLocators);
      var cloneRelationshipsTask =
        relationshipRepository.GetOutRelationships(targetContext.TargetStore.Name, cloneLocators);
      await Task.WhenAll(cloneVariantsTask, cloneRelationshipsTask).ConfigureAwait(false);

      var cloneVariants = cloneVariantsTask.Result
        .Select(v =>
        {
          IItemVariantLocator cloneSourceUri;
          v.TryGetCloneSourceVariantUri("not_important",
            out cloneSourceUri); // we don't care about the store - it will be the same
          return new
          {
            clone = v,
            cloneSourceIdentifier = cloneSourceUri
          };
        })
        .ToArray();

      var cloneRelationships = cloneRelationshipsTask.Result;

      // get the clone sources..
      var cloneSourceLocators = cloneVariants
        .Select(v => v.cloneSourceIdentifier)
        .Where(i => i != null)
        .Distinct(VariantIdentifierComparer)
        .ToArray();

      var cloneSourceVariantsTask = itemRepository.GetVariants(cloneSourceLocators);
      var cloneSourceRelationshipsTask =
        relationshipRepository.GetOutRelationships(targetContext.TargetStore.Name, cloneSourceLocators);
      await Task.WhenAll(cloneSourceVariantsTask, cloneSourceRelationshipsTask).ConfigureAwait(false);

      var cloneSourceVariants =
        cloneSourceVariantsTask.Result.ToDictionary(x => (IItemVariantIdentifier)x, x => x, VariantIdentifierComparer);
      var cloneSourceRelationships = cloneSourceRelationshipsTask.Result;

      return cloneVariants
        .Select(cloneVariantEntry =>
        {
          IItemVariant sourceVariant = null;
          if (cloneVariantEntry.cloneSourceIdentifier != null &&
              cloneSourceVariants.TryGetValue(cloneVariantEntry.cloneSourceIdentifier, out sourceVariant))
          {
            IReadOnlyCollection<IItemRelationship> cloneRels;
            if (!cloneRelationships.TryGetValue(cloneVariantEntry.clone, out cloneRels))
              cloneRels = new IItemRelationship[0];

            IReadOnlyCollection<IItemRelationship> sourceRels;
            if (!cloneSourceRelationships.TryGetValue(cloneVariantEntry.cloneSourceIdentifier, out sourceRels))
              sourceRels = new IItemRelationship[0];

            // decloneCloneFields
            return MergeCloneAndSourceVariants(
              cloneVariantEntry.clone,
              cloneRels,
              sourceVariant,
              sourceRels);
          }

          return null;
        })
        .Where(x => x != null)
        .ToArray();
    }

    protected virtual Tuple<IItemVariant, IItemRelationship[]> MergeCloneAndSourceVariants(
      IItemVariant cloneVariant,
      IEnumerable<IItemRelationship> cloneRelationships,
      IItemVariant sourceVariant,
      IEnumerable<IItemRelationship> sourceRelationships)
    {
      // item data
      var itemData = cloneVariant.Properties;

      // fields data
      var fieldsData = cloneVariant.Fields
        // remove the clone (source) fields
        .Where(x => x.FieldId != PublishingConstants.Clones.SourceItem &&
                    x.FieldId != PublishingConstants.Clones.SourceVariant)
        .Concat(
          // only use the fields from source if they don't exist in the clone
          sourceVariant.Fields
            .Where(x => cloneVariant.Fields.All(c => c.FieldId != x.FieldId))
            .Select(x => new FieldData(
              x.FieldId,
              cloneVariant.Id,
              x.RawValue,
              CreateFieldVarianceInfo(cloneVariant, x))))
        .ToArray();

      // links data
      var linksFromClone = cloneRelationships
        .Where(cloneRel =>
          // this is a special relationship that captures the link between an item and its template
          // this needs to be processed once .. hence it's being taken from the source and skiped from the clone (both must have the same template id)
            cloneRel.Type != ItemRelationshipType.TemplatedBy &&
            !cloneVariant.Fields.Any() ||
            cloneRel.SourceFieldId == null ||
            cloneVariant.Fields.Any(c => c.FieldId == cloneRel.SourceFieldId))
        .ToArray();

      var linksFromSource = sourceRelationships
        .Select(x => new ItemRelationship(
          Guid.NewGuid(), // generate a new id to avoid database unique key constraint as all items from all targets are saved in the same links table!
          x.SourceId == sourceVariant.Id ? cloneVariant.Id : x.SourceId, // replace the sourceId with the cloned item id
          x.TargetId,
          x.Type,
          x.SourceVariance,
          x.TargetVariance, x.TargetPath, x.SourceFieldId))
        .ToArray();

      // use the links from clone + any additional links from the source
      var linksData = linksFromClone
        .Concat(linksFromSource.Where(x =>
          !linksFromClone.Any(s => s.SourceId == x.SourceId &&
                                   s.SourceFieldId == x.SourceFieldId &&
                                   x.Type == s.Type)))
        .ToArray();

      return new Tuple<IItemVariant, IItemRelationship[]>(
        new ItemVariant(
          cloneVariant.Id,
          cloneVariant.Language,
          cloneVariant.Version,
          cloneVariant.Revision,
          itemData,
          fieldsData),
        linksData);
    }
  }
}