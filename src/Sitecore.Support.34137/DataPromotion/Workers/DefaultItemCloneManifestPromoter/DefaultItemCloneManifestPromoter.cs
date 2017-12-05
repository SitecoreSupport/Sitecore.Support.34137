using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Publishing.DataPromotion;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.Locators;

namespace Sitecore.Support.Framework.Publishing.DataPromotion
{
  public class
    DefaultItemCloneManifestPromoter : Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter
  {
    private static readonly IItemVariantIdentifierComparer VariantIdentifierComparer =
      new IItemVariantIdentifierComparer();

    public DefaultItemCloneManifestPromoter(
      ILogger<DefaultItemCloneManifestPromoter> logger,
      PromoterOptions options = null) : base(logger, options)
    {
    }

    public DefaultItemCloneManifestPromoter(
      ILogger<DefaultItemCloneManifestPromoter> logger,
      IConfiguration config) : base(logger, config)
    {
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
  }
}