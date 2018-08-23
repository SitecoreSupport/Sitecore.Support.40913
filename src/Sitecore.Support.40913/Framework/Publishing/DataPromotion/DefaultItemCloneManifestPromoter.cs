using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Sitecore.Framework.Publishing.DataPromotion;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.Locators;
using Sitecore.Framework.Publishing.Manifest;

namespace Sitecore.Support.Framework.Publishing.DataPromotion
{
  public class DefaultItemCloneManifestPromoter : Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter
  {
    public DefaultItemCloneManifestPromoter(Microsoft.Extensions.Logging.ILogger<Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter> logger, Sitecore.Framework.Publishing.DataPromotion.PromoterOptions options = null) : base(logger, options)
    {
    }

    public DefaultItemCloneManifestPromoter(Microsoft.Extensions.Logging.ILogger<Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter> logger, Microsoft.Extensions.Configuration.IConfiguration config) : base(logger, config)
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
      var options = (PromoterOptions)(typeof(Sitecore.Support.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter).BaseType.GetField("_options", BindingFlags.Instance | BindingFlags.NonPublic)).GetValue(this);
      var comparer = (IItemVariantIdentifierComparer)(typeof(Sitecore.Support.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter).BaseType.GetField("VariantIdentifierComparer", BindingFlags.Static | BindingFlags.NonPublic)).GetValue(null);

      await base.Promote(async () =>
      {
        var itemWorker = CreatePromoteWorker(manifestRepository, targetItemRepository, targetContext.Manifest.ManifestId, targetContext.CalculateResults, fieldsToReport);

        await ProcessManifestInBatches(
            manifestRepository,
            targetContext.Manifest.ManifestId,
            ManifestStepAction.PromoteCloneVariant,
            async (ItemVariantLocator[] batchUris) =>
            {
              return await DecloneVariants(targetContext, sourceItemRepository, relationshipRepository, batchUris).ConfigureAwait(false);
            },
            async declonedData =>
            {
              await Task.WhenAll(
                          itemWorker.SaveVariants(declonedData.Select(d => d.Item1).ToArray()),
                          relationshipRepository.Save(targetContext.TargetStore.ScDatabaseName, declonedData.ToDictionary(d => (IItemVariantIdentifier)d.Item1, d => (IReadOnlyCollection<IItemRelationship>)d.Item2, comparer)));
            },
            options.BatchSize,
            cancelTokenSource);
      },
      cancelTokenSource);
    }
  }
}