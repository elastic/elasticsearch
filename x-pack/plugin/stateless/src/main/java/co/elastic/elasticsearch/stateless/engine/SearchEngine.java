package co.elastic.elasticsearch.stateless.engine;

import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;

public class SearchEngine extends InternalEngine {

    public SearchEngine(EngineConfig config) {
        // override the EngineConfig to nullify the IndexCommitListener so that replica do not upload files to the object store
        super(
            new EngineConfig(
                config.getShardId(),
                config.getThreadPool(),
                config.getIndexSettings(),
                config.getWarmer(),
                config.getStore(),
                config.getMergePolicy(),
                config.getAnalyzer(),
                config.getSimilarity(),
                config.getCodecService(),
                config.getEventListener(),
                config.getQueryCache(),
                config.getQueryCachingPolicy(),
                config.getTranslogConfig(),
                config.getFlushMergesAfter(),
                config.getExternalRefreshListener(),
                config.getInternalRefreshListener(),
                config.getIndexSort(),
                config.getCircuitBreakerService(),
                config.getGlobalCheckpointSupplier(),
                config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(),
                config.getSnapshotCommitSupplier(),
                config.getLeafSorter(),
                config.getRelativeTimeInNanosSupplier(),
                null, // here
                false
            )
        );
        assert config.isRecoveringAsPrimary() == false;
    }
}
