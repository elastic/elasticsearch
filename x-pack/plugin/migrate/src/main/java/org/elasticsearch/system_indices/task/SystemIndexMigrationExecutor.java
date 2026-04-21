public class SystemIndexMigrationExecutor extends TransportAction<SystemIndexMigrationTaskParams, SystemIndexMigrationTaskParams> {
    private final Client client;
    private final ClusterService clusterService;
    private final SystemIndices systemIndices;
    private final IndexScopedSettings indexScopedSettings;
    private final ThreadPool threadPool;
    private final ProjectResolver projectResolver;

    public SystemIndexMigrationExecutor(
        Client client,
        ClusterService clusterService,
        SystemIndices systemIndices,
        IndexScopedSettings indexScopedSettings,
        ThreadPool threadPool
    ) {
        super(SystemIndexMigrationTaskParams.ACTION_NAME, threadPool.generic());
        this.client = client;
        this.clusterService = clusterService;
        this.systemIndices = systemIndices;
        this.indexScopedSettings = indexScopedSettings;
        this.threadPool = threadPool;
        this.projectResolver = projectResolver;
    }

    @Override
    public SystemIndexMigrationTaskParams newResponseInstance() {
        return new SystemIndexMigrationTaskParams();
    }

    @Override
    public SystemIndexMigrationTaskParams executor() {
        // implementation
    }
}