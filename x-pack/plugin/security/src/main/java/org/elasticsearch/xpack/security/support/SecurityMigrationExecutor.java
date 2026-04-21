public class SecurityMigrationExecutor extends TransportAction<SecurityMigrationTaskParams, SecurityMigrationTaskParams> {
    private final SecurityIndexManager securityIndexManager;
    private final Client client;
    private final TreeMap<Integer, SecurityMigrations.SecurityMigration> migrationByVersion;

    public SecurityMigrationExecutor(
        String taskName,
        Executor executor,
        SecurityIndexManager securityIndexManager,
        Client client,
        TreeMap<Integer, SecurityMigrations.SecurityMigration> migrationByVersion
    ) {
        super(taskName, executor);
        this.securityIndexManager = securityIndexManager;
        this.client = client;
        this.migrationByVersion = migrationByVersion;
    }

    @Override
    public SecurityMigrationTaskParams newResponseInstance() {
        return new SecurityMigrationTaskParams();
    }

    @Override
    public SecurityMigrationTaskParams executor() {
        // implementation
    }
}