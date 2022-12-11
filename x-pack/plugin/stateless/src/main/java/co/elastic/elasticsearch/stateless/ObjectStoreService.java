package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class ObjectStoreService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(ObjectStoreService.class);

    /**
     * This setting refers to the destination of the blobs in the object store.
     * Depending on the underlying object store type, it may be a bucket (for S3 or GCP), a location (for FS), or a container (for Azure).
     */
    public static final Setting<String> BUCKET = Setting.simpleString("stateless.object_store.bucket", Setting.Property.NodeScope);

    public static final Setting<String> CLIENT = Setting.simpleString("stateless.object_store.client", Setting.Property.NodeScope);

    public enum ObjectStoreType {
        FS((bucket, builder) -> builder.put("location", bucket), (client, builder) -> {}, false),
        S3((bucket, builder) -> builder.put("bucket", bucket), (client, builder) -> builder.put("client", client), true),
        GCS((bucket, builder) -> builder.put("bucket", bucket), (client, builder) -> builder.put("client", client), true),
        AZURE((bucket, builder) -> builder.put("container", bucket), (client, builder) -> builder.put("client", client), true);

        private final BiConsumer<String, Settings.Builder> bucketConsumer;
        private final BiConsumer<String, Settings.Builder> clientConsumer;
        private final boolean needsClient;

        ObjectStoreType(
            BiConsumer<String, Settings.Builder> bucketConsumer,
            BiConsumer<String, Settings.Builder> clientConsumer,
            boolean needsClient
        ) {
            this.bucketConsumer = bucketConsumer;
            this.clientConsumer = clientConsumer;
            this.needsClient = needsClient;
        }

        public Settings repositorySettings(String bucket, String client) {
            Settings.Builder builder = Settings.builder();
            bucketConsumer.accept(bucket, builder);
            clientConsumer.accept(client, builder);
            return builder.build();
        }

        public boolean needsClient() {
            return needsClient;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static final List<Setting<?>> TYPE_VALIDATOR_SETTINGS_LIST = List.of(BUCKET, CLIENT);
    public static final Setting<ObjectStoreType> TYPE = Setting.enumSetting(
        ObjectStoreType.class,
        "stateless.object_store.type",
        ObjectStoreType.FS,
        new Setting.Validator<ObjectStoreType>() {
            @Override
            public void validate(ObjectStoreType value) {}

            @Override
            public void validate(final ObjectStoreType value, final Map<Setting<?>, Object> settings, boolean isPresent) {
                final String bucket = (String) settings.get(BUCKET);
                final String client = (String) settings.get(CLIENT);
                if (bucket.isEmpty()) {
                    throw new IllegalArgumentException("setting " + BUCKET.getKey() + " must be set for an object store of type " + value);
                }
                if (value.needsClient()) {
                    if (client.isEmpty()) {
                        throw new IllegalArgumentException(
                            "setting " + CLIENT.getKey() + " must be set for an object store of type " + value
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return TYPE_VALIDATOR_SETTINGS_LIST.iterator();
            }
        },
        Setting.Property.NodeScope
    );

    private final Settings settings;
    private final Environment environment;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private BlobStoreRepository objectStore;

    @Inject
    public ObjectStoreService(Settings settings, Environment environment, Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.settings = settings;
        this.environment = environment;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
    }

    private RepositoriesService getRepositoriesService() {
        return Objects.requireNonNull(repositoriesServiceSupplier.get());
    }

    public BlobStoreRepository getObjectStore() {
        return Objects.requireNonNull(objectStore);
    }

    private static RepositoryMetadata getRepositoryMetadata(Settings settings) {
        ObjectStoreType type = TYPE.get(settings);
        String bucket = BUCKET.get(settings);
        String client = CLIENT.get(settings);

        return new RepositoryMetadata(Stateless.NAME, type.toString(), type.repositorySettings(bucket, client));
    }

    @Override
    protected void doStart() {
        assert objectStore == null;
        Repository repository = getRepositoriesService().createRepository(getRepositoryMetadata(settings));
        assert repository instanceof BlobStoreRepository;
        this.objectStore = (BlobStoreRepository) repository;
        getObjectStore().start();
        logger.info(
            "started object store service with type [{}], bucket [{}], client [{}]",
            TYPE.get(settings),
            BUCKET.get(settings),
            CLIENT.get(settings)
        );
    }

    @Override
    protected void doStop() {
        logger.trace("stopping object store service");
        getObjectStore().close();
        this.objectStore = null;
    }

    @Override
    protected void doClose() throws IOException {
        logger.trace("closing object store service");
    }

    void onCommitCreation(StatelessCommitRef commit) {
        logger.debug("{} commit created [{}][{}]", commit.getShardId(), commit.getSegmentsFileName(), commit.getGeneration());
        IOUtils.closeWhileHandlingException(commit);
    }
}
