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
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ObjectStoreService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(ObjectStoreService.class);

    /**
     * This setting refers to the destination of the blobs in the object store.
     * Depending on the underlying object store type, it may be a bucket (for S3 or GCP), a location (for FS), or a container (for Azure).
     */
    public static final Setting<String> BUCKET = Setting.simpleString("stateless.object_store.bucket", Setting.Property.NodeScope);

    public static final Setting<String> CLIENT = Setting.simpleString("stateless.object_store.client", Setting.Property.NodeScope);

    public static final String TYPE_FS = "fs";
    public static final String TYPE_S3 = "s3";
    public static final String TYPE_GCS = "gcs";
    public static final String TYPE_AZURE = "azure";

    public static final List<String> TYPES_SUPPORTED = List.of(TYPE_FS, TYPE_S3, TYPE_GCS, TYPE_AZURE);

    private static final List<Setting<?>> TYPE_VALIDATOR_SETTINGS_LIST = List.of(BUCKET, CLIENT);
    public static final Setting<String> TYPE = Setting.simpleString("stateless.object_store.type", new Setting.Validator<String>() {
        @Override
        public void validate(String value) {}

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings, boolean isPresent) {
            if (TYPES_SUPPORTED.contains(value) == false) {
                throw new IllegalArgumentException(
                    "Unsupported object store [" + TYPE.getKey() + "=" + value + "]. Allowed types are: [" + TYPES_SUPPORTED + "]."
                );
            }

            final String bucket = (String) settings.get(BUCKET);
            final String client = (String) settings.get(CLIENT);

            if (bucket.isEmpty()) {
                throw new IllegalArgumentException("setting " + BUCKET.getKey() + " must be set for an object store of type " + value);
            }
            if (value.equals(TYPE_S3) || value.equals(TYPE_GCS) || value.equals(TYPE_AZURE)) {
                if (client.isEmpty()) {
                    throw new IllegalArgumentException("setting " + CLIENT.getKey() + " must be set for an object store of type " + value);
                }
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return TYPE_VALIDATOR_SETTINGS_LIST.iterator();
        }
    }, Setting.Property.NodeScope);

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

    @Override
    protected void doStart() {
        assert objectStore == null;
        String type = TYPE.get(settings);
        String bucket = BUCKET.get(settings);
        String client = CLIENT.get(settings);

        Settings.Builder builder = Settings.builder();
        if (type.equals(TYPE_FS)) {
            builder = builder.put("location", bucket);
        } else if (type.equals(TYPE_S3)) {
            builder = builder.put("bucket", bucket).put("client", client);
        } else if (type.equals(TYPE_GCS)) {
            builder = builder.put("bucket", bucket).put("client", client);
        } else if (type.equals(TYPE_AZURE)) {
            builder = builder.put("container", bucket).put("client", client);
        }

        final RepositoryMetadata metadata = new RepositoryMetadata("stateless", type, builder.build());
        Repository repository = getRepositoriesService().createRepository(metadata);
        assert repository instanceof BlobStoreRepository;
        this.objectStore = (BlobStoreRepository) repository;

        getObjectStore().start();
        logger.info("started object store service with type [{}], bucket [{}], and client [{}]", type, bucket, client);
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
