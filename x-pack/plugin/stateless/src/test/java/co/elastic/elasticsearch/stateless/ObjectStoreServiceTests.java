package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.AZURE;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.FS;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.GCS;
import static co.elastic.elasticsearch.stateless.ObjectStoreService.ObjectStoreType.S3;

public class ObjectStoreServiceTests extends ESTestCase {
    public void testNoBucket() {
        ObjectStoreType type = randomFrom(ObjectStoreType.values());
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE.getKey(), type.name());
        expectThrows(IllegalArgumentException.class, () -> ObjectStoreService.TYPE.get(builder.build()));
    }

    public void testObjectStoreSettingsNoClient() {
        ObjectStoreType type = randomFrom(S3, GCS, AZURE);
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE.getKey(), type.name());
        builder.put(ObjectStoreService.BUCKET.getKey(), randomAlphaOfLength(5));
        expectThrows(IllegalArgumentException.class, () -> ObjectStoreService.TYPE.get(builder.build()));
    }

    public void testFSSettings() {
        String bucket = randomAlphaOfLength(5);
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE.getKey(), FS.name());
        builder.put(ObjectStoreService.BUCKET.getKey(), bucket);
        // no throw
        ObjectStoreType objectStoreType = ObjectStoreService.TYPE.get(builder.build());
        Settings settings = objectStoreType.repositorySettings(bucket, null);
        assertThat(settings.keySet().size(), Matchers.equalTo(1));
        assertThat(settings.get("location"), Matchers.equalTo(bucket));
    }

    public void testObjectStoreSettings() {
        validateObjectStoreSettings(S3, "bucket");
        validateObjectStoreSettings(GCS, "bucket");
        validateObjectStoreSettings(AZURE, "container");
    }

    private void validateObjectStoreSettings(ObjectStoreType type, String bucketName) {
        String bucket = randomAlphaOfLength(5);
        String client = randomAlphaOfLength(5);
        Settings.Builder builder = Settings.builder();
        builder.put(ObjectStoreService.TYPE.getKey(), type.name());
        builder.put(ObjectStoreService.BUCKET.getKey(), bucket);
        builder.put(ObjectStoreService.CLIENT.getKey(), client);
        // check no throw
        ObjectStoreType objectStoreType = ObjectStoreService.TYPE.get(builder.build());
        Settings settings = objectStoreType.repositorySettings(bucket, client);
        assertThat(settings.keySet().size(), Matchers.equalTo(2));
        assertThat(settings.get(bucketName), Matchers.equalTo(bucket));
        assertThat(settings.get("client"), Matchers.equalTo(client));
    }

}
