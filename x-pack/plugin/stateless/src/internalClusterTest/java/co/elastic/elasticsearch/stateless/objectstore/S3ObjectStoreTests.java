package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class S3ObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), List.of(S3RepositoryPlugin.class).stream()).toList();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/bucket", new S3HttpHandler("bucket"));
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put("s3.client.test.endpoint", httpServerUrl())
            .put("insecure.s3.client.test.access_key", "test_access_key")
            .put("insecure.s3.client.test.secret_key", "test_secret_key")
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.S3)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "bucket")
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test");
    }

}
