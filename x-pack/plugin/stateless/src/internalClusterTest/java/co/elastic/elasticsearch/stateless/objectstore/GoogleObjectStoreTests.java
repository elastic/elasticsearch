package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.gcs.GoogleCloudStoragePlugin;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

public class GoogleObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Map.of("/", new GoogleCloudStorageHttpHandler("bucket"), "/token", new FakeOAuth2HttpHandler());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        final Settings.Builder settings = super.nodeSettings();
        settings.put("gcs.client.test.endpoint", httpServerUrl());
        settings.put("gcs.client.test.token_uri", httpServerUrl() + "/token");
        settings.put(ObjectStoreService.TYPE.getKey(), ObjectStoreService.ObjectStoreType.GCS);
        settings.put(ObjectStoreService.BUCKET.getKey(), "bucket");
        settings.put(ObjectStoreService.CLIENT.getKey(), "test");

        final byte[] serviceAccount = TestUtils.createServiceAccount(random());
        final String serviceAccountString = new String(serviceAccount, StandardCharsets.UTF_8);
        settings.put("insecure.gcs.client.test.credentials_file", serviceAccountString);

        return settings;
    }

}
