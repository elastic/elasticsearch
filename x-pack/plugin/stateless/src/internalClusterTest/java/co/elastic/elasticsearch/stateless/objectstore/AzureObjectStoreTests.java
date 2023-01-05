package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import fixture.azure.AzureHttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.azure.AzureRepositoryPlugin;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class AzureObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    private static final String DEFAULT_ACCOUNT_NAME = "account";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), List.of(AzureRepositoryPlugin.class).stream()).toList();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/" + DEFAULT_ACCOUNT_NAME, new AzureHttpHandler(DEFAULT_ACCOUNT_NAME, "container"));
    }

    @Override
    protected Settings.Builder nodeSettings() {
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(StandardCharsets.UTF_8));
        String accountName = DEFAULT_ACCOUNT_NAME;

        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl() + "/" + accountName;
        Settings.Builder settings = super.nodeSettings().put("azure.client.test.endpoint_suffix", endpoint)
            .put("insecure.azure.client.test.account", accountName)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.AZURE)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "container")
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test");

        if (randomBoolean()) {
            settings = settings.put("insecure.azure.client.test.key", key);
        } else {
            // The SDK expects a valid SAS TOKEN
            settings = settings.put("insecure.azure.client.test.sas_token", "se=2021-07-20T13%3A21Z&sp=rwdl&sv=2018-11-09&sr=c&sig=random");
        }

        return settings;
    }

}
