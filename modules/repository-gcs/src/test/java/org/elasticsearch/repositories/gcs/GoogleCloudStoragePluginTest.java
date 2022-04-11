package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.util.List;

public class GoogleCloudStoragePluginTest extends ESTestCase {

    public void testGetSettings() {
        List<Setting<?>> settings = new GoogleCloudStoragePlugin(Settings.EMPTY).getSettings();

        Assert.assertEquals(
            List.of(
                "gcs.client.*.credentials_file",
                "gcs.client.*.endpoint",
                "gcs.client.*.project_id",
                "gcs.client.*.connect_timeout",
                "gcs.client.*.read_timeout",
                "gcs.client.*.application_name",
                "gcs.client.*.token_uri",
                "gcs.client.*.proxy.type",
                "gcs.client.*.proxy.host",
                "gcs.client.*.proxy.port"
            ),
            settings.stream().map(Setting::getKey).toList()
        );
    }
}
