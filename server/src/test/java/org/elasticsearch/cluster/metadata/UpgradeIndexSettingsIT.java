package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class UpgradeIndexSettingsIT extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testSettingsVersion() {
        createIndex(
                "test",
                Settings
                        .builder()
                        .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT.minimumIndexCompatibilityVersion())
                        .build());
        final long settingsVersion =
                client().admin().cluster().prepareState().get().getState().metaData().index("test").getSettingsVersion();
        client().admin().indices().prepareUpgrade("test").get();
        assertThat(
                client().admin().cluster().prepareState().get().getState().metaData().index("test").getSettingsVersion(),
                equalTo(1 + settingsVersion));
    }

}