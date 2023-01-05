package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.elasticsearch.common.settings.Settings;

public class FsObjectStoreTests extends AbstractObjectStoreIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.FS)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), randomRepoPath());
    }

}
