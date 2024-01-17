/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;

import java.util.Collection;
import java.util.Locale;

import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils.getObjectStoreMockRepository;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class StatelessPrefetchIT extends AbstractStatelessIntegTestCase {

    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    public void testSearchShardsStarted() throws Exception {
        startMasterAndIndexNode();
        // create large enough cache with small enough pages on the search node to allow prefetching all data
        String searchNode = startSearchNode(
            Settings.builder()
                .put(
                    SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                    new ByteSizeValue(randomIntBetween(10, 100), ByteSizeUnit.MB).getStringRep()
                )
                .put(
                    SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                    new ByteSizeValue(256, ByteSizeUnit.KB).getStringRep()
                )
                .build()
        );
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        int rounds = randomIntBetween(1, 4);
        int totalDocs = 0;
        for (int i = 0; i < rounds; i++) {
            int cnt = randomIntBetween(10, 100);
            indexDocsAndRefresh(indexName, cnt);
            totalDocs += cnt;
        }

        logger.info("--> blocking repository");
        var mockRepository = getObjectStoreMockRepository(internalCluster().getInstance(ObjectStoreService.class, searchNode));
        mockRepository.setBlockOnAnyFiles();
        logger.info("--> running search and verifying that the repository was not accessed because prefetching warmed the cache already");
        assertHitCount(client().prepareSearch().setQuery(new MatchAllQueryBuilder()), totalDocs);
        assertFalse(mockRepository.blocked());
    }
}
