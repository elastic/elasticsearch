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

import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.elasticsearch.common.settings.Settings;

import java.util.Locale;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.hamcrest.Matchers.equalTo;

public class StatelessHollowIndexShardsIT extends AbstractStatelessIntegTestCase {

    public void testHollowIndexShardsEnabledSetting() {
        boolean hollowShardsEnabled = randomBoolean();
        startMasterAndIndexNode(Settings.builder().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), hollowShardsEnabled).build());

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        var indexShard = findIndexShard(indexName);
        var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        var statelessCommitService = indexEngine.getStatelessCommitService();
        assertThat(statelessCommitService.areHollowIndexShardsEnabled(), equalTo(hollowShardsEnabled));
    }

}
