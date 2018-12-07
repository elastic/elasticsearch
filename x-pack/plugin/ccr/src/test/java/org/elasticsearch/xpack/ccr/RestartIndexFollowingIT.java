/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.Locale;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class RestartIndexFollowingIT extends CcrIntegTestCase {

    @Override
    protected int numberOfNodesPerCluster() {
        return 1;
    }

    public void testFollowIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0,
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true");
        assertAcked(leaderClient().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureLeaderGreen("index1");

        final PutFollowAction.Request followRequest = putFollow("index1", "index2");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            leaderClient().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> {
            assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value, equalTo(firstBatchNumDocs));
        });

        getFollowerCluster().fullRestart();
        ensureFollowerGreen("index2");

        final long secondBatchNumDocs = randomIntBetween(2, 64);
        for (int i = 0; i < secondBatchNumDocs; i++) {
            leaderClient().prepareIndex("index1", "doc").setSource("{}", XContentType.JSON).get();
        }

        assertBusy(() -> {
            assertThat(followerClient().prepareSearch("index2").get().getHits().getTotalHits().value,
                equalTo(firstBatchNumDocs + secondBatchNumDocs));
        });
    }

}
