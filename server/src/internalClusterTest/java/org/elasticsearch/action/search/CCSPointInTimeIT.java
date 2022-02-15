/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class CCSPointInTimeIT extends AbstractMultiClustersTestCase {

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of("remote_cluster");
    }

    void indexDocs(Client client, String index, int numDocs) {
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client.prepareIndex(index).setId(id).setSource("value", i).get();
        }
        client.admin().indices().prepareRefresh(index).get();
    }

    public void testBasic() {
        final Client localClient = client(LOCAL_CLUSTER);
        final Client remoteClient = client("remote_cluster");
        int localNumDocs = randomIntBetween(10, 50);
        assertAcked(localClient.admin().indices().prepareCreate("local_test"));
        indexDocs(localClient, "local_test", localNumDocs);

        int remoteNumDocs = randomIntBetween(10, 50);
        assertAcked(remoteClient.admin().indices().prepareCreate("remote_test"));
        indexDocs(remoteClient, "remote_test", remoteNumDocs);
        boolean includeLocalIndex = randomBoolean();
        List<String> indices = new ArrayList<>();
        if (includeLocalIndex) {
            indices.add(randomFrom("*", "local_*", "local_test"));
        }
        indices.add(randomFrom("*:*", "remote_cluster:*", "remote_cluster:remote_test"));
        String pitId = openPointInTime(indices.toArray(new String[0]), TimeValue.timeValueMinutes(2));
        try {
            if (randomBoolean()) {
                localClient.prepareIndex("local_test").setId("local_new").setSource().get();
                localClient.admin().indices().prepareRefresh().get();
            }
            if (randomBoolean()) {
                remoteClient.prepareIndex("remote_test").setId("remote_new").setSource().get();
                remoteClient.admin().indices().prepareRefresh().get();
            }
            SearchResponse resp = localClient.prepareSearch()
                .setPreference(null)
                .setQuery(new MatchAllQueryBuilder())
                .setPointInTime(new PointInTimeBuilder(pitId))
                .setSize(1000)
                .get();
            assertNoFailures(resp);
            assertHitCount(resp, (includeLocalIndex ? localNumDocs : 0) + remoteNumDocs);
        } finally {
            closePointInTime(pitId);
        }
    }

    private String openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(OpenPointInTimeAction.INSTANCE, request).actionGet();
        return response.getPointInTimeId();
    }

    private void closePointInTime(String readerId) {
        client().execute(ClosePointInTimeAction.INSTANCE, new ClosePointInTimeRequest(readerId)).actionGet();
    }
}
