/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class LenientPointInTimeIT extends ESIntegTestCase {

    public void testBasic() throws Exception {
        final String index = "my_test_index";
        createIndex(
            index,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 10).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        int numDocs = randomIntBetween(10, 50);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            prepareIndex(index).setId(id).setSource("value", i).get();
        }
        refresh(index);
        OpenPointInTimeResponse pointInTimeResponse = openPointInTime(new String[] { index }, TimeValue.timeValueMinutes(1));
        try {
            assertThat(10, equalTo(pointInTimeResponse.getTotalShards()));
            assertThat(10, equalTo(pointInTimeResponse.getSuccessfulShards()));
            assertThat(0, equalTo(pointInTimeResponse.getFailedShards()));
            assertThat(0, equalTo(pointInTimeResponse.getSkippedShards()));

            assertResponse(
                prepareSearch().setQuery(new MatchAllQueryBuilder())
                    .setPointInTime(new PointInTimeBuilder(pointInTimeResponse.getPointInTimeId())),
                resp -> {
                    assertThat(resp.pointInTimeId(), equalTo(pointInTimeResponse.getPointInTimeId()));
                    assertHitCount(resp, numDocs);
                }
            );

            final String randomDataNode = internalCluster().getNodeNameThat(
                settings -> DiscoveryNode.hasRole(settings, DiscoveryNodeRole.DATA_ROLE)
            );

            updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", randomDataNode));
            ensureGreen(index);

            assertResponse(
                prepareSearch().setQuery(new MatchAllQueryBuilder()),
                resp -> {
                    assertNotNull(resp.getHits().getTotalHits());
                    assertThat(resp.getHits().getTotalHits().value, lessThan((long)numDocs));
                }
            );

            OpenPointInTimeResponse pointInTimeResponseOneNodeDown = openPointInTime(
                new String[] { index },
                TimeValue.timeValueMinutes(10)
            );
            try {
                assertThat(10, equalTo(pointInTimeResponseOneNodeDown.getTotalShards()));
                assertThat(5, equalTo(pointInTimeResponseOneNodeDown.getSuccessfulShards()));
                assertThat(5, equalTo(pointInTimeResponseOneNodeDown.getFailedShards()));
                assertThat(0, equalTo(pointInTimeResponseOneNodeDown.getSkippedShards()));

                assertResponse(
                    prepareSearch().setQuery(new MatchAllQueryBuilder())
                        .setPointInTime(new PointInTimeBuilder(pointInTimeResponseOneNodeDown.getPointInTimeId())),
                    resp -> {
                        assertThat(resp.pointInTimeId(), equalTo(pointInTimeResponseOneNodeDown.getPointInTimeId()));
                        assertNotNull(resp.getHits().getTotalHits());
                        assertThat(resp.getHits().getTotalHits().value, lessThan((long) numDocs));
                    }
                );

                for (int i = numDocs; i < numDocs * 2; i++) {
                    String id = Integer.toString(i);
                    prepareIndex(index).setId(id).setSource("value", i).get();
                }

                assertResponse(
                    prepareSearch().setQuery(new MatchAllQueryBuilder()),
                    resp -> {
                        assertNotNull(resp.getHits().getTotalHits());
                        assertThat(resp.getHits().getTotalHits().value, greaterThan((long)numDocs));
                    }
                );

                assertResponse(
                    prepareSearch().setQuery(new MatchAllQueryBuilder())
                        .setPointInTime(new PointInTimeBuilder(pointInTimeResponseOneNodeDown.getPointInTimeId())),
                    resp -> {
                        assertThat(resp.pointInTimeId(), equalTo(pointInTimeResponseOneNodeDown.getPointInTimeId()));
                        assertNotNull(resp.getHits().getTotalHits());
                        assertThat(resp.getHits().getTotalHits().value, lessThan((long) numDocs));
                    }
                );


            } finally {
                closePointInTime(pointInTimeResponseOneNodeDown.getPointInTimeId());
            }

        } finally {
            closePointInTime(pointInTimeResponse.getPointInTimeId());
        }
    }

    private OpenPointInTimeResponse openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive).allowPartialSearchResults(true);
        return client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
    }

    private void closePointInTime(BytesReference readerId) {
        client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(readerId)).actionGet();
    }
}
