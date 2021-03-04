/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.xpack.core.action.ReloadAnalyzersResponse.ReloadDetails;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;

public class ReloadAnalyzersResponseTests
        extends AbstractResponseTestCase<org.elasticsearch.xpack.core.action.ReloadAnalyzersResponse, ReloadAnalyzersResponse> {

    private String index;
    private String id;
    private Set<Integer> shardIds;

    @Override
    protected org.elasticsearch.xpack.core.action.ReloadAnalyzersResponse createServerTestInstance(XContentType xContentType) {
        index = randomAlphaOfLength(8);
        id = randomAlphaOfLength(8);
        final int total = randomIntBetween(1, 16);
        final int successful = total - scaledRandomIntBetween(0, total);
        final int failed = scaledRandomIntBetween(0, total - successful);
        final List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        shardIds = new HashSet<>();
        for (int i = 0; i < failed; i++) {
            final DefaultShardOperationFailedException failure = new DefaultShardOperationFailedException(
                index,
                randomValueOtherThanMany(shardIds::contains, () -> randomIntBetween(0, total - 1)),
                new RetentionLeaseNotFoundException(id));
            failures.add(failure);
            shardIds.add(failure.shardId());
        }
        Map<String, ReloadDetails> reloadedDetailsMap = new HashMap<>();
        int randomIndices = randomIntBetween(0, 5);
        for (int i = 0; i < randomIndices; i++) {
            String indexName = randomAlphaOfLengthBetween(5, 10);
            Set<String> randomNodeIds = new HashSet<>(Arrays.asList(generateRandomStringArray(5, 5, false, true)));
            Set<String> randomAnalyzers = new HashSet<>(Arrays.asList(generateRandomStringArray(5, 5, false, true)));

            ReloadDetails reloadedDetails = new ReloadDetails(indexName, randomNodeIds, randomAnalyzers);
            reloadedDetailsMap.put(indexName, reloadedDetails);
        }
        return new org.elasticsearch.xpack.core.action.ReloadAnalyzersResponse(total, successful, failed, failures, reloadedDetailsMap);
    }

    @Override
    protected ReloadAnalyzersResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return ReloadAnalyzersResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.xpack.core.action.ReloadAnalyzersResponse serverTestInstance,
            ReloadAnalyzersResponse clientInstance) {
        assertThat(clientInstance.shards().total(), equalTo(serverTestInstance.getTotalShards()));
        assertThat(clientInstance.shards().successful(), equalTo(serverTestInstance.getSuccessfulShards()));
        assertThat(clientInstance.shards().skipped(), equalTo(0));
        assertThat(clientInstance.shards().failed(), equalTo(serverTestInstance.getFailedShards()));
        assertThat(clientInstance.shards().failures(), hasSize(clientInstance.shards().failed() == 0 ? 0 : 1)); // failures are grouped
        if (clientInstance.shards().failed() > 0) {
            final DefaultShardOperationFailedException groupedFailure = clientInstance.shards().failures().iterator().next();
            assertThat(groupedFailure.index(), equalTo(index));
            assertThat(groupedFailure.shardId(), in(shardIds));
            assertThat(groupedFailure.reason(), containsString("reason=retention lease with ID [" + id + "] not found"));
        }
        Map<String, ReloadDetails> serverDetails = serverTestInstance.getReloadDetails();
        assertThat(clientInstance.getReloadedDetails().size(), equalTo(serverDetails.size()));
        for (Entry<String, org.elasticsearch.client.indices.ReloadAnalyzersResponse.ReloadDetails> entry : clientInstance
                .getReloadedDetails().entrySet()) {
            String indexName = entry.getKey();
            assertTrue(serverDetails.keySet().contains(indexName));
            assertEquals(serverDetails.get(indexName).getIndexName(), entry.getValue().getIndexName());
            assertEquals(serverDetails.get(indexName).getReloadedAnalyzers(), entry.getValue().getReloadedAnalyzers());
            assertEquals(serverDetails.get(indexName).getReloadedIndicesNodes(), entry.getValue().getReloadedIndicesNodes());
        }
    }

}
