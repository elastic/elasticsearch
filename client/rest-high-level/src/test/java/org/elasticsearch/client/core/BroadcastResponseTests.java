/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;

public class BroadcastResponseTests extends AbstractResponseTestCase<org.elasticsearch.action.support.broadcast.BroadcastResponse,
    BroadcastResponse> {

    private String index;
    private String id;
    private Set<Integer> shardIds;

    @Override
    protected org.elasticsearch.action.support.broadcast.BroadcastResponse createServerTestInstance(XContentType xContentType) {
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

        return new org.elasticsearch.action.support.broadcast.BroadcastResponse(total, successful, failed, failures);
    }

    @Override
    protected BroadcastResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return BroadcastResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.support.broadcast.BroadcastResponse serverTestInstance,
                                   BroadcastResponse clientInstance) {
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
    }

}
