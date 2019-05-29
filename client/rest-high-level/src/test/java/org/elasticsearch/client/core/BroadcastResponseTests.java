/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;

public class BroadcastResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String index = randomAlphaOfLength(8);
        final String id = randomAlphaOfLength(8);
        final int total = randomIntBetween(1, 16);
        final int successful = total - scaledRandomIntBetween(0, total);
        final int failed = scaledRandomIntBetween(0, total - successful);
        final List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        final Set<Integer> shardIds = new HashSet<>();
        for (int i = 0; i < failed; i++) {
            final DefaultShardOperationFailedException failure = new DefaultShardOperationFailedException(
                    index,
                    randomValueOtherThanMany(shardIds::contains, () -> randomIntBetween(0, total - 1)),
                    new RetentionLeaseNotFoundException(id));
            failures.add(failure);
            shardIds.add(failure.shardId());
        }

        final org.elasticsearch.action.support.broadcast.BroadcastResponse to =
                new org.elasticsearch.action.support.broadcast.BroadcastResponse(total, successful, failed, failures);

        final XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference bytes = toShuffledXContent(to, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());

        final XContent xContent = XContentFactory.xContent(xContentType);
        final XContentParser parser = xContent.createParser(
                new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
                LoggingDeprecationHandler.INSTANCE,
                bytes.streamInput());
        final BroadcastResponse from = BroadcastResponse.fromXContent(parser);
        assertThat(from.shards().total(), equalTo(total));
        assertThat(from.shards().successful(), equalTo(successful));
        assertThat(from.shards().skipped(), equalTo(0));
        assertThat(from.shards().failed(), equalTo(failed));
        assertThat(from.shards().failures(), hasSize(failed == 0 ? failed : 1)); // failures are grouped
        if (failed > 0) {
            final DefaultShardOperationFailedException groupedFailure = from.shards().failures().iterator().next();
            assertThat(groupedFailure.index(), equalTo(index));
            assertThat(groupedFailure.shardId(), isIn(shardIds));
            assertThat(groupedFailure.reason(), containsString("reason=retention lease with ID [" + id + "] not found"));
        }
    }

}
