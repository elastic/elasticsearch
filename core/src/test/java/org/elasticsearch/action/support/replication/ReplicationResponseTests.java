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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShardRecoveringException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class ReplicationResponseTests extends ESTestCase {

    public void testShardInfoToString() {
        final int total = 5;
        final int successful = randomIntBetween(1, total);
        final ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(total, successful);
        assertEquals(String.format(Locale.ROOT, "ShardInfo{total=5, successful=%d, failures=[]}", successful), shardInfo.toString());
    }

    public void testShardInfoEqualsAndHashcode() {
        EqualsHashCodeTestUtils.CopyFunction<ReplicationResponse.ShardInfo> copy = shardInfo ->
                new ReplicationResponse.ShardInfo(shardInfo.getTotal(), shardInfo.getSuccessful(), shardInfo.getFailures());

        EqualsHashCodeTestUtils.MutateFunction<ReplicationResponse.ShardInfo> mutate = shardInfo -> {
            List<Supplier<ReplicationResponse.ShardInfo>> mutations = new ArrayList<>();
            mutations.add(() ->
                    new ReplicationResponse.ShardInfo(shardInfo.getTotal() + 1, shardInfo.getSuccessful(), shardInfo.getFailures()));
            mutations.add(() ->
                    new ReplicationResponse.ShardInfo(shardInfo.getTotal(), shardInfo.getSuccessful() + 1, shardInfo.getFailures()));
            mutations.add(() -> {
                int nbFailures = randomIntBetween(1, 5);
                return new ReplicationResponse.ShardInfo(shardInfo.getTotal(), shardInfo.getSuccessful(),  randomFailures(nbFailures));
            });
            return randomFrom(mutations).get();
        };

        checkEqualsAndHashCode(randomShardInfo(), copy, mutate);
    }

    public void testFailureEqualsAndHashcode() {
        EqualsHashCodeTestUtils.CopyFunction<ReplicationResponse.ShardInfo.Failure> copy = failure -> {
            Index index = failure.fullShardId().getIndex();
            ShardId shardId = new ShardId(index.getName(), index.getUUID(), failure.shardId());
            Exception cause = (Exception) failure.getCause();
            return new ReplicationResponse.ShardInfo.Failure(shardId, failure.nodeId(), cause, failure.status(), failure.primary());
        };

        EqualsHashCodeTestUtils.MutateFunction<ReplicationResponse.ShardInfo.Failure> mutate = failure -> {
            List<Supplier<ReplicationResponse.ShardInfo.Failure>> mutations = new ArrayList<>();

            final Index index = failure.fullShardId().getIndex();
            final ShardId randomIndex = new ShardId(randomUnicodeOfCodepointLength(5), index.getUUID(), failure.shardId());
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(randomIndex, failure.nodeId(), (Exception) failure.getCause(),
                    failure.status(), failure.primary()));

            final ShardId randomUUID = new ShardId(index.getName(), randomUnicodeOfCodepointLength(5), failure.shardId());
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(randomUUID, failure.nodeId(), (Exception) failure.getCause(),
                    failure.status(), failure.primary()));

            final ShardId randomShardId = new ShardId(index.getName(),index.getUUID(), failure.shardId() + randomIntBetween(1, 3));
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(randomShardId, failure.nodeId(), (Exception) failure.getCause(),
                    failure.status(), failure.primary()));

            final String randomNode = randomUnicodeOfLength(3);
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(failure.fullShardId(), randomNode, (Exception) failure.getCause(),
                    failure.status(), failure.primary()));

            final Exception randomException = randomFrom(new IllegalStateException("a"), new IllegalArgumentException("b"));
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(failure.fullShardId(), failure.nodeId(), randomException,
                    failure.status(), failure.primary()));

            final RestStatus randomStatus = randomFrom(RestStatus.values());
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(failure.fullShardId(), failure.nodeId(),
                    (Exception) failure.getCause(), randomStatus, failure.primary()));

            final boolean randomPrimary = !failure.primary();
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(failure.fullShardId(), failure.nodeId(),
                    (Exception) failure.getCause(), failure.status(), randomPrimary));

            return randomFrom(mutations).get();
        };

        checkEqualsAndHashCode(randomFailure(), copy, mutate);
    }

    public void testShardInfoToXContent() throws IOException {
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(10, 10);

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            shardInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{\"total\":10,\"successful\":10,\"failed\":0}", builder.string());
        }
    }

    public void testShardInfoWithFailureToXContent() throws IOException {
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(2, 1,
                new ReplicationResponse.ShardInfo.Failure(new ShardId("_index", "_uuid", 1), "_node_id",
                        new IllegalStateException("failure cause"), RestStatus.LOCKED, true));

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            shardInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{"
                + "\"total\":2,"
                + "\"successful\":1,"
                + "\"failed\":1,"
                + "\"failures\":["
                    + "{"
                    + "\"_index\":\"_index\","
                    + "\"_shard\":1,"
                    + "\"_node\":\"_node_id\","
                    + "\"reason\":{\"type\":\"illegal_state_exception\",\"reason\":\"failure cause\"},"
                    + "\"status\":\"LOCKED\","
                    + "\"primary\":true"
                    + "}"
                + "]"
            + "}", builder.string());
        }
    }

    public void testRandomShardInfoToXContent() throws IOException {
        final ReplicationResponse.ShardInfo shardInfo = randomShardInfo();

        // Build the expected JSON from the random ShardInfo
        StringBuilder expectedJson = new StringBuilder();
        expectedJson.append("{");
        expectedJson.append("\"total\":").append(shardInfo.getTotal()).append(",");
        expectedJson.append("\"successful\":").append(shardInfo.getSuccessful()).append(",");
        expectedJson.append("\"failed\":").append(shardInfo.getFailed());
        if (shardInfo.getFailures() != null && shardInfo.getFailures().length > 0) {
            expectedJson.append(",\"failures\":[");
            for (int i = 0; i < shardInfo.getFailures().length; i++) {
                ReplicationResponse.ShardInfo.Failure failure = shardInfo.getFailures()[i];
                appendExpectedFailure(expectedJson, failure);
                if (i + 1 < shardInfo.getFailures().length) {
                    expectedJson.append(",");
                }
            }
            expectedJson.append("]");
        }
        expectedJson.append("}");

        // Checks that the printed shardInfo matches the expected JSON
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            shardInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(expectedJson.toString(), builder.string());
        }
    }

    public void testFailureToXContent() throws IOException {
        ReplicationResponse.ShardInfo.Failure shardInfoFailure =
                new ReplicationResponse.ShardInfo.Failure(new ShardId("_index", "_uuid", 0), "_node_id",
                        new IllegalStateException("failure cause"), RestStatus.FORBIDDEN, false);

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            shardInfoFailure.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{"
                + "\"_index\":\"_index\","
                + "\"_shard\":0,"
                + "\"_node\":\"_node_id\","
                + "\"reason\":{"
                    + "\"type\":\"illegal_state_exception\","
                    + "\"reason\":\"failure cause\""
                + "},"
                + "\"status\":\"FORBIDDEN\","
                + "\"primary\":false"
            + "}", builder.string());
        }
    }

    public void testRandomFailureToXContent() throws IOException {
        ReplicationResponse.ShardInfo.Failure shardInfoFailure = randomFailure();

        // Build the expected JSON from the random ShardInfo.Failure
        StringBuilder expectedJson = new StringBuilder();
        appendExpectedFailure(expectedJson, shardInfoFailure);

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            shardInfoFailure.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(expectedJson.toString(), builder.string());
        }
    }

    /**
     * Builds the expected JSON for a given ShardInfo.Failure
     */
    private static void appendExpectedFailure(StringBuilder stringBuilder, ReplicationResponse.ShardInfo.Failure failure) {
        stringBuilder.append('{');
        stringBuilder.append("\"_index\":\"").append(failure.index()).append("\",");
        stringBuilder.append("\"_shard\":").append(failure.shardId()).append(',');
        stringBuilder.append("\"_node\":\"").append(failure.nodeId()).append("\",");
        if (failure.getCause() != null) {
            stringBuilder.append("\"reason\":");
            appendExpectedCause(stringBuilder, failure.getCause());
            stringBuilder.append(',');
        }
        stringBuilder.append("\"status\":\"").append(failure.status()).append("\",");
        stringBuilder.append("\"primary\":").append(failure.primary());
        stringBuilder.append('}');
    }

    /**
     * Builds the expected JSON for a given Throwable when printed within a ShardInfo.Failure
     */
    private static void appendExpectedCause(StringBuilder stringBuilder, Throwable cause) {
        stringBuilder.append('{');
        stringBuilder.append("\"type\":\"").append(ElasticsearchException.getExceptionName(cause)).append("\",");
        stringBuilder.append("\"reason\":\"").append(cause.getMessage()).append("\"");
        if (cause instanceof ElasticsearchException) {
            ElasticsearchException ex = (ElasticsearchException) cause;
            if (ex.getHeaderKeys().isEmpty() == false) {
                stringBuilder.append(',');
                Iterator<String> headers = ex.getHeaderKeys().iterator();
                while (headers.hasNext()) {
                    String name = headers.next();
                    List<String> values = ex.getHeader(name);
                    stringBuilder.append("\"").append(name.replaceFirst("es.", "")).append("\":");
                    if (values != null && values.isEmpty() == false) {
                        if (values.size() == 1) {
                            stringBuilder.append("\"").append(values.get(0)).append("\"");
                        }
                        if (headers.hasNext()) {
                            stringBuilder.append(',');
                        }
                    }
                }
            }
            if (ex.getCause() != null) {
                stringBuilder.append(',');
                stringBuilder.append("\"caused_by\":");
                appendExpectedCause(stringBuilder, ex.getCause());
            }
        }
        stringBuilder.append('}');
    }

    private static ReplicationResponse.ShardInfo randomShardInfo() {
        int total = randomIntBetween(1, 10);
        int successful = randomIntBetween(0, total);
        return new ReplicationResponse.ShardInfo(total, successful, randomFailures(Math.max(0, (total - successful))));
    }

    private static ReplicationResponse.ShardInfo.Failure[] randomFailures(int nbFailures) {
        List<ReplicationResponse.ShardInfo.Failure> randomFailures = new ArrayList<>(nbFailures);
        for (int i = 0; i < nbFailures; i++) {
            randomFailures.add(randomFailure());
        }
        return randomFailures.toArray(new ReplicationResponse.ShardInfo.Failure[nbFailures]);
    }

    private static ReplicationResponse.ShardInfo.Failure randomFailure() {
        return new ReplicationResponse.ShardInfo.Failure(
                new ShardId(randomAsciiOfLength(5), randomAsciiOfLength(5), randomIntBetween(0, 5)),
                randomAsciiOfLength(3),
                randomFrom(
                        new IndexShardRecoveringException(new ShardId("_test", "_0", 5)),
                        new ElasticsearchException(new IllegalArgumentException("argument is wrong")),
                        new RoutingMissingException("_test", "_type", "_id")
                ),
                randomFrom(RestStatus.values()),
                randomBoolean()
        );
    }
}
