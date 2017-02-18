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
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.instanceOf;

public class ReplicationResponseTests extends ESTestCase {

    public void testEqualsAndHashcode() {
        // Copy function for a ShardId
        EqualsHashCodeTestUtils.CopyFunction<ShardId> copyShardId = original ->
                new ShardId(original.getIndexName(), original.getIndex().getUUID(), original.getId());

        // Mutate function for a ShardId
        EqualsHashCodeTestUtils.MutateFunction<ShardId> mutateShardId = original -> {
            List<Supplier<ShardId>> mutations = new ArrayList<>();
            mutations.add(() ->
                    new ShardId(original.getIndexName() + "_mutate", original.getIndex().getUUID(), original.getId()));
            mutations.add(() ->
                    new ShardId(original.getIndexName(), original.getIndex().getUUID() + "_mutate", original.getId()));
            mutations.add(() ->
                    new ShardId(original.getIndexName(), original.getIndex().getUUID(), original.getId() + 1));
            return randomFrom(mutations).get();
        };

        // Copy function for a ShardInfo.Failure
        EqualsHashCodeTestUtils.CopyFunction<ReplicationResponse.ShardInfo.Failure> copyShardInfoFailure = original ->
                new ReplicationResponse.ShardInfo.Failure(copyShardId.copy(original.fullShardId()), original.nodeId(),
                        (Exception) original.getCause(), original.status(), original.primary());

        // Mutate function for a ShardInfo.Failure
        EqualsHashCodeTestUtils.MutateFunction<ReplicationResponse.ShardInfo.Failure> mutateShardInfoFailure = original -> {
            List<CheckedSupplier<ReplicationResponse.ShardInfo.Failure, IOException>> mutations = new ArrayList<>();
            mutations.add(() ->
                    new ReplicationResponse.ShardInfo.Failure(mutateShardId.mutate(original.fullShardId()), original.nodeId(),
                            (Exception) original.getCause(), original.status(), original.primary()));
            mutations.add(() ->
                    new ReplicationResponse.ShardInfo.Failure(original.fullShardId(), original.nodeId() + "_mutate",
                            (Exception) original.getCause(), original.status(), original.primary()));
            mutations.add(() ->
                    new ReplicationResponse.ShardInfo.Failure(original.fullShardId(), original.nodeId(),
                            new NullPointerException("mutate"), original.status(), original.primary()));
            mutations.add(() -> {
                    RestStatus mutate = null;
                    do {
                        mutate = randomFrom(RestStatus.values());
                    } while (mutate == original.status());
                    return new ReplicationResponse.ShardInfo.Failure(original.fullShardId(), original.nodeId(),
                            (Exception) original.getCause(), mutate, original.primary());
            });
            mutations.add(() ->
                    new ReplicationResponse.ShardInfo.Failure(original.fullShardId(), original.nodeId(),
                            (Exception) original.getCause(), original.status(), !original.primary()));
            return randomFrom(mutations).get();
        };

        // Copy function for a ShardInfo
        EqualsHashCodeTestUtils.CopyFunction<ReplicationResponse.ShardInfo> copyShardInfo = original -> {
            ReplicationResponse.ShardInfo.Failure[] originalFailures = original.getFailures();

            ReplicationResponse.ShardInfo.Failure[] copyFailures = new ReplicationResponse.ShardInfo.Failure[originalFailures.length];
            for (int i = 0; i < originalFailures.length; i++) {
                copyFailures[i] = copyShardInfoFailure.copy(originalFailures[i]);
            }
            return new ReplicationResponse.ShardInfo(original.getTotal(), original.getSuccessful(), copyFailures);
        };

        // Mutate function for a ShardInfo
        EqualsHashCodeTestUtils.MutateFunction<ReplicationResponse.ShardInfo> mutateShardInfo = original -> {
            List<CheckedSupplier<ReplicationResponse.ShardInfo, IOException>> mutations = new ArrayList<>();
            mutations.add(() ->
                    new ReplicationResponse.ShardInfo(original.getTotal() + 1, original.getSuccessful(), original.getFailures()));
            mutations.add(() ->
                    new ReplicationResponse.ShardInfo(original.getTotal(), original.getSuccessful() + 1, original.getFailures()));

            int originalNbFailures = original.getFailures().length;
            if (originalNbFailures == 0) {
                mutations.add(() ->
                        new ReplicationResponse.ShardInfo(original.getTotal(), original.getSuccessful(),
                                new ReplicationResponse.ShardInfo.Failure(
                                        new ShardId(randomAsciiOfLength(5), randomAsciiOfLength(5), randomIntBetween(0, 3)),
                                        randomAsciiOfLength(5), null, randomFrom(RestStatus.values()), randomBoolean())));
            } else {
                mutations.add(() -> {
                    ReplicationResponse.ShardInfo.Failure[] failures = new ReplicationResponse.ShardInfo.Failure[originalNbFailures];
                    int mutate = randomIntBetween(0, originalNbFailures - 1);
                    for (int i = 0; i < originalNbFailures; i++) {
                        if (i == mutate) {
                            failures[i] = mutateShardInfoFailure.mutate(original.getFailures()[i]);
                        } else {
                            failures[i] = copyShardInfoFailure.copy(original.getFailures()[i]);
                        }
                    }
                    return new ReplicationResponse.ShardInfo(original.getTotal(), original.getSuccessful(), failures);
                });
            }
            return randomFrom(mutations).get();
        };

        // Copy function for a ReplicationResponse
        EqualsHashCodeTestUtils.CopyFunction<ReplicationResponse> copyReplicationResponse = original -> {
            ReplicationResponse mutate = new ReplicationResponse();
            mutate.setShardInfo(copyShardInfo.copy(original.getShardInfo()));
            return mutate;
        };

        // Mutate function for a ReplicationResponse
        EqualsHashCodeTestUtils.MutateFunction<ReplicationResponse> mutateReplicationResponse = original -> {
            ReplicationResponse mutate = new ReplicationResponse();
            mutate.setShardInfo(mutateShardInfo.mutate(original.getShardInfo()));
            return mutate;
        };
        checkEqualsAndHashCode(randomReplicationResponse(), copyReplicationResponse, mutateReplicationResponse);
    }

    public void testShardInfoToString() {
        final int total = 5;
        final int successful = randomIntBetween(1, total);
        final ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(total, successful);
        assertEquals(String.format(Locale.ROOT, "ShardInfo{total=5, successful=%d, failures=[]}", successful), shardInfo.toString());
    }

    public void testShardInfoToXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3);
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfo, xContentType, randomBoolean());

        // Expected JSON is {"total":5,"successful":3,"failed":0}
        assertThat(shardInfo, instanceOf(ToXContentObject.class));
        try (XContentParser parser = createParser(xContentType.xContent(), shardInfoBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("total", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
            assertEquals(shardInfo.getTotal(), parser.intValue());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("successful", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
            assertEquals(shardInfo.getSuccessful(), parser.intValue());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("failed", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
            assertEquals(shardInfo.getFailed(), parser.intValue());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
    }

    public void testShardInfoToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(randomIntBetween(1, 5), randomIntBetween(1, 5));
        boolean humanReadable = randomBoolean();
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfo, xContentType, humanReadable);

        ReplicationResponse.ShardInfo parsedShardInfo;
        try (XContentParser parser = createParser(xContentType.xContent(), shardInfoBytes)) {
            // Move to the first start object
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedShardInfo = ReplicationResponse.ShardInfo.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        // We can use assertEquals because the shardInfo doesn't have a failure (and exceptions)
        assertEquals(shardInfo, parsedShardInfo);

        BytesReference parsedShardInfoBytes = XContentHelper.toXContent(parsedShardInfo, xContentType, humanReadable);
        assertEquals(shardInfoBytes, parsedShardInfoBytes);
    }

    public void testShardInfoWithFailureToXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo shardInfo = RandomObjects.randomShardInfo(random(), true);
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfo, xContentType, randomBoolean());

        try (XContentParser parser = createParser(xContentType.xContent(), shardInfoBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("total", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
            assertEquals(shardInfo.getTotal(), parser.intValue());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("successful", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
            assertEquals(shardInfo.getSuccessful(), parser.intValue());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("failed", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
            assertEquals(shardInfo.getFailed(), parser.intValue());

            if (shardInfo.getFailures() != null && shardInfo.getFailures().length > 0) {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("failures", parser.currentName());
                assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());

                for (int i = 0; i < shardInfo.getFailures().length; i++) {
                    assertFailure(parser, shardInfo.getFailures()[i]);
                }
                assertEquals(XContentParser.Token.END_ARRAY, parser.nextToken());
            }

            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
    }

    public void testRandomShardInfoFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo shardInfo = RandomObjects.randomShardInfo(random(), randomBoolean());
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfo, xContentType, randomBoolean());

        ReplicationResponse.ShardInfo parsedShardInfo;
        try (XContentParser parser = createParser(xContentType.xContent(), shardInfoBytes)) {
            // Move to the first start object
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedShardInfo = ReplicationResponse.ShardInfo.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        // We can't use assertEquals to compare the original ShardInfo with the parsed ShardInfo
        // because it may include random failures with exceptions, and exception types are not
        // preserved during ToXContent->FromXContent process.
        assertNotNull(parsedShardInfo);
        assertEquals(shardInfo.getTotal(), parsedShardInfo.getTotal());
        assertEquals(shardInfo.getSuccessful(), parsedShardInfo.getSuccessful());
        assertEquals(shardInfo.getFailed(), parsedShardInfo.getFailed());
        assertEquals(shardInfo.getFailures().length, parsedShardInfo.getFailures().length);

        for (int i = 0; i < shardInfo.getFailures().length; i++) {
            ReplicationResponse.ShardInfo.Failure parsedFailure = parsedShardInfo.getFailures()[i];
            ReplicationResponse.ShardInfo.Failure failure = shardInfo.getFailures()[i];

            assertEquals(failure.index(), parsedFailure.index());
            assertEquals(failure.shardId(), parsedFailure.shardId());
            assertEquals(failure.nodeId(), parsedFailure.nodeId());
            assertEquals(failure.status(), parsedFailure.status());
            assertEquals(failure.primary(), parsedFailure.primary());

            Throwable cause = failure.getCause();
            String expectedMessage = "Elasticsearch exception [type=" + ElasticsearchException.getExceptionName(cause)
                    + ", reason=" + cause.getMessage() + "]";
            assertEquals(expectedMessage, parsedFailure.getCause().getMessage());
        }
    }

    public void testRandomFailureToXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo.Failure shardInfoFailure = RandomObjects.randomShardInfoFailure(random());
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfoFailure, xContentType, randomBoolean());

        try (XContentParser parser = createParser(xContentType.xContent(), shardInfoBytes)) {
            assertFailure(parser, shardInfoFailure);
        }
    }

    public void testRandomFailureToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo.Failure shardInfoFailure = RandomObjects.randomShardInfoFailure(random());;
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfoFailure, xContentType, randomBoolean());

        ReplicationResponse.ShardInfo.Failure parsedFailure;
        try (XContentParser parser = createParser(xContentType.xContent(), shardInfoBytes)) {
            // Move to the first start object
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedFailure = ReplicationResponse.ShardInfo.Failure.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(shardInfoFailure.index(), parsedFailure.index());
        assertEquals(shardInfoFailure.shardId(), parsedFailure.shardId());
        assertEquals(shardInfoFailure.nodeId(), parsedFailure.nodeId());
        assertEquals(shardInfoFailure.status(), parsedFailure.status());
        assertEquals(shardInfoFailure.primary(), parsedFailure.primary());

        Throwable cause = shardInfoFailure.getCause();
        String expectedMessage = "Elasticsearch exception [type=" + ElasticsearchException.getExceptionName(cause)
                + ", reason=" + cause.getMessage() + "]";
        assertEquals(expectedMessage, parsedFailure.getCause().getMessage());
    }

    private static void assertFailure(XContentParser parser, ReplicationResponse.ShardInfo.Failure failure) throws IOException {
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("_index", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals(failure.index(), parser.text());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("_shard", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
        assertEquals(failure.shardId(), parser.intValue());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("_node", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals(failure.nodeId(), parser.text());

        Throwable cause = failure.getCause();
        if (cause != null) {
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("reason", parser.currentName());
            assertThrowable(parser, cause);
        }
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("status", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals(failure.status(), RestStatus.valueOf(parser.text()));
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("primary", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_BOOLEAN, parser.nextToken());
        assertEquals(failure.primary(), parser.booleanValue());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
    }

    private static void assertThrowable(XContentParser parser, Throwable cause) throws IOException {
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("type", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals(ElasticsearchException.getExceptionName(cause), parser.text());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("reason", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals(cause.getMessage(), parser.text());
        if (cause instanceof ElasticsearchException) {
            ElasticsearchException ex = (ElasticsearchException) cause;
            for (String name : ex.getHeaderKeys()) {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals(name, parser.currentName());
                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                assertEquals(ex.getHeader(name).get(0), parser.text());
            }
            for (String name : ex.getMetadataKeys()) {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals(name.replaceFirst("es.", ""), parser.currentName());
                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                assertEquals(ex.getMetadata(name).get(0), parser.text());
            }
            if (ex.getCause() != null) {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("caused_by", parser.currentName());
                assertThrowable(parser, ex.getCause());
            }
        }
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
    }

    private static ReplicationResponse randomReplicationResponse() {
        ReplicationResponse replicationResponse = new ReplicationResponse();
        replicationResponse.setShardInfo(RandomObjects.randomShardInfo(random(), randomBoolean()));
        return replicationResponse;
    }
}
