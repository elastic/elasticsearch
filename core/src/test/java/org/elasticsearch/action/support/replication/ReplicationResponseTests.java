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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.instanceOf;

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
                ReplicationResponse.ShardInfo.Failure[] randomFailures = RandomObjects.randomShardInfoFailures(random(), nbFailures);
                return new ReplicationResponse.ShardInfo(shardInfo.getTotal(), shardInfo.getSuccessful(), randomFailures);
            });
            return randomFrom(mutations).get();
        };

        checkEqualsAndHashCode(RandomObjects.randomShardInfo(random(), randomBoolean()), copy, mutate);
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
            final Set<String> indexNamePool = new HashSet<>(Arrays.asList(
                    randomUnicodeOfCodepointLength(5), randomUnicodeOfCodepointLength(6)));
            indexNamePool.remove(index.getName());
            final ShardId randomIndex = new ShardId(randomFrom(indexNamePool), index.getUUID(), failure.shardId());
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(randomIndex, failure.nodeId(), (Exception) failure.getCause(),
                    failure.status(), failure.primary()));

            final Set<String> uuidPool = new HashSet<>(Arrays.asList(randomUnicodeOfCodepointLength(5), randomUnicodeOfCodepointLength(6)));
            uuidPool.remove(index.getUUID());
            final ShardId randomUUID = new ShardId(index.getName(), randomFrom(uuidPool), failure.shardId());
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(randomUUID, failure.nodeId(), (Exception) failure.getCause(),
                    failure.status(), failure.primary()));

            final ShardId randomShardId = new ShardId(index.getName(),index.getUUID(), failure.shardId() + randomIntBetween(1, 3));
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(randomShardId, failure.nodeId(), (Exception) failure.getCause(),
                    failure.status(), failure.primary()));

            final Set<String> nodeIdPool = new HashSet<>(Arrays.asList(randomUnicodeOfLength(3), randomUnicodeOfLength(4)));
            nodeIdPool.remove(failure.nodeId());
            final String randomNode = randomFrom(nodeIdPool);
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(failure.fullShardId(), randomNode, (Exception) failure.getCause(),
                    failure.status(), failure.primary()));

            final Set<Exception> exceptionPool = new HashSet<>(Arrays.asList(
                    new IllegalStateException("a"), new IllegalArgumentException("b")));
            exceptionPool.remove(failure.getCause());
            final Exception randomException = randomFrom(exceptionPool);
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(failure.fullShardId(), failure.nodeId(), randomException,
                    failure.status(), failure.primary()));

            final Set<RestStatus> otherStatuses = new HashSet<>(Arrays.asList(RestStatus.values()));
            otherStatuses.remove(failure.status());
            final RestStatus randomStatus = randomFrom(otherStatuses);
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(failure.fullShardId(), failure.nodeId(),
                    (Exception) failure.getCause(), randomStatus, failure.primary()));

            final boolean randomPrimary = !failure.primary();
            mutations.add(() -> new ReplicationResponse.ShardInfo.Failure(failure.fullShardId(), failure.nodeId(),
                    (Exception) failure.getCause(), failure.status(), randomPrimary));

            return randomFrom(mutations).get();
        };

        checkEqualsAndHashCode(RandomObjects.randomShardInfoFailure(random()), copy, mutate);
    }

    public void testShardInfoToXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(5, 3);
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfo, xContentType);

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
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfo, xContentType);

        ReplicationResponse.ShardInfo parsedShardInfo;
        try (XContentParser parser = createParser(xContentType.xContent(), shardInfoBytes)) {
            // Move to the first start object
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedShardInfo = ReplicationResponse.ShardInfo.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        // We can use assertEquals because the shardInfo doesn't have a failure (and exceptions)
        assertEquals(shardInfo, parsedShardInfo);

        BytesReference parsedShardInfoBytes = XContentHelper.toXContent(parsedShardInfo, xContentType);
        assertEquals(shardInfoBytes, parsedShardInfoBytes);
    }

    public void testShardInfoWithFailureToXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo shardInfo = RandomObjects.randomShardInfo(random(), true);
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfo, xContentType);

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
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfo, xContentType);

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
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfoFailure, xContentType);

        try (XContentParser parser = createParser(xContentType.xContent(), shardInfoBytes)) {
            assertFailure(parser, shardInfoFailure);
        }
    }

    public void testRandomFailureToAndFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

        final ReplicationResponse.ShardInfo.Failure shardInfoFailure = RandomObjects.randomShardInfoFailure(random());;
        final BytesReference shardInfoBytes = XContentHelper.toXContent(shardInfoFailure, xContentType);

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
                assertEquals(name.replaceFirst("es.", ""), parser.currentName());
                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                assertEquals(ex.getHeader(name).get(0), parser.text());
            }
            if (ex.getCause() != null) {
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals("caused_by", parser.currentName());
                assertThrowable(parser, ex.getCause());
            }
        }
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
    }
}
