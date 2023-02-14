/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ShardFollowNodeTaskStatusTests extends AbstractXContentSerializingTestCase<ShardFollowNodeTaskStatus> {

    @Override
    protected ShardFollowNodeTaskStatus doParseInstance(XContentParser parser) throws IOException {
        return ShardFollowNodeTaskStatus.fromXContent(parser);
    }

    @Override
    protected ShardFollowNodeTaskStatus createTestInstance() {
        // if you change this constructor, reflect the changes in the hand-written assertions below
        return new ShardFollowNodeTaskStatus(
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            randomInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomReadExceptions(),
            randomNonNegativeLong(),
            randomBoolean() ? new ElasticsearchException("fatal error") : null
        );
    }

    @Override
    protected ShardFollowNodeTaskStatus mutateInstance(ShardFollowNodeTaskStatus instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected void assertEqualInstances(final ShardFollowNodeTaskStatus expectedInstance, final ShardFollowNodeTaskStatus newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getRemoteCluster(), equalTo(expectedInstance.getRemoteCluster()));
        assertThat(newInstance.leaderIndex(), equalTo(expectedInstance.leaderIndex()));
        assertThat(newInstance.followerIndex(), equalTo(expectedInstance.followerIndex()));
        assertThat(newInstance.getShardId(), equalTo(expectedInstance.getShardId()));
        assertThat(newInstance.leaderGlobalCheckpoint(), equalTo(expectedInstance.leaderGlobalCheckpoint()));
        assertThat(newInstance.leaderMaxSeqNo(), equalTo(expectedInstance.leaderMaxSeqNo()));
        assertThat(newInstance.followerGlobalCheckpoint(), equalTo(expectedInstance.followerGlobalCheckpoint()));
        assertThat(newInstance.lastRequestedSeqNo(), equalTo(expectedInstance.lastRequestedSeqNo()));
        assertThat(newInstance.outstandingReadRequests(), equalTo(expectedInstance.outstandingReadRequests()));
        assertThat(newInstance.outstandingWriteRequests(), equalTo(expectedInstance.outstandingWriteRequests()));
        assertThat(newInstance.writeBufferOperationCount(), equalTo(expectedInstance.writeBufferOperationCount()));
        assertThat(newInstance.followerMappingVersion(), equalTo(expectedInstance.followerMappingVersion()));
        assertThat(newInstance.followerSettingsVersion(), equalTo(expectedInstance.followerSettingsVersion()));
        assertThat(newInstance.followerAliasesVersion(), equalTo(expectedInstance.followerAliasesVersion()));
        assertThat(newInstance.totalReadTimeMillis(), equalTo(expectedInstance.totalReadTimeMillis()));
        assertThat(newInstance.successfulReadRequests(), equalTo(expectedInstance.successfulReadRequests()));
        assertThat(newInstance.failedReadRequests(), equalTo(expectedInstance.failedReadRequests()));
        assertThat(newInstance.operationsReads(), equalTo(expectedInstance.operationsReads()));
        assertThat(newInstance.bytesRead(), equalTo(expectedInstance.bytesRead()));
        assertThat(newInstance.totalWriteTimeMillis(), equalTo(expectedInstance.totalWriteTimeMillis()));
        assertThat(newInstance.successfulWriteRequests(), equalTo(expectedInstance.successfulWriteRequests()));
        assertThat(newInstance.failedWriteRequests(), equalTo(expectedInstance.failedWriteRequests()));
        assertThat(newInstance.operationWritten(), equalTo(expectedInstance.operationWritten()));
        assertThat(newInstance.readExceptions().size(), equalTo(expectedInstance.readExceptions().size()));
        assertThat(newInstance.readExceptions().keySet(), equalTo(expectedInstance.readExceptions().keySet()));
        for (final Map.Entry<Long, Tuple<Integer, ElasticsearchException>> entry : newInstance.readExceptions().entrySet()) {
            final Tuple<Integer, ElasticsearchException> expectedTuple = expectedInstance.readExceptions().get(entry.getKey());
            assertThat(entry.getValue().v1(), equalTo(expectedTuple.v1()));
            // x-content loses the exception
            final ElasticsearchException expected = expectedTuple.v2();
            assertThat(entry.getValue().v2().getMessage(), containsString(expected.getMessage()));
            assertNotNull(entry.getValue().v2().getCause());
            assertThat(
                entry.getValue().v2().getCause(),
                anyOf(instanceOf(ElasticsearchException.class), instanceOf(IllegalStateException.class))
            );
            assertThat(entry.getValue().v2().getCause().getMessage(), containsString(expected.getCause().getMessage()));
        }
        assertThat(newInstance.timeSinceLastReadMillis(), equalTo(expectedInstance.timeSinceLastReadMillis()));
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    private NavigableMap<Long, Tuple<Integer, ElasticsearchException>> randomReadExceptions() {
        final int count = randomIntBetween(0, 16);
        final NavigableMap<Long, Tuple<Integer, ElasticsearchException>> readExceptions = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            readExceptions.put(
                randomNonNegativeLong(),
                Tuple.tuple(
                    randomIntBetween(0, Integer.MAX_VALUE),
                    new ElasticsearchException(new IllegalStateException("index [" + i + "]"))
                )
            );
        }
        return readExceptions;
    }

    @Override
    protected Writeable.Reader<ShardFollowNodeTaskStatus> instanceReader() {
        return ShardFollowNodeTaskStatus::new;
    }

}
