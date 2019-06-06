/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats.AutoFollowedCluster;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AutoFollowStatsTests extends AbstractSerializingTestCase<AutoFollowStats> {

    @Override
    protected AutoFollowStats doParseInstance(XContentParser parser) throws IOException {
        return AutoFollowStats.fromXContent(parser);
    }

    @Override
    protected AutoFollowStats createTestInstance() {
        return new AutoFollowStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomReadExceptions(),
            randomTrackingClusters()
        );
    }

    static NavigableMap<String, Tuple<Long, ElasticsearchException>> randomReadExceptions() {
        final int count = randomIntBetween(0, 16);
        final NavigableMap<String, Tuple<Long, ElasticsearchException>> readExceptions = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            readExceptions.put("" + i, Tuple.tuple(randomNonNegativeLong(),
                new ElasticsearchException(new IllegalStateException("index [" + i + "]"))));
        }
        return readExceptions;
    }

    static NavigableMap<String, AutoFollowedCluster> randomTrackingClusters() {
        final int count = randomIntBetween(0, 16);
        final NavigableMap<String, AutoFollowedCluster> readExceptions = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            readExceptions.put("" + i, new AutoFollowedCluster(randomLong(), randomNonNegativeLong()));
        }
        return readExceptions;
    }

    @Override
    protected Writeable.Reader<AutoFollowStats> instanceReader() {
        return AutoFollowStats::new;
    }

    @Override
    protected void assertEqualInstances(AutoFollowStats expectedInstance, AutoFollowStats newInstance) {
        assertNotSame(expectedInstance, newInstance);

        assertThat(newInstance.getNumberOfFailedRemoteClusterStateRequests(),
            equalTo(expectedInstance.getNumberOfFailedRemoteClusterStateRequests()));
        assertThat(newInstance.getNumberOfFailedFollowIndices(), equalTo(expectedInstance.getNumberOfFailedFollowIndices()));
        assertThat(newInstance.getNumberOfSuccessfulFollowIndices(), equalTo(expectedInstance.getNumberOfSuccessfulFollowIndices()));

        assertThat(newInstance.getRecentAutoFollowErrors().size(), equalTo(expectedInstance.getRecentAutoFollowErrors().size()));
        assertThat(newInstance.getRecentAutoFollowErrors().keySet(), equalTo(expectedInstance.getRecentAutoFollowErrors().keySet()));
        for (final Map.Entry<String, Tuple<Long, ElasticsearchException>> entry : newInstance.getRecentAutoFollowErrors().entrySet()) {
            // x-content loses the exception
            final Tuple<Long, ElasticsearchException> expected = expectedInstance.getRecentAutoFollowErrors().get(entry.getKey());
            assertThat(entry.getValue().v1(), equalTo(expected.v1()));
            assertThat(entry.getValue().v2().getMessage(), containsString(expected.v2().getMessage()));
            assertNotNull(entry.getValue().v2().getCause());
            assertThat(
                entry.getValue().v2().getCause(),
                anyOf(instanceOf(ElasticsearchException.class), instanceOf(IllegalStateException.class)));
            assertThat(entry.getValue().v2().getCause().getMessage(), containsString(expected.v2().getCause().getMessage()));
        }

        assertThat(newInstance.getAutoFollowedClusters(), equalTo(expectedInstance.getAutoFollowedClusters()));
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }
}
