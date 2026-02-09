/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.BWCVersions;
import org.elasticsearch.xpack.stateless.autoscaling.MetricQuality;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MemoryMetrics;
import org.elasticsearch.xpack.stateless.autoscaling.search.load.NodeSearchLoadSnapshot;

import java.io.IOException;

public class SearchTierMetricsTests extends AbstractWireSerializingTestCase<SearchTierMetrics> {

    public final void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var testInstance = createTestInstance();
            for (TransportVersion bwcVersion : BWCVersions.DEFAULT_BWC_VERSIONS) {
                assertBwcSerialization(testInstance, bwcVersion);
            }
        }
    }

    // todo: clear out duplication from AbstractSerializationTestCase.
    protected final void assertBwcSerialization(SearchTierMetrics testInstance, TransportVersion version) throws IOException {
        SearchTierMetrics deserializedInstance = copyWriteable(testInstance, getNamedWriteableRegistry(), instanceReader(), version);
        assertOnBWCObject(deserializedInstance, mutateInstanceForVersion(testInstance, version), version);
    }

    protected void assertOnBWCObject(SearchTierMetrics bwcSerializedObject, SearchTierMetrics testInstance, TransportVersion version) {
        assertNotSame(version.toString(), bwcSerializedObject, testInstance);
        assertEquals(version.toString(), bwcSerializedObject, testInstance);
        assertEquals(version.toString(), bwcSerializedObject.hashCode(), testInstance.hashCode());
    }

    protected SearchTierMetrics mutateInstanceForVersion(SearchTierMetrics instance, TransportVersion version) {
        if (version.supports(SearchTierMetrics.SP_TRANSPORT_VERSION)) {
            return instance;
        } else {
            return new SearchTierMetrics(
                instance.getMemoryMetrics(),
                instance.getMaxShardCopies(),
                instance.getStorageMetrics(),
                instance.getNodesLoad(),
                -1,
                -1
            );
        }
    }

    @Override
    protected Writeable.Reader<SearchTierMetrics> instanceReader() {
        return SearchTierMetrics::new;
    }

    @Override
    protected SearchTierMetrics createTestInstance() {
        return new SearchTierMetrics(
            randomMemoryMetrics(),
            new MaxShardCopies(between(1, 10), randomQuality()),
            new StorageMetrics(
                randomLongBetween(0, Long.MAX_VALUE),
                randomLongBetween(0, Long.MAX_VALUE),
                randomLongBetween(0, Long.MAX_VALUE),
                randomQuality()
            ),
            randomList(between(0, 10), () -> new NodeSearchLoadSnapshot(randomUUID(), randomDoubleBetween(0, 1000, true), randomQuality())),
            between(0, 1000),
            between(0, 1000)
        );
    }

    @Override
    protected SearchTierMetrics mutateInstance(SearchTierMetrics instance) throws IOException {
        // todo: verify all, for not just SP.
        return switch (between(0, 1)) {
            case 0 -> new SearchTierMetrics(
                instance.getMemoryMetrics(),
                instance.getMaxShardCopies(),
                instance.getStorageMetrics(),
                instance.getNodesLoad(),
                randomValueOtherThan(instance.getSearchPowerMin(), () -> between(0, 1000)),
                instance.getSearchPowerMax()
            );
            case 1 -> new SearchTierMetrics(
                instance.getMemoryMetrics(),
                instance.getMaxShardCopies(),
                instance.getStorageMetrics(),
                instance.getNodesLoad(),
                instance.getSearchPowerMin(),
                randomValueOtherThan(instance.getSearchPowerMax(), () -> between(0, 1000))
            );
            default -> throw new AssertionError();
        };
    }

    public static MemoryMetrics randomMemoryMetrics() {
        return new MemoryMetrics(randomLong(), randomLong(), randomQuality());
    }

    public static MetricQuality randomQuality() {
        return randomFrom(MetricQuality.values());
    }

}
