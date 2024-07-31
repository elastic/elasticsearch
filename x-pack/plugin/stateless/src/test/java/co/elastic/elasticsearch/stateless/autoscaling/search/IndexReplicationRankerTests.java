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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.IndexRankingProperties;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.IndexProperties;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.getRankedIndicesBelowThreshold;
import static co.elastic.elasticsearch.stateless.autoscaling.search.IndexReplicationRanker.rankedIndexComparator;

public class IndexReplicationRankerTests extends ESTestCase {

    IndexRankingProperties createSystemIndex(long interactiveSize, long recency) {
        return new IndexRankingProperties(new IndexProperties(randomAlphaOfLength(8), 1, 0, true, false, recency), interactiveSize);
    }

    IndexRankingProperties createRegularIndex(long interactiveSize, long recency) {
        return new IndexRankingProperties(new IndexProperties(randomAlphaOfLength(8), 1, 0, false, false, recency), interactiveSize);
    }

    IndexRankingProperties createDataStream(long interactiveSize, long recency) {
        return new IndexRankingProperties(new IndexProperties(randomAlphaOfLength(8), 1, 0, false, true, recency), interactiveSize);
    }

    public void testRankedIndexComparator() {
        // system indices should always be greater than regular indices or data stream
        assertEquals(
            1,
            rankedIndexComparator.compare(createSystemIndex(randomLong(), randomLong()), createRegularIndex(randomLong(), randomLong()))
        );
        assertEquals(
            1,
            rankedIndexComparator.compare(createSystemIndex(randomLong(), randomLong()), createDataStream(randomLong(), randomLong()))
        );
        // system indices tie-breaking by size, then by recency
        assertEquals(0, rankedIndexComparator.compare(createSystemIndex(2000, 0), createSystemIndex(2000, 0)));
        assertEquals(-1, rankedIndexComparator.compare(createSystemIndex(1000, randomLong()), createSystemIndex(2000, randomLong())));
        assertEquals(-1, rankedIndexComparator.compare(createSystemIndex(2000, 1000), createSystemIndex(2000, 2000)));

        assertEquals(1, rankedIndexComparator.compare(createSystemIndex(2000, randomLong()), createSystemIndex(1000, randomLong())));
        assertEquals(1, rankedIndexComparator.compare(createSystemIndex(2000, 2000), createSystemIndex(2000, 1000)));
        // regular indices should always sort higher than data streams
        assertEquals(
            1,
            rankedIndexComparator.compare(createRegularIndex(randomLong(), randomLong()), createDataStream(randomLong(), randomLong()))
        );
        // regular index tie-breaking by size, then by recency
        assertEquals(0, rankedIndexComparator.compare(createRegularIndex(1000, 1000), createRegularIndex(1000, 1000)));
        assertEquals(-1, rankedIndexComparator.compare(createRegularIndex(1000, randomLong()), createRegularIndex(2000, randomLong())));
        assertEquals(-1, rankedIndexComparator.compare(createRegularIndex(1000, 1000), createRegularIndex(1000, 2000)));
        assertEquals(1, rankedIndexComparator.compare(createRegularIndex(2000, randomLong()), createRegularIndex(1000, randomLong())));
        assertEquals(1, rankedIndexComparator.compare(createRegularIndex(2000, 2000), createRegularIndex(2000, 1000)));

        // datastreams should tie-break on recency, then on size
        assertEquals(0, rankedIndexComparator.compare(createDataStream(2000, 2000), createDataStream(2000, 2000)));
        assertEquals(-1, rankedIndexComparator.compare(createDataStream(randomLong(), 1000), createDataStream(randomLong(), 2000)));
        assertEquals(-1, rankedIndexComparator.compare(createDataStream(1000, 2000), createDataStream(2000, 2000)));
        assertEquals(1, rankedIndexComparator.compare(createDataStream(randomLong(), 2000), createDataStream(randomLong(), 1000)));
        assertEquals(1, rankedIndexComparator.compare(createDataStream(2000, 2000), createDataStream(1000, 2000)));
    }

    public void testGetRankedIndicesBelowThreshold() {
        assertEquals(0, getRankedIndicesBelowThreshold(Collections.emptyList(), randomLong()).twoReplicaEligableIndices().size());

        long now = System.currentTimeMillis();
        IndexRankingProperties systemIndexNonInteractive = createSystemIndex(0, randomLong());
        IndexRankingProperties systemIndexInteractive = createSystemIndex(1000, randomLong());
        IndexRankingProperties index1 = createRegularIndex(1000, now);
        IndexRankingProperties index2 = createRegularIndex(2000, now);
        IndexRankingProperties ds1backing1 = createDataStream(0, now - 1000);
        IndexRankingProperties ds1backing2 = createDataStream(1000, now);
        IndexRankingProperties ds2backing1 = createDataStream(1000, now - 2000);
        IndexRankingProperties ds2backing2 = createDataStream(1500, now);

        Collection<IndexRankingProperties> indices = List.of(
            systemIndexInteractive,
            systemIndexNonInteractive,
            index1,
            index2,
            ds1backing1,
            ds1backing2,
            ds2backing1,
            ds2backing2
        );
        Collection<IndexRankingProperties> copyOfOriginal = new ArrayList<>(indices);
        Set<String> rankedIndicesBelowThreshold = getRankedIndicesBelowThreshold(indices, Long.MAX_VALUE).twoReplicaEligableIndices();
        assertEquals("ranking should not affect the input", copyOfOriginal, indices);
        assertTrue(
            "ranking should not remove system indices",
            rankedIndicesBelowThreshold.contains(systemIndexInteractive.indexProperties().name())
        );
        assertFalse(
            "ranking should remove non interactive system indices",
            rankedIndicesBelowThreshold.contains(systemIndexNonInteractive.indexProperties().name())
        );
        assertEquals("ranking should remove indices with 0 interactiveSize", indices.size() - 2, rankedIndicesBelowThreshold.size());
        assertFalse(rankedIndicesBelowThreshold.contains(ds1backing1.indexProperties().name()));
        assertThat(
            rankedIndicesBelowThreshold,
            Matchers.containsInAnyOrder(
                systemIndexInteractive.indexProperties().name(),
                index2.indexProperties().name(),
                index1.indexProperties().name(),
                ds2backing2.indexProperties().name(),
                ds1backing2.indexProperties().name(),
                ds2backing1.indexProperties().name()
            )
        );

        // check ordering by increasing threshold, total_interactive_bytes = 7500
        assertEquals(0, getRankedIndicesBelowThreshold(indices, 0).twoReplicaEligableIndices().size());
        assertNull(getRankedIndicesBelowThreshold(indices, 0).lastTwoReplicaIndex());
        assertEquals(systemIndexInteractive, getRankedIndicesBelowThreshold(indices, 0).firstOneReplicaIndex());
        assertEquals(1, getRankedIndicesBelowThreshold(indices, 1000).twoReplicaEligableIndices().size());
        assertTrue(
            getRankedIndicesBelowThreshold(indices, 1000).twoReplicaEligableIndices()
                .contains(systemIndexInteractive.indexProperties().name())
        );
        assertEquals(systemIndexInteractive, getRankedIndicesBelowThreshold(indices, 1000).lastTwoReplicaIndex());
        assertEquals(index2, getRankedIndicesBelowThreshold(indices, 1000).firstOneReplicaIndex());
        assertEquals(2, getRankedIndicesBelowThreshold(indices, 3000).twoReplicaEligableIndices().size());
        assertTrue(getRankedIndicesBelowThreshold(indices, 3000).twoReplicaEligableIndices().contains(index2.indexProperties().name()));
        assertEquals(index2, getRankedIndicesBelowThreshold(indices, 3000).lastTwoReplicaIndex());
        assertEquals(index1, getRankedIndicesBelowThreshold(indices, 3000).firstOneReplicaIndex());
        assertEquals(3, getRankedIndicesBelowThreshold(indices, 4000).twoReplicaEligableIndices().size());
        assertTrue(getRankedIndicesBelowThreshold(indices, 4000).twoReplicaEligableIndices().contains(index1.indexProperties().name()));
        assertEquals(index1, getRankedIndicesBelowThreshold(indices, 4000).lastTwoReplicaIndex());
        assertEquals(ds2backing2, getRankedIndicesBelowThreshold(indices, 4000).firstOneReplicaIndex());
        assertEquals(4, getRankedIndicesBelowThreshold(indices, 5500).twoReplicaEligableIndices().size());
        assertTrue(
            getRankedIndicesBelowThreshold(indices, 5500).twoReplicaEligableIndices().contains(ds2backing2.indexProperties().name())
        );
        assertEquals(ds2backing2, getRankedIndicesBelowThreshold(indices, 5500).lastTwoReplicaIndex());
        assertEquals(ds1backing2, getRankedIndicesBelowThreshold(indices, 5500).firstOneReplicaIndex());
        assertEquals(5, getRankedIndicesBelowThreshold(indices, 6500).twoReplicaEligableIndices().size());
        assertTrue(
            getRankedIndicesBelowThreshold(indices, 6500).twoReplicaEligableIndices().contains(ds1backing2.indexProperties().name())
        );
        assertEquals(ds1backing2, getRankedIndicesBelowThreshold(indices, 6500).lastTwoReplicaIndex());
        assertEquals(ds2backing1, getRankedIndicesBelowThreshold(indices, 6500).firstOneReplicaIndex());
        assertEquals(6, getRankedIndicesBelowThreshold(indices, 7500).twoReplicaEligableIndices().size());
        assertEquals(ds2backing1, getRankedIndicesBelowThreshold(indices, 7500).lastTwoReplicaIndex());
        assertNull(getRankedIndicesBelowThreshold(indices, 7500).firstOneReplicaIndex());
    }
}
