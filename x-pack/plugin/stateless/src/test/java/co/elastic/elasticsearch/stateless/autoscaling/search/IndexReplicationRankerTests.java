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

import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.IndexProperties;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchMetricsService.IndexRankingProperties;

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
        assertEquals(0, getRankedIndicesBelowThreshold(Collections.emptyList(), randomLong()).size());

        long now = System.currentTimeMillis();
        IndexRankingProperties systemIndex = createSystemIndex(0, randomLong());
        IndexRankingProperties index1 = createRegularIndex(1000, now);
        IndexRankingProperties index2 = createRegularIndex(2000, now);
        IndexRankingProperties ds1backing1 = createDataStream(0, now - 1000);
        IndexRankingProperties ds1backing2 = createDataStream(1000, now);
        IndexRankingProperties ds2backing1 = createDataStream(1000, now - 2000);
        IndexRankingProperties ds2backing2 = createDataStream(1500, now);

        Collection<IndexRankingProperties> indices = List.of(
            systemIndex,
            index1,
            index2,
            ds1backing1,
            ds1backing2,
            ds2backing1,
            ds2backing2
        );
        Collection<IndexRankingProperties> copyOfOriginal = new ArrayList<>(indices);
        Set<String> rankedIndicesBelowThreshold = getRankedIndicesBelowThreshold(indices, Long.MAX_VALUE);
        assertEquals("ranking should not affect the input", copyOfOriginal, indices);
        assertTrue("ranking should not remove system indices", rankedIndicesBelowThreshold.contains(systemIndex.indexProperties().name()));
        assertEquals("ranking should remove indices with 0 interactiveSize", indices.size() - 1, rankedIndicesBelowThreshold.size());
        assertFalse(rankedIndicesBelowThreshold.contains(ds1backing1.indexProperties().name()));
        assertThat(
            rankedIndicesBelowThreshold,
            Matchers.containsInAnyOrder(
                systemIndex.indexProperties().name(),
                index2.indexProperties().name(),
                index1.indexProperties().name(),
                ds2backing2.indexProperties().name(),
                ds1backing2.indexProperties().name(),
                ds2backing1.indexProperties().name()
            )
        );

        // check ordering by increasing threshold, total_interactive_bytes = 6500
        assertEquals(1, getRankedIndicesBelowThreshold(indices, 0).size());
        assertTrue(getRankedIndicesBelowThreshold(indices, 0).contains(systemIndex.indexProperties().name()));
        assertEquals(2, getRankedIndicesBelowThreshold(indices, 2000).size());
        assertTrue(getRankedIndicesBelowThreshold(indices, 2000).contains(index2.indexProperties().name()));
        assertEquals(3, getRankedIndicesBelowThreshold(indices, 3000).size());
        assertTrue(getRankedIndicesBelowThreshold(indices, 3000).contains(index1.indexProperties().name()));
        assertEquals(4, getRankedIndicesBelowThreshold(indices, 4500).size());
        assertTrue(getRankedIndicesBelowThreshold(indices, 4500).contains(ds2backing2.indexProperties().name()));
        assertEquals(5, getRankedIndicesBelowThreshold(indices, 5500).size());
        assertTrue(getRankedIndicesBelowThreshold(indices, 5500).contains(ds1backing2.indexProperties().name()));
        assertEquals(6, getRankedIndicesBelowThreshold(indices, 6500).size());
    }
}
