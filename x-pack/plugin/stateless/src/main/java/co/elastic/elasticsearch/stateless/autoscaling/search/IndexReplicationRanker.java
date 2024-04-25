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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;

/**
 * Utility class for ranking indices based on their properties for the purpose of a replication size decision.
 */
public class IndexReplicationRanker {

    /**
     * <p>Rank indices according to the following order:</p>
     *
     * <ul>
     *     <li>system indices</li>
     *     <li>regular indices</li>
     *     <li>data streams (backing indices)</li>
     *</ul>
     * <p>Within system and regular indices we rank larger indices first (interactive size, which in this case should
     * be equal to total size), tie breaking on index recency (creation time, newer indices first).</p>
     *
     * <p>Withing data streams, we want to rank all write indices highest, followed by the most recent
     * ones, tie breaking by interactive size. We achieve ranking write indices highest
     * by setting their recency property for ranking to @{@link Long#MAX_VALUE}, otherwise we use the
     * index creation date.</p>
     */
    static Comparator<IndexRankingProperties> rankedIndexComparator =
        // rank system indices highest
        comparing((IndexRankingProperties p) -> p.indexProperties().isSystem())
            // then rank regular indices before data streams (reverse order imposed by isDataStream())
            .thenComparing(p -> p.indexProperties().isDataStream(), reverseOrder())
            // tie-breaking data streams by recency, then by size and
            // regular indices by size, then by recency
            .thenComparing((p1, p2) -> {
                if (p1.indexProperties().isDataStream()) {
                    var res = Long.compare(p1.indexProperties().recency(), p2.indexProperties().recency());
                    return (res != 0) ? res : Long.compare(p1.interactiveSize(), p2.interactiveSize());
                } else {
                    var res = Long.compare(p1.interactiveSize(), p2.interactiveSize());
                    return (res != 0) ? res : Long.compare(p1.indexProperties().recency(), p2.indexProperties().recency());
                }
            });

    /**
     * Returns an ordered list of index properties for all system indices and interactive
     * indices (interactiveSize() > 0), sorted by the ordering criteria in {@link
     * IndexReplicationRanker#rankedIndexComparator}.
     */
    static Set<String> getRankedIndicesBelowThreshold(Collection<IndexRankingProperties> indices, long threshold) {
        var rankedIndices = new ArrayList<IndexRankingProperties>();
        for (var index : indices) {
            // all system indices should be ranked highest, regardless of whether they have an @timestamp field
            // and are thus considered interactive or not, so we add them explicitly here.
            if (index.indexProperties().isSystem() || index.interactiveSize() > 0) {
                rankedIndices.add(index);
            }
        }
        Collections.sort(rankedIndices, rankedIndexComparator.reversed());
        long cumulativeSize = 0;
        var twoReplicaEligableIndices = new HashSet<String>();
        for (IndexRankingProperties index : rankedIndices) {
            cumulativeSize += index.interactiveSize();
            if (cumulativeSize <= threshold) {
                twoReplicaEligableIndices.add(index.indexProperties().name());
            }
        }
        return twoReplicaEligableIndices;
    }

    record IndexRankingProperties(SearchMetricsService.IndexProperties indexProperties, long interactiveSize) {}
}
