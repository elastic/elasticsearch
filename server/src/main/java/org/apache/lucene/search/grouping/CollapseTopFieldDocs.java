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
package org.apache.lucene.search.grouping;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.PriorityQueue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents hits returned by {@link CollapsingTopDocsCollector#getTopDocs()}.
 */
public final class CollapseTopFieldDocs extends TopFieldDocs {
    /** The field used for collapsing **/
    public final String field;
    /** The collapse value for each top doc */
    public final Object[] collapseValues;

    public CollapseTopFieldDocs(String field, long totalHits, ScoreDoc[] scoreDocs,
                                SortField[] sortFields, Object[] values, float maxScore) {
        super(totalHits, scoreDocs, sortFields, maxScore);
        this.field = field;
        this.collapseValues = values;
    }

    // Refers to one hit:
    private static final class ShardRef {
        // Which shard (index into shardHits[]):
        final int shardIndex;

        // True if we should use the incoming ScoreDoc.shardIndex for sort order
        final boolean useScoreDocIndex;

        // Which hit within the shard:
        int hitIndex;

        ShardRef(int shardIndex, boolean useScoreDocIndex) {
            this.shardIndex = shardIndex;
            this.useScoreDocIndex = useScoreDocIndex;
        }

        @Override
        public String toString() {
            return "ShardRef(shardIndex=" + shardIndex + " hitIndex=" + hitIndex + ")";
        }

        int getShardIndex(ScoreDoc scoreDoc) {
            if (useScoreDocIndex) {
                if (scoreDoc.shardIndex == -1) {
                    throw new IllegalArgumentException("setShardIndex is false but TopDocs["
                        + shardIndex + "].scoreDocs[" + hitIndex + "] is not set");
                }
                return scoreDoc.shardIndex;
            } else {
                // NOTE: we don't assert that shardIndex is -1 here, because caller could in fact have set it but asked us to ignore it now
                return shardIndex;
            }
        }
    }

    /**
     * if we need to tie-break since score / sort value are the same we first compare shard index (lower shard wins)
     * and then iff shard index is the same we use the hit index.
     */
    static boolean tieBreakLessThan(ShardRef first, ScoreDoc firstDoc, ShardRef second, ScoreDoc secondDoc) {
        final int firstShardIndex = first.getShardIndex(firstDoc);
        final int secondShardIndex = second.getShardIndex(secondDoc);
        // Tie break: earlier shard wins
        if (firstShardIndex < secondShardIndex) {
            return true;
        } else if (firstShardIndex > secondShardIndex) {
            return false;
        } else {
            // Tie break in same shard: resolve however the
            // shard had resolved it:
            assert first.hitIndex != second.hitIndex;
            return first.hitIndex < second.hitIndex;
        }
    }

    private static class MergeSortQueue extends PriorityQueue<ShardRef> {
        // These are really FieldDoc instances:
        final ScoreDoc[][] shardHits;
        final FieldComparator<?>[] comparators;
        final int[] reverseMul;

        MergeSortQueue(Sort sort, CollapseTopFieldDocs[] shardHits) {
            super(shardHits.length);
            this.shardHits = new ScoreDoc[shardHits.length][];
            for (int shardIDX = 0; shardIDX < shardHits.length; shardIDX++) {
                final ScoreDoc[] shard = shardHits[shardIDX].scoreDocs;
                if (shard != null) {
                    this.shardHits[shardIDX] = shard;
                    // Fail gracefully if API is misused:
                    for (int hitIDX = 0; hitIDX < shard.length; hitIDX++) {
                        final ScoreDoc sd = shard[hitIDX];
                        final FieldDoc gd = (FieldDoc) sd;
                        assert gd.fields != null;
                    }
                }
            }

            final SortField[] sortFields = sort.getSort();
            comparators = new FieldComparator[sortFields.length];
            reverseMul = new int[sortFields.length];
            for (int compIDX = 0; compIDX < sortFields.length; compIDX++) {
                final SortField sortField = sortFields[compIDX];
                comparators[compIDX] = sortField.getComparator(1, compIDX);
                reverseMul[compIDX] = sortField.getReverse() ? -1 : 1;
            }
        }

        // Returns true if first is < second
        @Override
        public boolean lessThan(ShardRef first, ShardRef second) {
            assert first != second;
            final FieldDoc firstFD = (FieldDoc) shardHits[first.shardIndex][first.hitIndex];
            final FieldDoc secondFD = (FieldDoc) shardHits[second.shardIndex][second.hitIndex];

            for (int compIDX = 0; compIDX < comparators.length; compIDX++) {
                final FieldComparator comp = comparators[compIDX];

                final int cmp =
                    reverseMul[compIDX] * comp.compareValues(firstFD.fields[compIDX], secondFD.fields[compIDX]);

                if (cmp != 0) {
                    return cmp < 0;
                }
            }
            return tieBreakLessThan(first, firstFD, second, secondFD);
        }
    }

    /**
     * Returns a new CollapseTopDocs, containing topN collapsed results across
     * the provided CollapseTopDocs, sorting by score. Each {@link CollapseTopFieldDocs} instance must be sorted.
     **/
    public static CollapseTopFieldDocs merge(Sort sort, int start, int size,
                                             CollapseTopFieldDocs[] shardHits, boolean setShardIndex) {
        String collapseField = shardHits[0].field;
        for (int i = 1; i < shardHits.length; i++) {
            if (collapseField.equals(shardHits[i].field) == false) {
                throw new IllegalArgumentException("collapse field differ across shards [" +
                    collapseField + "] != [" + shardHits[i].field + "]");
            }
        }
        final PriorityQueue<ShardRef> queue = new MergeSortQueue(sort, shardHits);

        long totalHitCount = 0;
        int availHitCount = 0;
        float maxScore = Float.MIN_VALUE;
        for(int shardIDX=0;shardIDX<shardHits.length;shardIDX++) {
            final CollapseTopFieldDocs shard = shardHits[shardIDX];
            // totalHits can be non-zero even if no hits were
            // collected, when searchAfter was used:
            totalHitCount += shard.totalHits;
            if (shard.scoreDocs != null && shard.scoreDocs.length > 0) {
                availHitCount += shard.scoreDocs.length;
                queue.add(new ShardRef(shardIDX, setShardIndex == false));
                maxScore = Math.max(maxScore, shard.getMaxScore());
            }
        }

        if (availHitCount == 0) {
            maxScore = Float.NaN;
        }

        final ScoreDoc[] hits;
        final Object[] values;
        if (availHitCount <= start) {
            hits = new ScoreDoc[0];
            values = new Object[0];
        } else {
            List<ScoreDoc> hitList = new ArrayList<>();
            List<Object> collapseList = new ArrayList<>();
            int requestedResultWindow = start + size;
            int numIterOnHits = Math.min(availHitCount, requestedResultWindow);
            int hitUpto = 0;
            Set<Object> seen = new HashSet<>();
            while (hitUpto < numIterOnHits) {
                if (queue.size() == 0) {
                    break;
                }
                ShardRef ref = queue.top();
                final ScoreDoc hit = shardHits[ref.shardIndex].scoreDocs[ref.hitIndex];
                final Object collapseValue = shardHits[ref.shardIndex].collapseValues[ref.hitIndex++];
                if (seen.contains(collapseValue)) {
                    if (ref.hitIndex < shardHits[ref.shardIndex].scoreDocs.length) {
                        queue.updateTop();
                    } else {
                        queue.pop();
                    }
                    continue;
                }
                seen.add(collapseValue);
                if (setShardIndex) {
                    hit.shardIndex = ref.shardIndex;
                }
                if (hitUpto >= start) {
                    hitList.add(hit);
                    collapseList.add(collapseValue);
                }

                hitUpto++;

                if (ref.hitIndex < shardHits[ref.shardIndex].scoreDocs.length) {
                    // Not done with this these TopDocs yet:
                    queue.updateTop();
                } else {
                    queue.pop();
                }
            }
            hits = hitList.toArray(new ScoreDoc[0]);
            values = collapseList.toArray(new Object[0]);
        }
        return new CollapseTopFieldDocs(collapseField, totalHitCount, hits, sort.getSort(), values, maxScore);
    }
}
