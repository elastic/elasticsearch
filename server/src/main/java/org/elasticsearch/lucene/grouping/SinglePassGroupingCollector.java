/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */
package org.elasticsearch.lucene.grouping;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.GroupSelector;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;

import static org.apache.lucene.search.SortField.Type.SCORE;

/**
 * A collector that groups documents based on field values and returns {@link TopFieldGroups}
 * output. The grouping is done in a single pass by selecting only the top sorted document per grouping key.
 * The value used for the key of each group can be found in {@link TopFieldGroups#groupValues}.
 *
 * This collector optionally supports searching after a previous result through the 'after' parameter.
 *
 * TODO: If the sort is based on score we should propagate the mininum competitive score when <code>orderedGroups</code>
 *       is full. This is safe for grouping since the group sort is the same as the query sort.
 */
public class SinglePassGroupingCollector<T> extends SimpleCollector {

    /**
     * Creates a {@link SinglePassGroupingCollector} on a {@link NumericDocValues} field.
     * It accepts also {@link SortedNumericDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param groupField        The sort field used to group documents.
     * @param groupFieldType    The {@link MappedFieldType} for this sort field.
     * @param groupSort         The {@link Sort} used to sort the groups.
     *                          The grouping keeps only the top sorted document per grouping key.
     *                          This must be non-null, ie, if you want to groupSort by relevance
     *                          use Sort.RELEVANCE.
     * @param topN              How many top groups to keep.
     * @param after             The field values to search after. Can be null.
     */
    public static SinglePassGroupingCollector<?> createNumeric(
        String groupField,
        MappedFieldType groupFieldType,
        Sort groupSort,
        int topN,
        @Nullable FieldDoc after
    ) {
        return new SinglePassGroupingCollector<>(new GroupingDocValuesSelector.Numeric(groupFieldType), groupField, groupSort, topN, after);
    }

    /**
     * Creates a {@link SinglePassGroupingCollector} on a {@link SortedDocValues} field.
     * It accepts also {@link SortedSetDocValues} field but the collect will fail with
     * an {@link IllegalStateException} if a document contains more than one value for the field.
     *
     * @param groupField        The sort field used to group documents.
     * @param groupFieldType    The {@link MappedFieldType} for this sort field.
     * @param groupSort         The {@link Sort} used to sort the groups. The grouping keeps only the top sorted
     *                          document per grouping key.
     *                          This must be non-null, ie, if you want to groupSort by relevance use Sort.RELEVANCE.
     * @param topN              How many top groups to keep.
     * @param after             The field values to search after. Can be null.
     */
    public static SinglePassGroupingCollector<?> createKeyword(
        String groupField,
        MappedFieldType groupFieldType,
        Sort groupSort,
        int topN,
        @Nullable FieldDoc after
    ) {
        return new SinglePassGroupingCollector<>(new GroupingDocValuesSelector.Keyword(groupFieldType), groupField, groupSort, topN, after);
    }

    private final String groupField;
    private final FieldDoc after;
    private final Sort groupSort;
    private final GroupSelector<T> groupSelector;
    private final FieldComparator<?>[] comparators;
    private final LeafFieldComparator[] leafComparators;
    private final int[] reversed;
    private final int topNGroups;
    private final boolean needsScores;
    private final Map<T, SearchGroup<T>> groupMap;
    private final int compIDXEnd;

    private int totalHitCount;

    // Set once we reach topNGroups unique groups:
    private TreeSet<SearchGroup<T>> orderedGroups;

    private int docBase;
    private int spareSlot;

    SinglePassGroupingCollector(
        GroupSelector<T> groupSelector,
        String groupField,
        Sort groupSort,
        int topNGroups,
        @Nullable FieldDoc after
    ) {
        assert after == null || (groupSort.getSort().length == 1 && after.doc == Integer.MAX_VALUE);
        this.groupSelector = groupSelector;
        this.groupField = groupField;
        this.groupSort = groupSort;
        this.after = after;

        if (topNGroups < 1) {
            throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
        }

        this.topNGroups = topNGroups;
        this.needsScores = groupSort.needsScores();
        final SortField[] sortFields = groupSort.getSort();
        comparators = new FieldComparator<?>[sortFields.length];
        leafComparators = new LeafFieldComparator[sortFields.length];
        compIDXEnd = comparators.length - 1;
        reversed = new int[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            final SortField sortField = sortFields[i];
            // use topNGroups + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot):
            comparators[i] = sortField.getComparator(topNGroups + 1, false);
            reversed[i] = sortField.getReverse() ? -1 : 1;
        }
        if (after != null) {
            @SuppressWarnings("unchecked")
            FieldComparator<Object> comparator = (FieldComparator<Object>) comparators[0];
            comparator.setTopValue(after.fields[0]);
        }

        spareSlot = topNGroups;
        groupMap = Maps.newMapWithExpectedSize(topNGroups);
    }

    @Override
    public ScoreMode scoreMode() {
        return needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    /**
     * Returns top groups, starting from offset. This may return null, if no groups were collected, or
     * if the number of unique groups collected is &lt;= offset.
     *
     * @param groupOffset The offset in the collected groups
     * @return top groups, starting from offset
     */
    public TopFieldGroups getTopGroups(int groupOffset) throws IOException {
        if (groupOffset < 0) {
            throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
        }

        if (groupMap.size() <= groupOffset) {
            TotalHits totalHits = new TotalHits(totalHitCount, TotalHits.Relation.EQUAL_TO);
            return new TopFieldGroups(groupField, totalHits, new ScoreDoc[0], groupSort.getSort(), new Object[0]);
        }

        if (orderedGroups == null) {
            buildSortedSet();
        }

        int scorePos = -1;
        for (int index = 0; index < groupSort.getSort().length; index++) {
            SortField sortField = groupSort.getSort()[index];
            if (sortField.getType() == SCORE) {
                scorePos = index;
                break;
            }
        }

        int size = Math.max(0, orderedGroups.size() - groupOffset);
        final FieldDoc[] topDocs = new FieldDoc[size];
        Object[] groupValues = new Object[size];
        final int sortFieldCount = comparators.length;
        int upto = 0;
        int pos = 0;
        for (SearchGroup<T> group : orderedGroups) {
            if (upto++ < groupOffset) {
                continue;
            }
            float score = Float.NaN;
            final Object[] sortValues = new Object[sortFieldCount];
            for (int sortFieldIDX = 0; sortFieldIDX < sortFieldCount; sortFieldIDX++) {
                sortValues[sortFieldIDX] = comparators[sortFieldIDX].value(group.slot);
                if (sortFieldIDX == scorePos) {
                    score = (float) sortValues[sortFieldIDX];
                }
            }
            topDocs[pos] = new FieldDoc(group.doc, score, sortValues);
            groupValues[pos++] = group.groupValue;
        }
        TotalHits totalHits = new TotalHits(totalHitCount, TotalHits.Relation.EQUAL_TO);
        return new TopFieldGroups(groupField, totalHits, topDocs, groupSort.getSort(), groupValues);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        groupSelector.setScorer(scorer);
        for (LeafFieldComparator comparator : leafComparators) {
            comparator.setScorer(scorer);
        }
    }

    private boolean isCompetitive(int doc) throws IOException {
        if (after != null) {
            int cmp = reversed[0] * leafComparators[0].compareTop(doc);
            if (cmp >= 0) {
                return false;
            }
        }

        // If orderedGroups != null we already have collected N groups and
        // can short circuit by comparing this document to the bottom group,
        // without having to find what group this document belongs to.

        // Even if this document belongs to a group in the top N, we'll know that
        // we don't have to update that group.

        // Downside: if the number of unique groups is very low, this is
        // wasted effort as we will most likely be updating an existing group.
        if (orderedGroups != null) {
            for (int compIDX = 0;; compIDX++) {
                final int c = reversed[compIDX] * leafComparators[compIDX].compareBottom(doc);
                if (c < 0) {
                    // Definitely not competitive. So don't even bother to continue
                    return false;
                } else if (c > 0) {
                    // Definitely competitive.
                    break;
                } else if (compIDX == compIDXEnd) {
                    // Here c=0. If we're at the last comparator, this doc is not
                    // competitive, since docs are visited in doc Id order, which means
                    // this doc cannot compete with any other document in the queue.
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void collect(int doc) throws IOException {
        totalHitCount++;
        if (isCompetitive(doc) == false) {
            return;
        }

        // TODO: should we add option to mean "ignore docs that
        // don't have the group field" (instead of stuffing them
        // under null group)?
        groupSelector.advanceTo(doc);
        T groupValue = groupSelector.currentValue();

        final SearchGroup<T> group = groupMap.get(groupValue);

        if (group == null) {

            // First time we are seeing this group, or, we've seen
            // it before but it fell out of the top N and is now
            // coming back

            if (groupMap.size() < topNGroups) {

                // Still in startup transient: we have not
                // seen enough unique groups to start pruning them;
                // just keep collecting them

                // Add a new CollectedSearchGroup:
                SearchGroup<T> sg = new SearchGroup<>(docBase + doc, groupMap.size(), groupSelector.copyValue());
                for (LeafFieldComparator fc : leafComparators) {
                    fc.copy(sg.slot, doc);
                }
                groupMap.put(sg.groupValue, sg);

                if (groupMap.size() == topNGroups) {
                    // End of startup transient: we now have max
                    // number of groups; from here on we will drop
                    // bottom group when we insert new one:
                    buildSortedSet();
                }

                return;
            }

            // We already tested that the document is competitive, so replace
            // the bottom group with this new group.
            final SearchGroup<T> bottomGroup = orderedGroups.pollLast();
            assert orderedGroups.size() == topNGroups - 1;

            groupMap.remove(bottomGroup.groupValue);

            // reuse the removed CollectedSearchGroup
            bottomGroup.groupValue = groupSelector.copyValue();
            bottomGroup.doc = docBase + doc;

            for (LeafFieldComparator fc : leafComparators) {
                fc.copy(bottomGroup.slot, doc);
            }

            groupMap.put(bottomGroup.groupValue, bottomGroup);
            orderedGroups.add(bottomGroup);
            assert orderedGroups.size() == topNGroups;

            final int lastComparatorSlot = orderedGroups.last().slot;
            for (LeafFieldComparator fc : leafComparators) {
                fc.setBottom(lastComparatorSlot);
            }

            return;
        }

        // Update existing group:
        for (int compIDX = 0;; compIDX++) {
            leafComparators[compIDX].copy(spareSlot, doc);

            final int c = reversed[compIDX] * comparators[compIDX].compare(group.slot, spareSlot);
            if (c < 0) {
                // Definitely not competitive.
                return;
            } else if (c > 0) {
                // Definitely competitive; set remaining comparators:
                for (int compIDX2 = compIDX + 1; compIDX2 < comparators.length; compIDX2++) {
                    leafComparators[compIDX2].copy(spareSlot, doc);
                }
                break;
            } else if (compIDX == compIDXEnd) {
                // Here c=0. If we're at the last comparator, this doc is not
                // competitive, since docs are visited in doc Id order, which means
                // this doc cannot compete with any other document in the queue.
                return;
            }
        }

        // Remove before updating the group since lookup is done via comparators
        // TODO: optimize this

        final SearchGroup<T> prevLast;
        if (orderedGroups != null) {
            prevLast = orderedGroups.last();
            orderedGroups.remove(group);
            assert orderedGroups.size() == topNGroups - 1;
        } else {
            prevLast = null;
        }

        group.doc = docBase + doc;

        // Swap slots
        final int tmp = spareSlot;
        spareSlot = group.slot;
        group.slot = tmp;

        // Re-add the changed group
        if (orderedGroups != null) {
            orderedGroups.add(group);
            assert orderedGroups.size() == topNGroups;
            final SearchGroup<?> newLast = orderedGroups.last();
            // If we changed the value of the last group, or changed which group was last, then update
            // bottom:
            if (group == newLast || prevLast != newLast) {
                for (LeafFieldComparator fc : leafComparators) {
                    fc.setBottom(newLast.slot);
                }
            }
        }
    }

    private void buildSortedSet() throws IOException {
        final Comparator<SearchGroup<?>> comparator = (o1, o2) -> {
            for (int compIDX = 0;; compIDX++) {
                FieldComparator<?> fc = comparators[compIDX];
                final int c = reversed[compIDX] * fc.compare(o1.slot, o2.slot);
                if (c != 0) {
                    return c;
                } else if (compIDX == compIDXEnd) {
                    return o1.doc - o2.doc;
                }
            }
        };

        orderedGroups = new TreeSet<>(comparator);
        orderedGroups.addAll(groupMap.values());
        assert orderedGroups.size() > 0;

        for (LeafFieldComparator fc : leafComparators) {
            fc.setBottom(orderedGroups.last().slot);
        }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
        docBase = readerContext.docBase;
        for (int i = 0; i < comparators.length; i++) {
            leafComparators[i] = comparators[i].getLeafComparator(readerContext);
        }
        groupSelector.setNextReader(readerContext);
    }

    /**
     * @return the GroupSelector used for this Collector
     */
    public GroupSelector<T> getGroupSelector() {
        return groupSelector;
    }
}
