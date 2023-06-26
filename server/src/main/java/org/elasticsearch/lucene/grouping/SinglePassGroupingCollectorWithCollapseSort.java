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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.GroupSelector;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
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
public class SinglePassGroupingCollectorWithCollapseSort<T> extends SinglePassGroupingCollector<T> {

    /**
     * Creates a {@link SinglePassGroupingCollector} on a {@link NumericDocValues} field.
     * It accepts also {@link SortedNumericDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param groupField        The sort field used to group documents.
     * @param groupFieldType    The {@link MappedFieldType} for this sort field.
     * @param topSort           The {@link Sort} used to sort the groups.
     *                          The grouping keeps only the top sorted document per grouping key.
     *                          This must be non-null, ie, if you want to topSort by relevance
     *                          use Sort.RELEVANCE.
     * @param collapseSort      {@link Sort} used to select head hit for each group. If absent, defaults to Top Sort,
     *                          which corresponds to the current behaviour.
     * @param topN              How many top groups to keep.
     * @param totalNumDocs      How many possible top groups.
     * @param after             The field values to search after. Can be null.
     */
    public static SinglePassGroupingCollectorWithCollapseSort<?> createNumeric(
        String groupField,
        MappedFieldType groupFieldType,
        Sort topSort,
        @Nullable Sort collapseSort,
        int topN,
        int totalNumDocs,
        @Nullable FieldDoc after
    ) {
        return new SinglePassGroupingCollectorWithCollapseSort<>(
            new GroupingDocValuesSelector.Numeric(groupFieldType),
            groupField,
            topSort,
            collapseSort,
            topN,
            totalNumDocs,
            after);
    }

    /**
     * Creates a {@link SinglePassGroupingCollector} on a {@link SortedDocValues} field.
     * It accepts also {@link SortedSetDocValues} field but the collect will fail with
     * an {@link IllegalStateException} if a document contains more than one value for the field.
     *
     * @param groupField        The sort field used to group documents.
     * @param groupFieldType    The {@link MappedFieldType} for this sort field.
     * @param topSort           The {@link Sort} used to sort the groups.
     *                          The grouping keeps only the top sorted document per grouping key.
     *                          This must be non-null, ie, if you want to topSort by relevance
     *                          use Sort.RELEVANCE.
     * @param collapseSort      {@link Sort} used to select head hit for each group. If absent, defaults to Top Sort,
     *                          which corresponds to the current behaviour.
     * @param topN              How many top groups to keep.
     * @param totalNumDocs      How many possible top groups.
     * @param after             The field values to search after. Can be null.
     */
    public static SinglePassGroupingCollectorWithCollapseSort<?> createKeyword(
        String groupField,
        MappedFieldType groupFieldType,
        Sort topSort,
        @Nullable Sort collapseSort,
        int topN,
        int totalNumDocs,
        @Nullable FieldDoc after
    ) {
        return new SinglePassGroupingCollectorWithCollapseSort<>(
            new GroupingDocValuesSelector.Keyword(groupFieldType),
            groupField,
            topSort,
            collapseSort,
            topN,
            totalNumDocs,
            after);
    }

    private final String groupField;
    private final FieldDoc after;
    private final Sort topSort;
    protected final Sort collapseSort;
    private final GroupSelector<T> groupSelector;
    private final FieldComparator<?>[] collapseComparators;
    private final FieldComparator<?>[] topComparators;
    private final LeafFieldComparator[] collapseLeafComparators;
    private final LeafFieldComparator[] topLeafComparators;
    private final int[] collapseReversed;
    private final int[] topReversed;
    private final int topNGroups;
    private final boolean needsScores;
    private final Map<T, SearchGroup<T>> groupMap;
    private final int collapseCompIDXEnd;
    private final int topCompIDXEnd;

    private int totalHitCount;

    private TreeSet<SearchGroup<T>> orderedGroups;

    private int docBase;
    private int spareSlot;

    private SinglePassGroupingCollectorWithCollapseSort(
        GroupSelector<T> groupSelector,
        String groupField,
        Sort topSort,
        @Nullable Sort collapseSort,
        int topNGroups,
        int totalNumDocs,
        @Nullable FieldDoc after
    ) {
        super(groupSelector, groupField, topSort, topNGroups, after);
        assert after == null || (topSort.getSort().length == 1 && after.doc == Integer.MAX_VALUE);
        this.groupSelector = groupSelector;
        this.groupField = groupField;
        this.topSort = topSort;
        this.collapseSort = collapseSort == null ? topSort : collapseSort;
        this.after = after;

        if (topNGroups < 1) {
            throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
        }

        this.topNGroups = topNGroups;
        this.needsScores = topSort.needsScores();
        final SortField[] collapseSortFields = this.collapseSort.getSort();
        final SortField[] topSortFields = this.topSort.getSort();
        collapseComparators = new FieldComparator<?>[collapseSortFields.length];
        topComparators = new FieldComparator<?>[topSortFields.length];
        collapseLeafComparators = new LeafFieldComparator[collapseSortFields.length];
        topLeafComparators = new LeafFieldComparator[topSortFields.length];
        collapseCompIDXEnd = collapseComparators.length - 1;
        topCompIDXEnd = topLeafComparators.length - 1;
        collapseReversed = new int[collapseSortFields.length];
        topReversed = new int[topSortFields.length];
        for (int i = 0; i < collapseSortFields.length; i++) {
            final SortField sortField = collapseSortFields[i];
            // use totalNumDocs + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot):
            collapseComparators[i] = sortField.getComparator(totalNumDocs + 1, false);
            collapseReversed[i] = sortField.getReverse() ? -1 : 1;
        }
        for (int i = 0; i < topSortFields.length; i++) {
            final SortField sortField = topSortFields[i];
            // use totalNumDocs + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot):
            topComparators[i] = sortField.getComparator(totalNumDocs, false);
            topReversed[i] = sortField.getReverse() ? -1 : 1;
        }
        if (after != null) {
            @SuppressWarnings("unchecked")
            FieldComparator<Object> topComparator = (FieldComparator<Object>) topComparators[0];
            topComparator.setTopValue(after.fields[0]);
        }

        spareSlot = totalNumDocs;
        groupMap = new HashMap<>();
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
            return new TopFieldGroups(groupField, totalHits,
                new ScoreDoc[0], topSort.getSort(), new ScoreDoc[0], collapseSort.getSort(),
                new Object[0]);
        }

        if (orderedGroups == null) {
            buildSortedSet();
        }

        int scorePos = -1;
        for (int index = 0; index < topSort.getSort().length; index++) {
            SortField sortField = topSort.getSort()[index];
            if (sortField.getType() == SCORE) {
                scorePos = index;
                break;
            }
        }

        int orderedGroupsSize = Math.min(topNGroups, orderedGroups.size());
        int size = Math.max(0, orderedGroupsSize - groupOffset);
        final FieldDoc[] topDocs = new FieldDoc[size];
        final FieldDoc[] collapseDocs = new FieldDoc[size];
        Object[] groupValues = new Object[size];
        final int topSortFieldCount = topComparators.length;
        final int collapseSortFieldCount = collapseComparators.length;
        int upto = 0;
        int pos = 0;
        for (SearchGroup<T> group : orderedGroups) {
            if (upto > size - 1) {
                break;
            }
            if (upto++ < groupOffset) {
                continue;
            }
            float score = Float.NaN;
            final Object[] topSortValues = new Object[topSortFieldCount];
            final Object[] collapseSortValues = new Object[collapseSortFieldCount];

            for (int sortFieldIDX = 0; sortFieldIDX < topSortFieldCount; sortFieldIDX++) {
                topSortValues[sortFieldIDX] = topComparators[sortFieldIDX].value(group.slot);
                if (sortFieldIDX == scorePos) {
                    score = (float) topSortValues[sortFieldIDX];
                }
            }
            for (int sortFieldIDX = 0; sortFieldIDX < collapseSortFieldCount; sortFieldIDX++) {
                collapseSortValues[sortFieldIDX] = collapseComparators[sortFieldIDX].value(group.slot);
            }

            topDocs[pos] = new FieldDoc(group.doc, score, topSortValues);
            collapseDocs[pos] = new FieldDoc(group.doc, Float.NaN, collapseSortValues);

            groupValues[pos++] = group.groupValue;
        }
        TotalHits totalHits = new TotalHits(totalHitCount, TotalHits.Relation.EQUAL_TO);
        return new TopFieldGroups(groupField, totalHits, topDocs, topSort.getSort(), collapseDocs, collapseSort.getSort(), groupValues);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        groupSelector.setScorer(scorer);
        for (LeafFieldComparator comparator : topLeafComparators) {
            comparator.setScorer(scorer);
        }
        for (LeafFieldComparator comparator : collapseLeafComparators) {
            comparator.setScorer(scorer);
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        totalHitCount++;
        if (after != null) {
            int cmp = topReversed[0] * topLeafComparators[0].compareTop(doc);
            if (cmp >= 0) {
                return;
            }
        }

        // TODO: should we add option to mean "ignore docs that
        // don't have the group field" (instead of stuffing them
        // under null group)?
        groupSelector.advanceTo(doc);
        T groupValue = groupSelector.currentValue();

        final SearchGroup<T> group = groupMap.get(groupValue);

        // First time we are seeing this group
        if (group == null) {
            // Add a new CollectedSearchGroup:
            SearchGroup<T> sg = new SearchGroup<>(docBase + doc, groupMap.size(), groupSelector.copyValue());
            for (LeafFieldComparator fc : topLeafComparators) {
                fc.copy(sg.slot, doc);
            }
            for (LeafFieldComparator fc : collapseLeafComparators) {
                fc.copy(sg.slot, doc);
            }
            groupMap.put(sg.groupValue, sg);

            return;
        }

        // Update existing group:
        for (int compIDX = 0;; compIDX++) {
            collapseLeafComparators[compIDX].copy(spareSlot, doc);

            final int c = collapseReversed[compIDX] * collapseComparators[compIDX].compare(group.slot, spareSlot);
            if (c < 0) {
                // Definitely not competitive.
                return;
            } else if (c > 0) {
                // Definitely competitive.
                group.doc = docBase + doc;
                for (LeafFieldComparator fc : topLeafComparators) {
                    fc.copy(group.slot, doc);
                }
                for (LeafFieldComparator fc : collapseLeafComparators) {
                    fc.copy(group.slot, doc);
                }
                break;
            } else if (compIDX == collapseCompIDXEnd) {
                // Here c=0. If we're at the last comparator, this doc is not
                // competitive, since docs are visited in doc Id order, which means
                // this doc cannot compete with any other document in the queue.
                return;
            }
        }
    }

    private void buildSortedSet() throws IOException {
        final Comparator<SearchGroup<?>> comparator = (o1, o2) -> {
            for (int compIDX = 0;; compIDX++) {
                FieldComparator<?> fc = topComparators[compIDX];
                final int c = topReversed[compIDX] * fc.compare(o1.slot, o2.slot);
                if (c != 0) {
                    return c;
                } else if (compIDX == topCompIDXEnd) {
                    return o1.doc - o2.doc;
                }
            }
        };

        orderedGroups = new TreeSet<>(comparator);
        orderedGroups.addAll(groupMap.values());
        assert orderedGroups.size() > 0;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
        docBase = readerContext.docBase;
        for (int i = 0; i < topComparators.length; i++) {
            topLeafComparators[i] = topComparators[i].getLeafComparator(readerContext);
        }
        for (int i = 0; i < collapseComparators.length; i++) {
            collapseLeafComparators[i] = collapseComparators[i].getLeafComparator(readerContext);
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
