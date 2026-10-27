/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the sort type validation done when merging the top docs of shards. The main scenario is a sort on a field that is mapped
 * in some of the searched indices only, with an {@code unmapped_type} that differs from the mapped type: the shards of the indices
 * where the field is not mapped return docs with null sort values and a placeholder sort type that must not conflict.
 */
public class SortFieldValidationTests extends ESTestCase {

    private static final String FIELD = "field";

    public void testPlaceholderDoesNotConflictWithNumericShard() {
        for (boolean inGivenOrder : new boolean[] { true, false }) {
            TopFieldDocs mapped = shard(numeric(SortField.Type.FLOAT, false), 0, 3.14f);
            TopFieldDocs unmapped = shard(placeholder(false, SortField.STRING_LAST), 1, (Object) null);

            Sort sort = validate(inGivenOrder, mapped, unmapped);

            assertThat(sort.getSort()[0].getType(), equalTo(SortField.Type.FLOAT));
            assertThat(((FieldDoc) unmapped.scoreDocs[0]).fields[0], equalTo(Float.POSITIVE_INFINITY));
        }
    }

    public void testPlaceholderDocsAreMergedLastByDefault() {
        for (boolean inGivenOrder : new boolean[] { true, false }) {
            TopFieldDocs mapped = shard(numeric(SortField.Type.DOUBLE, false), 0, 3.14, 5.0);
            TopFieldDocs unmapped = shard(placeholder(false, SortField.STRING_LAST), 10, (Object) null);

            Sort sort = validate(inGivenOrder, mapped, unmapped);

            assertThat(mergedDocIds(sort, mapped, unmapped), equalTo(List.of(0, 1, 10)));
        }
    }

    public void testPlaceholderDocsAreMergedFirstWhenMissingFirst() {
        for (boolean inGivenOrder : new boolean[] { true, false }) {
            TopFieldDocs mapped = shard(numeric(SortField.Type.DOUBLE, false), 0, 3.14, 5.0);
            TopFieldDocs unmapped = shard(placeholder(false, SortField.STRING_FIRST), 10, (Object) null);

            Sort sort = validate(inGivenOrder, mapped, unmapped);

            assertThat(mergedDocIds(sort, mapped, unmapped), equalTo(List.of(10, 0, 1)));
        }
    }

    public void testPlaceholderDocsAreMergedLastOnDescendingSort() {
        for (boolean inGivenOrder : new boolean[] { true, false }) {
            // for a descending sort that puts missing values last, the string sort field is built with STRING_FIRST
            TopFieldDocs mapped = shard(numeric(SortField.Type.DOUBLE, true), 0, 5.0, 3.14);
            TopFieldDocs unmapped = shard(placeholder(true, SortField.STRING_FIRST), 10, (Object) null);

            Sort sort = validate(inGivenOrder, mapped, unmapped);

            assertThat(mergedDocIds(sort, mapped, unmapped), equalTo(List.of(0, 1, 10)));
        }
    }

    public void testPlaceholderDocsAreMergedFirstOnDescendingSortWhenMissingFirst() {
        for (boolean inGivenOrder : new boolean[] { true, false }) {
            TopFieldDocs mapped = shard(numeric(SortField.Type.DOUBLE, true), 0, 5.0, 3.14);
            TopFieldDocs unmapped = shard(placeholder(true, SortField.STRING_LAST), 10, (Object) null);

            Sort sort = validate(inGivenOrder, mapped, unmapped);

            assertThat(mergedDocIds(sort, mapped, unmapped), equalTo(List.of(10, 0, 1)));
        }
    }

    public void testPlaceholderWithIntAndLongShards() {
        TopFieldDocs intShard = shard(numeric(SortField.Type.INT, false), 0, 7);
        TopFieldDocs longShard = shard(numeric(SortField.Type.LONG, false), 1, 8L);
        TopFieldDocs unmapped = shard(placeholder(false, SortField.STRING_LAST), 10, (Object) null);

        Sort sort = validate(true, intShard, longShard, unmapped);

        assertThat(sort.getSort()[0].getType(), equalTo(SortField.Type.LONG));
        assertThat(((FieldDoc) unmapped.scoreDocs[0]).fields[0], equalTo(Long.MAX_VALUE));
        assertThat(mergedDocIds(sort, intShard, longShard, unmapped), equalTo(List.of(0, 1, 10)));
    }

    public void testPlaceholderWithFloatAndLongShards() {
        TopFieldDocs floatShard = shard(numeric(SortField.Type.FLOAT, false), 0, 1.5f);
        TopFieldDocs longShard = shard(numeric(SortField.Type.LONG, false), 1, 8L);
        TopFieldDocs unmapped = shard(placeholder(false, SortField.STRING_FIRST), 10, (Object) null);

        Sort sort = validate(true, unmapped, floatShard, longShard);

        assertThat(sort.getSort()[0].getType(), equalTo(SortField.Type.DOUBLE));
        assertThat(((FieldDoc) unmapped.scoreDocs[0]).fields[0], equalTo(Double.NEGATIVE_INFINITY));
        assertThat(mergedDocIds(sort, unmapped, floatShard, longShard), equalTo(List.of(10, 0, 1)));
    }

    public void testSeveralPlaceholderShards() {
        TopFieldDocs unmapped1 = shard(placeholder(false, SortField.STRING_LAST), 10, (Object) null, null);
        TopFieldDocs mapped = shard(numeric(SortField.Type.LONG, false), 0, 3L);
        TopFieldDocs unmapped2 = shard(placeholder(false, SortField.STRING_LAST), 20, (Object) null);

        Sort sort = validate(true, unmapped1, mapped, unmapped2);

        assertThat(sort.getSort()[0].getType(), equalTo(SortField.Type.LONG));
        assertThat(mergedDocIds(sort, unmapped1, mapped, unmapped2).getFirst(), equalTo(0));
    }

    public void testOnlyPlaceholderShards() {
        TopFieldDocs unmapped1 = shard(placeholder(false, SortField.STRING_LAST), 0, (Object) null);
        TopFieldDocs unmapped2 = shard(placeholder(false, SortField.STRING_LAST), 1, (Object) null);

        Sort sort = validate(true, unmapped1, unmapped2);

        assertThat(sort.getSort()[0].getType(), equalTo(SortField.Type.STRING));
        assertThat(((FieldDoc) unmapped1.scoreDocs[0]).fields[0], nullValue());
        assertThat(((FieldDoc) unmapped2.scoreDocs[0]).fields[0], nullValue());
    }

    public void testStringShardWithValuesStillConflictsWithNumericShard() {
        for (boolean inGivenOrder : new boolean[] { true, false }) {
            TopFieldDocs strings = shard(placeholder(false, SortField.STRING_LAST), 0, new BytesRef("a"));
            TopFieldDocs numbers = shard(numeric(SortField.Type.LONG, false), 1, 8L);

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validate(inGivenOrder, strings, numbers));
            assertThat(e.getMessage(), containsString("incompatible sort types"));
        }
    }

    public void testStringShardWithSomeValuesStillConflictsWithNumericShard() {
        TopFieldDocs strings = shard(placeholder(false, SortField.STRING_LAST), 0, null, new BytesRef("a"));
        TopFieldDocs numbers = shard(numeric(SortField.Type.LONG, false), 2, 8L);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validate(true, strings, numbers));
        assertThat(e.getMessage(), containsString("incompatible sort types"));
    }

    public void testNullValuesWithCustomMissingValueStillConflictWithNumericShard() {
        // a sort field that is neither STRING_FIRST nor STRING_LAST fills the missing values itself, it can't be a placeholder
        TopFieldDocs strings = shard(new SortField(FIELD, SortField.Type.STRING), 0, (Object) null);
        TopFieldDocs numbers = shard(numeric(SortField.Type.LONG, false), 1, 8L);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validate(true, strings, numbers));
        assertThat(e.getMessage(), containsString("incompatible sort types"));
    }

    public void testPlaceholderWithNonNumericNonStringShardStillConflicts() {
        TopFieldDocs scores = shard(SortField.FIELD_SCORE, 0, 1.0f);
        TopFieldDocs unmapped = shard(placeholder(false, SortField.STRING_LAST), 1, (Object) null);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validate(true, scores, unmapped));
        assertThat(e.getMessage(), containsString("incompatible sort types"));
    }

    private static Sort validate(boolean inGivenOrder, TopFieldDocs... shards) {
        List<TopFieldDocs> results = new ArrayList<>(List.of(shards));
        if (inGivenOrder == false) {
            Collections.reverse(results);
        }
        return SortFieldValidation.validateAndMaybeRewrite(results, results.getFirst().fields);
    }

    /** Merges the shards in the given order, the same way as the coordinating node does, and returns the doc ids in merged order. */
    private static List<Integer> mergedDocIds(Sort sort, TopFieldDocs... shards) {
        TopDocs merged = TopDocs.merge(sort, 0, 10, shards);
        List<Integer> docIds = new ArrayList<>();
        for (var scoreDoc : merged.scoreDocs) {
            docIds.add(scoreDoc.doc);
        }
        return docIds;
    }

    private static SortField numeric(SortField.Type type, boolean reverse) {
        return new SortField(FIELD, type, reverse);
    }

    private static SortField placeholder(boolean reverse, Object missingFirstOrLast) {
        return new SortField(FIELD, SortField.Type.STRING, reverse, missingFirstOrLast);
    }

    private static TopFieldDocs shard(SortField sortField, int firstDocId, Object... sortValues) {
        FieldDoc[] docs = new FieldDoc[sortValues.length];
        for (int i = 0; i < sortValues.length; i++) {
            docs[i] = new FieldDoc(firstDocId + i, Float.NaN, new Object[] { sortValues[i] });
        }
        return new TopFieldDocs(new TotalHits(docs.length, TotalHits.Relation.EQUAL_TO), docs, new SortField[] { sortField });
    }
}
