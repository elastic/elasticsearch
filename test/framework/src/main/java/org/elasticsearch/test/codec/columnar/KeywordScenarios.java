/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;

/**
 * The named keyword document shapes. Each scenario builds a deterministic corpus flavor (single dense, multi
 * sparse, nulls, duplicates, high cardinality, unicode, long values, and so on) from {@link KeywordValues}.
 * Scenarios own document structure only; the literals come from {@link KeywordValues}. Document counts and
 * per-document choices are randomized through {@link org.elasticsearch.test.ESTestCase}, so a corpus
 * reproduces from a test seed while the shape stays fixed.
 *
 * <p>All multi-valued shapes except {@link #duplicates} generate distinct values within a document. The
 * {@link #duplicates} scenario keeps repeats on purpose: both strict columnar layouts preserve repeated
 * keyword values in doc values, and the value-multiplicity checks assert the baseline and ColumNAR agree on
 * those preserved duplicate entries.
 */
public final class KeywordScenarios {

    private KeywordScenarios() {}

    public static DuelScenario singleDense() {
        return new DuelScenario("single_dense", () -> build(doc -> List.of(KeywordValues.themed())));
    }

    public static DuelScenario singleSparse() {
        return new DuelScenario("single_sparse", () -> build(doc -> present() ? List.of(KeywordValues.themed()) : null));
    }

    public static DuelScenario multiDense() {
        return new DuelScenario("multi_dense", () -> build(doc -> distinctThemed(randomIntBetween(2, 4))));
    }

    public static DuelScenario multiSparse() {
        return new DuelScenario("multi_sparse", () -> build(doc -> present() ? distinctThemed(randomIntBetween(2, 4)) : null));
    }

    public static DuelScenario nulls() {
        return new DuelScenario("nulls", () -> build(doc -> switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> withInlineNull(distinctThemed(2));
            case 2 -> List.of(KeywordValues.themed());
            default -> throw new AssertionError("unreachable");
        }));
    }

    public static DuelScenario emptyArrays() {
        return new DuelScenario("empty_arrays", () -> build(doc -> randomBoolean() ? List.of() : distinctThemed(randomIntBetween(1, 3))));
    }

    public static DuelScenario duplicates() {
        // This is the only shape that repeats a value within a document (for example [alpha, alpha, bravo]);
        // every other multi-valued shape is kept distinct. Both strict columnar layouts preserve the repeats in
        // doc values, which the value-multiplicity checks verify the baseline and ColumNAR agree on.
        return new DuelScenario("duplicates", () -> build(doc -> {
            final String repeated = KeywordValues.themed();
            final List<String> values = new ArrayList<>();
            final int copies = randomIntBetween(2, 4);
            for (int i = 0; i < copies; i++) {
                values.add(repeated);
            }
            values.add(KeywordValues.themed());
            return values;
        }));
    }

    public static DuelScenario highCardinality() {
        return new DuelScenario("high_cardinality", () -> build(doc -> List.of(KeywordValues.unique(doc))));
    }

    public static DuelScenario unicode() {
        return new DuelScenario("unicode", () -> build(doc -> distinct(randomList(1, 3, KeywordValues::unicode))));
    }

    public static DuelScenario longValues() {
        return new DuelScenario("long_values", () -> build(doc -> distinct(randomList(1, 2, KeywordValues::longValue))));
    }

    /**
     * Indexes empty strings alongside nulls and regular values. A term query on {@code ""} rewrites to
     * {@code BinaryDocValuesLengthQuery}, which is a distinct execution path from non-empty terms; including
     * nulls in the same corpus verifies the codec keeps a null slot and an empty value apart.
     */
    public static DuelScenario emptyStrings() {
        return new DuelScenario("empty_strings", () -> build(doc -> switch (randomInt(3)) {
            case 0 -> List.of(KeywordValues.emptyString());
            case 1 -> null;
            case 2 -> List.of(KeywordValues.emptyString(), KeywordValues.themed());
            case 3 -> List.of(KeywordValues.themed());
            default -> throw new AssertionError("unreachable");
        }));
    }

    /**
     * Generates a corpus above the default Lucene block size (128 values) to exercise the ordinal block
     * cursor and the single-chunk read cache, which only activate beyond that threshold.
     */
    public static DuelScenario largeCorpus() {
        return new DuelScenario("large_corpus", () -> build(randomIntBetween(300, 500), doc -> List.of(KeywordValues.themed())));
    }

    /**
     * A field mapped {@code doc_values.multi_value: false}, so every document holds one value or none. The values span
     * the shapes the multi-valued scenarios cover one at a time - repeated, unique, unicode, long, and empty - so a
     * single corpus exercises the single-valued layout with a dictionary, escapes, and variable lengths.
     */
    public static DuelScenario singleValued() {
        return new DuelScenario("single_valued", () -> build(doc -> switch (randomInt(5)) {
            case 0 -> null;
            case 1 -> List.of(KeywordValues.themed());
            case 2 -> List.of(KeywordValues.unique(doc));
            case 3 -> List.of(KeywordValues.unicode());
            case 4 -> List.of(KeywordValues.longValue());
            case 5 -> List.of(KeywordValues.emptyString());
            default -> throw new AssertionError("unreachable");
        }), false);
    }

    public static DuelScenario randomizedMixed() {
        return new DuelScenario("randomized_mixed", () -> build(doc -> switch (randomInt(6)) {
            case 0 -> null;
            case 1 -> List.of();
            case 2 -> List.of(KeywordValues.any());
            case 3 -> distinct(randomList(2, 4, KeywordValues::any));
            case 4 -> withInlineNull(distinct(randomList(2, 2, KeywordValues::any)));
            case 5 -> List.of(KeywordValues.unique(doc));
            case 6 -> distinct(randomList(1, 2, KeywordValues::longValue));
            default -> throw new AssertionError("unreachable");
        }));
    }

    private static List<DuelDoc> build(final DocValues perDoc) {
        return build(randomIntBetween(30, 120), perDoc);
    }

    private static List<DuelDoc> build(final int count, final DocValues perDoc) {
        final List<DuelDoc> docs = new ArrayList<>(count);
        for (long docId = 0; docId < count; docId++) {
            docs.add(new DuelDoc(docId, perDoc.valuesFor(docId)));
        }
        return docs;
    }

    private static List<String> distinctThemed(int size) {
        return randomSubsetOf(Math.min(size, KeywordValues.THEMED.size()), KeywordValues.THEMED);
    }

    private static List<String> distinct(final List<String> values) {
        return new ArrayList<>(new LinkedHashSet<>(values));
    }

    private static List<String> withInlineNull(final List<String> values) {
        final List<String> withNull = new ArrayList<>(values);
        withNull.add(Math.min(1, withNull.size()), null);
        return withNull;
    }

    private static boolean present() {
        return randomIntBetween(0, 9) < 6;
    }

    @FunctionalInterface
    private interface DocValues {
        List<String> valuesFor(long docId);
    }
}
