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
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;

/**
 * A named keyword document shape. Each scenario builds a deterministic corpus flavor (single dense, multi
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
public final class KeywordScenario {

    private final String name;
    private final Supplier<List<KeywordDoc>> builder;

    private KeywordScenario(final String name, final Supplier<List<KeywordDoc>> builder) {
        this.name = name;
        this.builder = builder;
    }

    public String name() {
        return name;
    }

    /**
     * @return a freshly generated corpus for this shape. Called once per duel so both indices in a pair see
     *         the identical document list.
     */
    public List<KeywordDoc> documents() {
        return builder.get();
    }

    public static KeywordScenario singleDense() {
        return new KeywordScenario("single_dense", () -> build(doc -> List.of(KeywordValues.themed())));
    }

    public static KeywordScenario singleSparse() {
        return new KeywordScenario("single_sparse", () -> build(doc -> present() ? List.of(KeywordValues.themed()) : null));
    }

    public static KeywordScenario multiDense() {
        return new KeywordScenario("multi_dense", () -> build(doc -> distinctThemed(randomIntBetween(2, 4))));
    }

    public static KeywordScenario multiSparse() {
        return new KeywordScenario("multi_sparse", () -> build(doc -> present() ? distinctThemed(randomIntBetween(2, 4)) : null));
    }

    public static KeywordScenario nulls() {
        return new KeywordScenario("nulls", () -> build(doc -> switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> withInlineNull(distinctThemed(2));
            case 2 -> List.of(KeywordValues.themed());
            default -> throw new AssertionError("unreachable");
        }));
    }

    public static KeywordScenario emptyArrays() {
        return new KeywordScenario(
            "empty_arrays",
            () -> build(doc -> randomBoolean() ? List.of() : distinctThemed(randomIntBetween(1, 3)))
        );
    }

    public static KeywordScenario duplicates() {
        // This is the only shape that repeats a value within a document (for example [alpha, alpha, bravo]);
        // every other multi-valued shape is kept distinct. Both strict columnar layouts preserve the repeats in
        // doc values, which the value-multiplicity checks verify the baseline and ColumNAR agree on.
        return new KeywordScenario("duplicates", () -> build(doc -> {
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

    public static KeywordScenario highCardinality() {
        return new KeywordScenario("high_cardinality", () -> build(doc -> List.of(KeywordValues.unique(doc))));
    }

    public static KeywordScenario unicode() {
        return new KeywordScenario("unicode", () -> build(doc -> distinct(randomList(1, 3, KeywordValues::unicode))));
    }

    public static KeywordScenario longValues() {
        return new KeywordScenario("long_values", () -> build(doc -> distinct(randomList(1, 2, KeywordValues::longValue))));
    }

    /**
     * Indexes empty strings alongside nulls and regular values. A term query on {@code ""} rewrites to
     * {@code BinaryDocValuesLengthQuery}, which is a distinct execution path from non-empty terms; including
     * nulls in the same corpus verifies the codec keeps a null slot and an empty value apart.
     */
    public static KeywordScenario emptyStrings() {
        return new KeywordScenario("empty_strings", () -> build(doc -> switch (randomInt(3)) {
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
    public static KeywordScenario largeCorpus() {
        return new KeywordScenario("large_corpus", () -> build(randomIntBetween(300, 500), doc -> List.of(KeywordValues.themed())));
    }

    public static KeywordScenario randomizedMixed() {
        return new KeywordScenario("randomized_mixed", () -> build(doc -> switch (randomInt(6)) {
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

    private static List<KeywordDoc> build(final DocValues perDoc) {
        return build(randomIntBetween(30, 120), perDoc);
    }

    private static List<KeywordDoc> build(final int count, final DocValues perDoc) {
        final List<KeywordDoc> docs = new ArrayList<>(count);
        for (long docId = 0; docId < count; docId++) {
            docs.add(new KeywordDoc(docId, perDoc.valuesFor(docId)));
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
