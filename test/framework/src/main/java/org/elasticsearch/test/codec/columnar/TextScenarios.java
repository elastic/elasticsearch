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
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * The named text document shapes. Each scenario builds a corpus from {@link TextValues}, with document counts and
 * per-document choices randomized through {@link org.elasticsearch.test.ESTestCase} so a corpus reproduces from a
 * test seed while the shape stays fixed.
 *
 * <p>Every corpus opens with a lowercase one-word document and a multi-word document, whatever the shape, so the
 * whole-value and phrase probes a check derives from the corpus always exist and never compare zero hits against
 * zero hits.
 */
public final class TextScenarios {

    private TextScenarios() {}

    public static DuelScenario sentencesDense() {
        return new DuelScenario("sentences_dense", () -> build(doc -> List.of(wordOrSentence())));
    }

    public static DuelScenario sentencesSparse() {
        return new DuelScenario("sentences_sparse", () -> build(doc -> randomIntBetween(0, 9) < 6 ? List.of(wordOrSentence()) : null));
    }

    public static DuelScenario arrays() {
        return new DuelScenario("arrays", () -> build(doc -> switch (randomInt(5)) {
            case 0 -> null;
            case 1 -> List.of();
            case 2, 3 -> withInlineNull(distinct(randomIntBetween(2, 4)));
            case 4 -> repeated();
            case 5 -> distinct(randomIntBetween(1, 4));
            default -> throw new AssertionError("unreachable");
        }));
    }

    public static DuelScenario unicode() {
        return new DuelScenario(
            "unicode",
            () -> build(doc -> randomBoolean() ? List.of(TextValues.unicodeSentence()) : List.of(TextValues.singleWord()))
        );
    }

    public static DuelScenario longValues() {
        return new DuelScenario("long_values", () -> build(doc -> switch (randomInt(2)) {
            case 0 -> List.of(TextValues.longSentence());
            case 1 -> List.of(TextValues.longSentence(), TextValues.sentence(2, 6));
            case 2 -> List.of(wordOrSentence());
            default -> throw new AssertionError("unreachable");
        }));
    }

    public static DuelScenario emptyStrings() {
        return new DuelScenario("empty_strings", () -> build(doc -> switch (randomInt(3)) {
            case 0 -> List.of("");
            case 1 -> null;
            case 2 -> List.of("", wordOrSentence());
            case 3 -> List.of(wordOrSentence());
            default -> throw new AssertionError("unreachable");
        }));
    }

    public static DuelScenario largeCorpus() {
        return new DuelScenario("large_corpus", () -> build(randomIntBetween(300, 500), doc -> List.of(wordOrSentence())));
    }

    public static DuelScenario singleValued() {
        return new DuelScenario("single_valued", () -> build(randomIntBetween(30, 120), doc -> switch (randomInt(5)) {
            case 0 -> null;
            case 1 -> List.of(TextValues.singleWord());
            case 2 -> List.of(TextValues.sentence(2, 12));
            case 3 -> List.of(TextValues.unicodeSentence());
            case 4 -> List.of(TextValues.longSentence());
            case 5 -> List.of("");
            default -> throw new AssertionError("unreachable");
        }), false);
    }

    public static DuelScenario randomizedMixed() {
        return new DuelScenario("randomized_mixed", () -> build(doc -> switch (randomInt(6)) {
            case 0 -> null;
            case 1 -> List.of();
            case 2 -> List.of(TextValues.singleWord());
            case 3 -> distinct(randomIntBetween(2, 4));
            case 4 -> withInlineNull(distinct(2));
            case 5 -> List.of(TextValues.unicodeSentence());
            case 6 -> List.of(TextValues.longSentence());
            default -> throw new AssertionError("unreachable");
        }));
    }

    private static List<DuelDoc> build(final DocValues perDoc) {
        return build(randomIntBetween(30, 120), perDoc);
    }

    private static List<DuelDoc> build(int count, final DocValues perDoc) {
        final List<DuelDoc> docs = new ArrayList<>(count);
        for (long docId = 0; docId < count; docId++) {
            final List<String> values = switch ((int) docId) {
                case 0 -> List.of(TextValues.asciiWord());
                case 1 -> List.of(TextValues.sentence(2, 12));
                default -> perDoc.valuesFor(docId);
            };
            docs.add(new DuelDoc(docId, values));
        }
        return docs;
    }

    private static String wordOrSentence() {
        return randomInt(3) == 0 ? TextValues.singleWord() : TextValues.sentence(2, 12);
    }

    private static List<String> distinct(int size) {
        final List<String> values = new ArrayList<>(size);
        while (values.size() < size) {
            final String candidate = wordOrSentence();
            if (values.contains(candidate) == false) {
                values.add(candidate);
            }
        }
        return values;
    }

    private static List<String> repeated() {
        final String repeated = wordOrSentence();
        final List<String> values = new ArrayList<>();
        for (int i = randomIntBetween(2, 3); i > 0; i--) {
            values.add(repeated);
        }
        values.add(wordOrSentence());
        return values;
    }

    private static List<String> withInlineNull(final List<String> values) {
        final List<String> withNull = new ArrayList<>(values);
        withNull.add(Math.min(1, withNull.size()), null);
        return withNull;
    }

    @FunctionalInterface
    private interface DocValues {
        List<String> valuesFor(long docId);
    }
}
