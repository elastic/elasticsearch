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
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Generates text literals for a duel corpus. Every word is one token under the standard analyzer, and a value is
 * words joined by single spaces, optionally capitalized and closed with one punctuation mark. That keeps
 * {@link #tokens} an exact, independent model of the analysis a query sees, so query oracles never depend on the
 * code under test. Arbitrary random unicode is left out on purpose: the standard tokenizer would split it in ways
 * the model cannot predict. All randomness flows through {@link org.elasticsearch.test.ESTestCase}.
 */
public final class TextValues {

    private static final List<String> ASCII_WORDS = List.of(
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa",
        "quebec",
        "romeo",
        "sierra",
        "tango",
        "uniform",
        "victor",
        "whiskey",
        "xray",
        "yankee",
        "zulu",
        "amber",
        "basalt",
        "cobalt",
        "dune"
    );

    private static final List<String> UNICODE_WORDS = List.of(
        "caf\u00e9",
        "na\u00efve",
        "stra\u00dfe",
        "\u00fcber",
        "se\u00f1or",
        "\u03b5\u03bb\u03bb\u03b7\u03bd\u03b9\u03ba\u03ac",
        "\u0440\u0443\u0441\u0441\u043a\u0438\u0439"
    );

    private static final List<String> ALL_WORDS = concat(ASCII_WORDS, UNICODE_WORDS);

    private static final List<String> CLOSING_PUNCTUATION = List.of(".", ",");

    private TextValues() {}

    /**
     * @return a lowercase ASCII word, which stays unchanged under analysis.
     */
    public static String asciiWord() {
        return randomFrom(ASCII_WORDS);
    }

    /**
     * @return a lowercase word that is either ASCII or non-ASCII.
     */
    public static String word() {
        return randomFrom(ALL_WORDS);
    }

    /**
     * @return a single word, capitalized about half of the time.
     */
    public static String singleWord() {
        return randomBoolean() ? word() : capitalize(word());
    }

    /**
     * @return between {@code minWords} and {@code maxWords} words, capitalized and punctuated at random.
     */
    public static String sentence(int minWords, int maxWords) {
        return compose(randomIntBetween(minWords, maxWords), ALL_WORDS);
    }

    /**
     * @return a sentence made only of non-ASCII words.
     */
    public static String unicodeSentence() {
        return compose(randomIntBetween(1, 6), UNICODE_WORDS);
    }

    /**
     * @return a sentence long enough to span several kilobytes, which exercises the large-value path of a codec.
     */
    public static String longSentence() {
        return compose(randomIntBetween(200, 1500), ALL_WORDS);
    }

    /**
     * @return the tokens the standard analyzer produces for a value made by this class: lowercase words with the
     *         closing punctuation removed. An empty value has no tokens.
     */
    public static List<String> tokens(final String value) {
        final List<String> tokens = new ArrayList<>();
        for (final String word : value.split(" ")) {
            final String stripped = stripClosingPunctuation(word);
            if (stripped.isEmpty() == false) {
                tokens.add(lowercase(stripped));
            }
        }
        return tokens;
    }

    private static String compose(int words, final List<String> vocabulary) {
        final StringBuilder sentence = new StringBuilder();
        for (int i = 0; i < words; i++) {
            if (i > 0) {
                sentence.append(' ');
            }
            sentence.append(randomFrom(vocabulary));
        }
        final String composed = randomBoolean() ? capitalize(sentence.toString()) : sentence.toString();
        return randomBoolean() ? composed + randomFrom(CLOSING_PUNCTUATION) : composed;
    }

    private static String capitalize(final String value) {
        final int first = value.codePointAt(0);
        return new StringBuilder().appendCodePoint(Character.toUpperCase(first))
            .append(value, Character.charCount(first), value.length())
            .toString();
    }

    private static String lowercase(final String value) {
        final StringBuilder lowered = new StringBuilder(value.length());
        value.codePoints().forEach(codePoint -> lowered.appendCodePoint(Character.toLowerCase(codePoint)));
        return lowered.toString();
    }

    private static String stripClosingPunctuation(final String word) {
        return CLOSING_PUNCTUATION.stream().anyMatch(word::endsWith) ? word.substring(0, word.length() - 1) : word;
    }

    private static List<String> concat(final List<String> first, final List<String> second) {
        final List<String> all = new ArrayList<>(first);
        all.addAll(second);
        return List.copyOf(all);
    }
}
