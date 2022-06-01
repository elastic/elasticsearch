/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Port of the C++ class <a href="https://github.com/elastic/ml-cpp/blob/main/include/core/CWordDictionary.h">
 * <code>CWordDictionary</code></a>.
 */
public class CategorizationPartOfSpeechDictionary {

    static final String DICTIONARY_FILE_PATH = "/org/elasticsearch/xpack/ml/aggs/categorization/ml-en.dict";

    static final String PART_OF_SPEECH_SEPARATOR = "@";

    public enum PartOfSpeech {
        NOT_IN_DICTIONARY('\0'),
        UNKNOWN('?'),
        NOUN('N'),
        PLURAL('p'),
        VERB('V'),
        ADJECTIVE('A'),
        ADVERB('v'),
        CONJUNCTION('C'),
        PREPOSITION('P'),
        INTERJECTION('!'),
        PRONOUN('r'),
        DEFINITE_ARTICLE('D'),
        INDEFINITE_ARTICLE('I');

        private final char code;

        PartOfSpeech(char code) {
            this.code = code;
        }

        char getCode() {
            return code;
        }

        private static final Map<Character, PartOfSpeech> CODE_MAPPING =
            // 'h', 'o', 't', and 'i' are codes for specialist types of noun and verb that we don't distinguish
            Stream.concat(
                Map.of('h', NOUN, 'o', NOUN, 't', VERB, 'i', VERB).entrySet().stream(),
                Stream.of(PartOfSpeech.values()).collect(Collectors.toMap(PartOfSpeech::getCode, Function.identity())).entrySet().stream()
            )
                .collect(
                    Collectors.toUnmodifiableMap(Map.Entry<Character, PartOfSpeech>::getKey, Map.Entry<Character, PartOfSpeech>::getValue)
                );

        static PartOfSpeech fromCode(char partOfSpeechCode) {
            PartOfSpeech pos = CODE_MAPPING.get(partOfSpeechCode);
            if (pos == null) {
                throw new IllegalArgumentException("Unknown part-of-speech code [" + partOfSpeechCode + "]");
            }
            return pos;
        }
    }

    /**
     * Lazy loaded singleton instance to avoid loading the dictionary repeatedly.
     */
    private static CategorizationPartOfSpeechDictionary instance;
    private static final Object INIT_LOCK = new Object();

    /**
     * Keys are lower case.
     */
    private final Map<String, PartOfSpeech> partOfSpeechDictionary = new HashMap<>();
    private final int maxDictionaryWordLength;

    CategorizationPartOfSpeechDictionary(InputStream is) throws IOException {

        int maxLength = 0;
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] split = line.split(PART_OF_SPEECH_SEPARATOR);
            if (split.length != 2) {
                throw new IllegalArgumentException(
                    "Unexpected format in line [" + line + "]: expected one [" + PART_OF_SPEECH_SEPARATOR + "] separator"
                );
            }
            if (split[0].isEmpty()) {
                throw new IllegalArgumentException(
                    "Unexpected format in line [" + line + "]: nothing preceding [" + PART_OF_SPEECH_SEPARATOR + "] separator"
                );
            }
            if (split[1].isEmpty()) {
                throw new IllegalArgumentException(
                    "Unexpected format in line [" + line + "]: nothing following [" + PART_OF_SPEECH_SEPARATOR + "] separator"
                );
            }
            String lowerCaseWord = split[0].toLowerCase(Locale.ROOT);
            partOfSpeechDictionary.put(lowerCaseWord, PartOfSpeech.fromCode(split[1].charAt(0)));
            maxLength = Math.max(maxLength, lowerCaseWord.length());
        }
        maxDictionaryWordLength = maxLength;
    }

    // TODO: now we have this in Java, perform this operation in Java for anomaly detection categorization instead of in C++.
    // (It could maybe be incorporated into the categorization analyzer and then shared between aggregation and anomaly detection.)
    /**
     * Find the part of speech (noun, verb, adjective, etc.) for a supplied word.
     * @return Which part of speech does the supplied word represent? {@link PartOfSpeech#NOT_IN_DICTIONARY} is returned
     *         for words that aren't in the dictionary at all.
     */
    public PartOfSpeech getPartOfSpeech(String word) {
        if (word.length() > maxDictionaryWordLength) {
            return PartOfSpeech.NOT_IN_DICTIONARY;
        }
        // This is quite slow as it creates a new string for every lookup. However, experiments show
        // that trying to do case-insensitive comparisons instead of creating a lower case string is
        // even slower.
        return partOfSpeechDictionary.getOrDefault(word.toLowerCase(Locale.ROOT), PartOfSpeech.NOT_IN_DICTIONARY);
    }

    /**
     * @return Is the supplied word in the dictionary?
     */
    public boolean isInDictionary(String word) {
        return getPartOfSpeech(word) != PartOfSpeech.NOT_IN_DICTIONARY;
    }

    public static CategorizationPartOfSpeechDictionary getInstance() throws IOException {
        if (instance != null) {
            return instance;
        }
        synchronized (INIT_LOCK) {
            if (instance == null) {
                try (InputStream is = CategorizationPartOfSpeechDictionary.class.getResourceAsStream(DICTIONARY_FILE_PATH)) {
                    instance = new CategorizationPartOfSpeechDictionary(is);
                }
            }
            return instance;
        }
    }
}
