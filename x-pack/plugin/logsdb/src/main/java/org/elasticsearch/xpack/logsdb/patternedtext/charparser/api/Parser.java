/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import java.util.List;

/**
 * Interface for parsing raw text messages into structured patterns with typed arguments.
 *
 * <p>Implementations of this interface are responsible for analyzing input text, identifying static parts and extracting
 * dynamic parts into arguments (like timestamps, numbers, hexadecimal etc.).
 *
 * <p>The parser operates by recognizing tokens and sub-tokens within the input text, matching them against configured patterns, and
 * producing an ordered list of typed arguments, where the details about each argument include:
 * <ul>
 *     <li>the type of the argument</li>
 *     <li>the extracted value (e.g. number for numeric arguments, millis since epoch for timestamps etc.)</li>
 *     <li>the start and end position of the text that was used for argument extraction within the input message</li>
 * </ul>
 */
public interface Parser {

    char PLACEHOLDER_PREFIX = '%';

    /**
     * Parses a raw text message and extracts an ordered list of typed arguments. The first argument of type {@link Timestamp} is THE
     * timestamp of the message.
     *
     * @param rawMessage the raw text message to parse
     * @return an ordered list of typed arguments extracted from the message, including start and end positions within the original text
     * @throws ParseException if the message cannot be parsed
     */
    List<Argument<?>> parse(String rawMessage) throws ParseException;

    /**
     * Constructs a pattern string from the raw message and the list of extracted arguments.
     * @param rawMessage the original raw message
     * @param arguments the list of extracted arguments
     * @param patternedMessage a StringBuilder to append the constructed pattern to
     * @param putPlaceholders if true, placeholders will be used for arguments; if false, the argument parts will be omitted from the
     *                        pattern
     */
    static void constructPattern(String rawMessage, List<Argument<?>> arguments, StringBuilder patternedMessage, boolean putPlaceholders) {
        patternedMessage.setLength(0);
        int currentIndex = 0;
        for (Argument<?> argument : arguments) {
            int argStart = argument.startPosition();
            int argEnd = argStart + argument.length();
            // Append the static part before the argument
            if (currentIndex < argStart) {
                patternedMessage.append(rawMessage, currentIndex, argStart);
            }
            // Append the argument placeholder or skip it
            if (putPlaceholders) {
                patternedMessage.append(PLACEHOLDER_PREFIX).append(argument.type().getSymbol());
            }
            currentIndex = argEnd;
        }
        // Append any remaining static part after the last argument
        if (currentIndex < rawMessage.length()) {
            patternedMessage.append(rawMessage, currentIndex, rawMessage.length());
        }
    }
}
