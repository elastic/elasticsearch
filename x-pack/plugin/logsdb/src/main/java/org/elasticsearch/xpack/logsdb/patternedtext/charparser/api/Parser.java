/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

/**
 * Interface for parsing raw text messages into structured patterns with typed arguments.
 *
 * <p>Implementations of this interface are responsible for analyzing input text and extracting
 * meaningful patterns, timestamps, and parameter values.
 * The parsing process converts unstructured log messages into a standardized format that
 * separates the static template from the variable data.
 *
 * <p>The parser operates by recognizing tokens and sub-tokens within the input text,
 * matching them against configured patterns, and producing a structured representation
 * that includes:
 * <ul>
 *     <li>A template string with parameter placeholders</li>
 *     <li>Extracted timestamp information (if present)</li>
 *     <li>Typed arguments corresponding to the template parameters</li>
 * </ul>
 */
public interface Parser {

    /**
     * Parses a raw text message into a structured pattern with extracted components.
     *
     * <p>This method analyzes the input message and extracts variable data while preserving the overall structure as a template.
     *
     * <p><strong>Example:</strong>
     * <pre>
     * Input:  "2023-10-05 14:30:25 INFO received 305 packets from 135.122.123.222"
     * Output: PatternedMessage with:
     *         - template: "%T INFO received %I packets from %4"
     *         - timestamp: parsed datetime value
     *         - arguments: [Integer:{305}, IPv4:{135.122.123.222}]
     * </pre>
     *
     * @param rawMessage the input text message to parse, must not be null
     * @return a {@link PatternedMessage} containing the extracted template, timestamp, and typed arguments
     * @throws IllegalArgumentException if rawMessage is null
     * @throws ParseException if a parsing error occurs
     */
    PatternedMessage parse(String rawMessage) throws ParseException;
}
