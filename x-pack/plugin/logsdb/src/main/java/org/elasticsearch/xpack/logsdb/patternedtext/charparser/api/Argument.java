/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

/**
 * Represents a typed argument extracted from a text message.
 * <p>
 * An argument holds the original value and its encoding type, and can provide a string representation of the value.
 *
 * @param <T> the type of the argument's value
 */
public interface Argument<T> {
    /**
     * Returns the original value of the argument.
     *
     * @return the argument's value
     */
    T value();

    /**
     * Returns the encoding type of the argument.
     *
     * @return the encoding type
     */
    EncodingType type();

    /**
     * Returns the start position (first character) of the text that was used to extract this argument in the original text.
     * @return the start position (inclusive)
     */
    int startPosition();

    /**
     * Returns the length (number of characters) of the text that was used to extract this argument in the original text.
     * @return the length
     */
    int length();

    /**
     * Returns a string representation of the argument's value.
     *
     * @return the string representation of the value
     */
    String encode();
}
