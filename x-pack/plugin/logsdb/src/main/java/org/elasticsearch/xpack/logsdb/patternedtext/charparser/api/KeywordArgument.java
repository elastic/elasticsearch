/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

/**
 * Represents a keyword argument extracted from a text message.
 * <p>
 * A keyword is different from a simple text token in that it describes a token that is encoded as a string,
 * but it represents a message argument and not a static token.
 * Ideally, only arguments with low cardinality should be represented by a Keyword.
 * High cardinality ones (like UUIDs for example) should be represented by a different type, as much as possible.
 * Since we rely on a generic schema for the identification of arguments, we take into account that it would be used
 * for high cardinality arguments as well.
 */
public final class KeywordArgument implements Argument<String> {
    private final int startPosition;
    private final int length;
    private final StringBuilder value;

    public KeywordArgument(String s, int start, int length) {
        this.startPosition = start;
        this.length = length;
        this.value = new StringBuilder(length);
        this.value.append(s, start, start + length);
    }

    @Override
    public String value() {
        return value.toString();
    }

    @Override
    public EncodingType type() {
        return EncodingType.TEXT;
    }

    @Override
    public int startPosition() {
        return startPosition;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public String encode() {
        return value.toString();
    }
}
