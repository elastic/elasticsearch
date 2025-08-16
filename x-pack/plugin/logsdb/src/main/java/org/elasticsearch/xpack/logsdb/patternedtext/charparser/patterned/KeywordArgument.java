/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

public final class KeywordArgument implements Argument<String> {
    private final StringBuilder value;

    public KeywordArgument(String s, int start, int length) {
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
    public String encode() {
        return value.toString();
    }
}
