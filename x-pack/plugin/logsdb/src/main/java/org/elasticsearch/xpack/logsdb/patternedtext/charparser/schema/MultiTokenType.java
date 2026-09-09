/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.Type;

public class MultiTokenType implements Type {
    private final String name;
    private final EncodingType encodingType;
    private final MultiTokenFormat format;
    private final String description;

    public MultiTokenType(String name, EncodingType encodingType, MultiTokenFormat format, String description) {
        this.name = name;
        this.encodingType = encodingType;
        this.format = format;
        this.description = description;
    }

    public String name() {
        return name;
    }

    public EncodingType encodingType() {
        return encodingType;
    }

    public MultiTokenFormat getFormat() {
        return format;
    }

    public int getNumberOfSubTokens() {
        return format.getNumberOfSubTokens();
    }

    public String getDescription() {
        return description;
    }
}
