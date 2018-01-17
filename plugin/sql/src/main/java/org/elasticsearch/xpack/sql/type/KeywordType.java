/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;

public class KeywordType extends StringType {

    static final int DEFAULT_LENGTH = 256;
    static final boolean DEFAULT_NORMALIZED = false;
    static final KeywordType DEFAULT = new KeywordType(true, DEFAULT_LENGTH, DEFAULT_NORMALIZED, emptyMap());

    private final int length;
    private final boolean normalized;

    KeywordType(boolean docValues, int length, boolean normalized, Map<String, DataType> fields) {
        super(docValues, fields);
        this.length = length;
        this.normalized = normalized;
    }

    @Override
    public boolean isInexact() {
        return normalized;
    }

    public boolean isNormalized() {
        return normalized;
    }

    @Override
    public String esName() {
        return "keyword";
    }

    @Override
    public int precision() {
        return length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(length, hasDocValues(), fields());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && length == ((KeywordType) obj).length;
    }

    static DataType from(boolean docValues, int length, boolean normalized, Map<String, DataType> fields) {
        return docValues && length == DEFAULT_LENGTH && fields.isEmpty() && normalized == DEFAULT_NORMALIZED 
                ? DEFAULT 
                : new KeywordType(docValues, length, normalized, fields);
    }
}
