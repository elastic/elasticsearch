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
    static final KeywordType DEFAULT = new KeywordType(true, DEFAULT_LENGTH, emptyMap());

    private final int length;
    
    KeywordType(boolean docValues, int length, Map<String, DataType> fields) {
        super(docValues, fields);
        this.length = length;
    }

    @Override
    public String esName() {
        return "keyword";
    }

    @Override
    public int precision() {
        return length;
    }

    static DataType from(boolean docValues, int length, Map<String, DataType> fields) {
        return docValues && length == DEFAULT_LENGTH && fields.isEmpty() ? DEFAULT : new KeywordType(docValues, length, fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(length, hasDocValues(), fields());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        KeywordType other = (KeywordType) obj;
        return Objects.equals(hasDocValues(), other.hasDocValues())
                && Objects.equals(length, other.length)
                && Objects.equals(fields(), other.fields());
    }
}
