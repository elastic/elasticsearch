/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.util.Collections.emptyMap;

// String type is a special type of CompoundDataType
public abstract class StringType extends CompoundDataType {

    private final boolean docValue;
    private final Map<String, DataType> fields;
    private final Map<String, KeywordType> exactKeywords;


    StringType(boolean docValue, Map<String, DataType> fields) {
        super(JDBCType.VARCHAR, docValue, fields);

        this.docValue = docValue;
        this.fields = fields;

        if (docValue || fields.isEmpty()) {
            exactKeywords = emptyMap();
        } else {
            exactKeywords = new LinkedHashMap<>();
            for (Entry<String, DataType> entry : fields.entrySet()) {
                DataType t = entry.getValue();
                // consider only non-normalized keywords
                if (t instanceof KeywordType) {
                    KeywordType kt = (KeywordType) t;
                    if (!kt.isNormalized()) {
                        exactKeywords.put(entry.getKey(), kt);
                    }
                }
            }
        }
    }

    public abstract boolean isInexact();

    public Map<String, DataType> fields() {
        return properties();
    }

    public Map<String, KeywordType> exactKeywords() {
        return exactKeywords;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public int precision() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int hashCode() {
        return Objects.hash(docValue, fields);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            StringType other = (StringType) obj;
            return Objects.equals(docValue, other.docValue)
                    && Objects.equals(fields(), other.fields());
        }
        return false;
    }

    @Override
    public String toString() {
        return esName();
    }
}