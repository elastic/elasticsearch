/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;

/**
 * SQL-related information about an index field with text type
 */
public class TextEsField extends EsField {

    public TextEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        this(name, properties, hasDocValues, false);
    }
    
    public TextEsField(String name, Map<String, EsField> properties, boolean hasDocValues, boolean isAlias) {
        super(name, TEXT, properties, hasDocValues, isAlias);
    }

    @Override
    public EsField getExactField() {
        Tuple<EsField, String> findExact = findExact();
        if (findExact.v1() == null) {
            throw new QlIllegalArgumentException(findExact.v2());
        }
        return findExact.v1();
    }

    @Override
    public Exact getExactInfo() {
        return PROCESS_EXACT_FIELD.apply(findExact());
    }

    private Tuple<EsField, String> findExact() {
        EsField field = null;
        for (EsField property : getProperties().values()) {
            if (property.getDataType() == KEYWORD && property.getExactInfo().hasExact()) {
                if (field != null) {
                    return new Tuple<>(null, "Multiple exact keyword candidates available for [" + getName() +
                        "]; specify which one to use");
                }
                field = property;
            }
        }
        if (field == null) {
            return new Tuple<>(null, "No keyword/multi-field defined exact matches for [" + getName() +
                "]; define one or use MATCH/QUERY instead");
        }
        return new Tuple<>(field, null);
    }

    private Function<Tuple<EsField, String>, Exact> PROCESS_EXACT_FIELD = tuple -> {
        if (tuple.v1() == null) {
            return new Exact(false, tuple.v2());
        } else {
            return new Exact(true, null);
        }
    };
}
