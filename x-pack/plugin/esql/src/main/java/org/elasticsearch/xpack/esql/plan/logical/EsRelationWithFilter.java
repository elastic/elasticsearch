/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.options.EsSourceOptions;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class EsRelationWithFilter extends EsRelation {
    private final String fieldName;

    private final String queryString;

    public EsRelationWithFilter(
        Source source,
        EsIndex index,
        List<Attribute> attributes,
        EsSourceOptions esSourceOptions,
        boolean frozen,
        String fieldName,
        String queryString
        ) {
        super(
            source,
            index,
            attributes,
            esSourceOptions,
            frozen
        );

        this.fieldName = fieldName;
        this.queryString = queryString;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getQueryString() {
        return queryString;
    }
}
