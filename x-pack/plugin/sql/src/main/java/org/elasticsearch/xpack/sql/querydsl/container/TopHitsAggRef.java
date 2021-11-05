/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.AggRef;
import org.elasticsearch.xpack.ql.type.DataType;

/**
 * Reference to a TopHits aggregation.
 * Since only one field is returned we only need its data type
 */
public class TopHitsAggRef extends AggRef {

    // only for readability via toString()
    private final String name;
    private final DataType fieldDataType;

    public TopHitsAggRef(String name, DataType fieldDataType) {
        this.name = name;
        this.fieldDataType = fieldDataType;
    }

    public String name() {
        return name;
    }

    public DataType fieldDataType() {
        return fieldDataType;
    }

    @Override
    public String toString() {
        return ">" + name + "[" + fieldDataType.typeName() + "]";
    }
}
