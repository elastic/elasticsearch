/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

public class PivotMathFieldRef extends FieldReference {

    private final String name;
    private final DataType dataType;

    public PivotMathFieldRef(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    @Override
    public String name() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    @Override
    public void collectFields(QlSourceBuilder sourceBuilder) {
        sourceBuilder.addFetchField(name, SqlDataTypes.format(dataType));
    }

    @Override
    public String toString() {
        return name;
    }
}
