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

public class SearchHitFieldRef extends FieldReference {
    private final String name;
    private final DataType dataType;
    private final String hitName;

    public SearchHitFieldRef(String name, DataType dataType) {
        this(name, dataType, null);
    }

    public SearchHitFieldRef(String name, DataType dataType, String hitName) {
        this.name = name;
        this.dataType = dataType;
        this.hitName = hitName;
    }

    public String hitName() {
        return hitName;
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
        // nested fields are handled by inner hits
        if (hitName != null) {
            return;
        }
        sourceBuilder.addFetchField(name, SqlDataTypes.format(dataType));
    }

    @Override
    public String toString() {
        return name;
    }
}
