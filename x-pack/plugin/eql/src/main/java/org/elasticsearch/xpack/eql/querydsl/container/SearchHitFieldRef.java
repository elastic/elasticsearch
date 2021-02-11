/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;

// NB: this class is taken from SQL - it hasn't been ported over to QL
// since at this stage is unclear whether the whole FieldExtraction infrastructure
// needs porting or just the field extraction
public class SearchHitFieldRef implements FieldExtraction {

    private final String name;
    private final DataType dataType;
    private final String hitName;

    public SearchHitFieldRef(String name, DataType dataType, boolean isAlias) {
        this(name, dataType, isAlias, null);
    }

    public SearchHitFieldRef(String name, DataType dataType, boolean isAlias, String hitName) {
        this.name = name;
        this.dataType = dataType;
        this.hitName = hitName;
    }

    public String hitName() {
        return hitName;
    }

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
        sourceBuilder.addFetchField(name, format(dataType));
    }

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return false;
    }

    @Override
    public String toString() {
        return name;
    }

    private static String format(DataType dataType) {
        // We need epoch_millis for the tiebreaker timestamp field, because parsing timestamp strings
        // can have a negative performance impact
        return dataType == DATETIME ? "epoch_millis" : null;
    }
}
