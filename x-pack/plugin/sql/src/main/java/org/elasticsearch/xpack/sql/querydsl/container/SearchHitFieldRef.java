/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.type.DataType;

public class SearchHitFieldRef extends FieldReference {
    private final String name;
    private final String fullFieldName; // path included. If field full path is a.b.c, full field name is "a.b.c" and name is "c"
    private final DataType dataType;
    private final boolean docValue;
    private final String hitName;

    public SearchHitFieldRef(String name, String fullFieldName, DataType dataType, boolean useDocValueInsteadOfSource, boolean isAlias) {
        this(name, fullFieldName, dataType, useDocValueInsteadOfSource, isAlias, null);
    }

    public SearchHitFieldRef(String name, String fullFieldName, DataType dataType, boolean useDocValueInsteadOfSource, boolean isAlias,
            String hitName) {
        this.name = name;
        this.fullFieldName = fullFieldName;
        this.dataType = dataType;
        // these field types can only be extracted from docvalue_fields (ie, values already computed by Elasticsearch)
        // because, for us to be able to extract them from _source, we would need the mapping of those fields (which we don't have)
        this.docValue = isAlias ? useDocValueInsteadOfSource : (dataType.isFromDocValuesOnly() ? useDocValueInsteadOfSource : false);
        this.hitName = hitName;
    }

    public String hitName() {
        return hitName;
    }

    @Override
    public String name() {
        return name;
    }
    
    public String fullFieldName() {
        return fullFieldName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean useDocValue() {
        return docValue;
    }

    @Override
    public void collectFields(QlSourceBuilder sourceBuilder) {
        // nested fields are handled by inner hits
        if (hitName != null) {
            return;
        }
        if (docValue) {
            sourceBuilder.addDocField(name, dataType.format());
        } else {
            sourceBuilder.addSourceField(name);
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
