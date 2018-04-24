/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;

public class SearchHitFieldRef extends FieldReference {
    private final String name;
    private final boolean docValue;
    private final String hitName;

    public SearchHitFieldRef(String name, boolean useDocValueInsteadOfSource) {
        this(name, useDocValueInsteadOfSource, null);
    }

    public SearchHitFieldRef(String name, boolean useDocValueInsteadOfSource, String hitName) {
        this.name = name;
        this.docValue = useDocValueInsteadOfSource;
        this.hitName = hitName;
    }

    public String hitName() {
        return hitName;
    }

    @Override
    public String name() {
        return name;
    }

    public boolean useDocValue() {
        return docValue;
    }

    @Override
    public void collectFields(SqlSourceBuilder sourceBuilder) {
        // nested fields are handled by inner hits
        if (hitName != null) {
            return;
        }
        if (docValue) {
            sourceBuilder.addDocField(name);
        } else {
            sourceBuilder.addSourceField(name);
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
