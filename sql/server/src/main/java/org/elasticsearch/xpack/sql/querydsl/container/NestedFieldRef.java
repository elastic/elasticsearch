/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

public class NestedFieldRef implements FieldReference {
    private final String parent, name;
    private final boolean docValue;

    public NestedFieldRef(String parent, String name, boolean useDocValueInsteadOfSource) {
        this.parent = parent;
        this.name = name;
        this.docValue = useDocValueInsteadOfSource;
    }

    public String parent() {
        return parent;
    }

    @Override
    public String name() {
        return name;
    }

    public boolean useDocValue() {
        return docValue;
    }

    @Override
    public String toString() {
        return name;
    }
}