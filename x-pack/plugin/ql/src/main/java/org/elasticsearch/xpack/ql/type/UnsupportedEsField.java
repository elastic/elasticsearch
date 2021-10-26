/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.type;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * SQL-related information about an index field that cannot be supported by SQL.
 * All the subfields (properties) of an unsupported type should also be unsupported.
 */
public class UnsupportedEsField extends EsField {

    private final String originalType;
    private final String inherited; // for fields belonging to parents (or grandparents) that have an unsupported type

    public UnsupportedEsField(String name, String originalType) {
        this(name, originalType, null, new TreeMap<>());
    }

    public UnsupportedEsField(String name, String originalType, String inherited, Map<String, EsField> properties) {
        super(name, DataTypes.UNSUPPORTED, properties, false);
        this.originalType = originalType;
        this.inherited = inherited;
    }

    public String getOriginalType() {
        return originalType;
    }

    public String getInherited() {
        return inherited;
    }

    public boolean hasInherited() {
        return inherited != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        UnsupportedEsField that = (UnsupportedEsField) o;
        return Objects.equals(originalType, that.originalType)
                && Objects.equals(inherited, that.inherited);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originalType, inherited);
    }
}
