/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A field visitor that allows to load a selection of the stored fields by exact name
 * {@code _id} and {@code _routing} fields are always loaded.
 */
public class CustomFieldsVisitor extends FieldsVisitor {

    private final Set<String> fields;

    public CustomFieldsVisitor(Set<String> fields, boolean loadSource) {
        super(loadSource);
        this.fields = new HashSet<>(fields);
        // metadata fields are already handled by FieldsVisitor, so removing
        // them here means that if the only fields requested are metadata
        // fields then we can shortcut loading
        List.of("_id", "_routing", "_source").forEach(this.fields::remove);
    }

    @Override
    public Set<String> getFieldNames() {
        Set<String> fields = new HashSet<>(super.getFieldNames());
        fields.addAll(this.fields);
        return fields;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (fields.isEmpty()) {
            return super.needsField(fieldInfo);
        }
        if (super.needsField(fieldInfo) == Status.YES) {
            return Status.YES;
        }
        if (fields.contains(fieldInfo.name)) {
            return Status.YES;
        }
        return Status.NO;
    }
}
