/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;

import java.util.HashSet;
import java.util.Set;

/**
 * A field visitor that allows to load a selection of the stored fields by exact name.
 * {@code _id}, {@code _routing}, and {@code _ignored} fields are always loaded.
 * {@code _source} is always loaded unless disabled explicitly.
 */
public class CustomFieldsVisitor extends FieldsVisitor {
    private final Set<String> fields;

    public CustomFieldsVisitor(Set<String> fields, boolean loadSource) {
        super(loadSource);
        this.fields = new HashSet<>(fields);
        // metadata fields that are always retrieved are already handled by FieldsVisitor, so removing
        // them here means that if the only fields requested are those metadata fields then we can shortcut loading
        FieldsVisitor.BASE_REQUIRED_FIELDS.forEach(this.fields::remove);
        this.fields.remove(this.sourceFieldName);
        this.fields.remove(IgnoredFieldMapper.NAME);
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
