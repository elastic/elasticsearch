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
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.LegacyTypeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Base {@link StoredFieldVisitor} that retrieves all non-redundant metadata.
 */
public class FieldsVisitor extends FieldNamesProvidingStoredFieldsVisitor {
    static final Set<String> BASE_REQUIRED_FIELDS = Set.of(IdFieldMapper.NAME, RoutingFieldMapper.NAME);

    private final boolean loadSource;
    final String sourceFieldName;
    private final Set<String> requiredFields;
    private BytesReference source;
    private String id;
    private final Map<String, List<Object>> fieldsValues = new HashMap<>();

    public FieldsVisitor(boolean loadSource) {
        this(loadSource, SourceFieldMapper.NAME);
    }

    @SuppressWarnings("this-escape")
    public FieldsVisitor(boolean loadSource, String sourceFieldName) {
        this.loadSource = loadSource;
        this.sourceFieldName = sourceFieldName;
        requiredFields = new HashSet<>();
        reset();
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        final String name = fieldInfo.name;
        var requiredFields = this.requiredFields;
        if (requiredFields.remove(name)) {
            return Status.YES;
        }
        // Always load _ignored to be explicit about ignored fields
        // This works because _ignored is added as the first metadata mapper,
        // so its stored fields always appear first in the list.
        // Note that _ignored is also multi-valued, which is why it can't be removed from the set like other fields
        if (IgnoredFieldMapper.NAME.equals(name)) {
            return Status.YES;
        }
        // All these fields are single-valued so we can stop when the set is empty
        if (requiredFields.isEmpty()) {
            return Status.STOP;
        }
        // support _uid for loading older indices
        if ("_uid".equals(name)) {
            if (requiredFields.remove(IdFieldMapper.NAME) || requiredFields.remove(LegacyTypeFieldMapper.NAME)) {
                return Status.YES;
            }
        }
        return Status.NO;
    }

    @Override
    public Set<String> getFieldNames() {
        return requiredFields;
    }

    public final void postProcess(Function<String, MappedFieldType> fieldTypeLookup) {
        for (Map.Entry<String, List<Object>> entry : fields().entrySet()) {
            MappedFieldType fieldType = fieldTypeLookup.apply(entry.getKey());
            if (fieldType == null) {
                continue; // TODO this is lame
            }
            List<Object> fieldValues = entry.getValue();
            for (int i = 0; i < fieldValues.size(); i++) {
                fieldValues.set(i, fieldType.valueForDisplay(fieldValues.get(i)));
            }
        }
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        final String name = fieldInfo.name;
        if (sourceFieldName.equals(name)) {
            source = new BytesArray(value);
        } else if (IdFieldMapper.NAME.equals(name)) {
            id = Uid.decodeId(value, 0, value.length);
        } else {
            addValue(name, new BytesRef(value));
        }
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) {
        final String name = fieldInfo.name;
        assert sourceFieldName.equals(name) == false : "source field must go through binaryField";
        if ("_uid".equals(name)) {
            // 5.x-only
            int delimiterIndex = value.indexOf('#'); // type is not allowed to have # in it..., ids can
            String type = value.substring(0, delimiterIndex);
            id = value.substring(delimiterIndex + 1);
            addValue(LegacyTypeFieldMapper.NAME, type);
        } else if (IdFieldMapper.NAME.equals(name)) {
            // only applies to 5.x indices that have single_type = true
            id = value;
        } else {
            addValue(name, value);
        }
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) {
        addValue(fieldInfo.name, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) {
        addValue(fieldInfo.name, value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) {
        addValue(fieldInfo.name, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) {
        addValue(fieldInfo.name, value);
    }

    public BytesReference source() {
        return source;
    }

    public String id() {
        return id;
    }

    public String routing() {
        List<Object> values = fieldsValues.get(RoutingFieldMapper.NAME);
        if (values == null || values.isEmpty()) {
            return null;
        }
        assert values.size() == 1;
        return values.getFirst().toString();
    }

    public Map<String, List<Object>> fields() {
        return fieldsValues;
    }

    public void reset() {
        fieldsValues.clear();
        source = null;
        id = null;

        requiredFields.addAll(BASE_REQUIRED_FIELDS);
        if (loadSource) {
            requiredFields.add(sourceFieldName);
        }
    }

    void addValue(String name, Object value) {
        fieldsValues.computeIfAbsent(name, k -> new ArrayList<>(2)).add(value);
    }
}
