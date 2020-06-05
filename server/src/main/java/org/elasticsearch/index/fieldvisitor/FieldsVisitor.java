/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 * Base {@link StoredFieldVisitor} that retrieves all non-redundant metadata.
 */
public class FieldsVisitor extends StoredFieldVisitor {
    private static final Set<String> BASE_REQUIRED_FIELDS = unmodifiableSet(newHashSet(
            IdFieldMapper.NAME,
            RoutingFieldMapper.NAME));

    private final boolean loadSource;
    private final String sourceFieldName;
    private final Set<String> requiredFields;
    protected BytesReference source;
    protected String id;
    protected Map<String, List<Object>> fieldsValues;

    public FieldsVisitor(boolean loadSource) {
        this(loadSource, SourceFieldMapper.NAME);
    }

    public FieldsVisitor(boolean loadSource, String sourceFieldName) {
        this.loadSource = loadSource;
        this.sourceFieldName = sourceFieldName;
        requiredFields = new HashSet<>();
        reset();
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (requiredFields.remove(fieldInfo.name)) {
            return Status.YES;
        }
        // Always load _ignored to be explicit about ignored fields
        // This works because _ignored is added as the first metadata mapper,
        // so its stored fields always appear first in the list.
        if (IgnoredFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        }
        // All these fields are single-valued so we can stop when the set is
        // empty
        return requiredFields.isEmpty()
                ? Status.STOP
                : Status.NO;
    }

    public void postProcess(MapperService mapperService) {
        for (Map.Entry<String, List<Object>> entry : fields().entrySet()) {
            MappedFieldType fieldType = mapperService.fieldType(entry.getKey());
            if (fieldType == null) {
                throw new IllegalStateException("Field [" + entry.getKey()
                    + "] exists in the index but not in mappings");
            }
            List<Object> fieldValues = entry.getValue();
            for (int i = 0; i < fieldValues.size(); i++) {
                fieldValues.set(i, fieldType.valueForDisplay(fieldValues.get(i)));
            }
        }
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        binaryField(fieldInfo, new BytesRef(value));
    }

    public void binaryField(FieldInfo fieldInfo, BytesRef value) {
        if (sourceFieldName.equals(fieldInfo.name)) {
            source = new BytesArray(value);
        } else if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
            id = Uid.decodeId(value.bytes, value.offset, value.length);
        } else {
            addValue(fieldInfo.name, value);
        }
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] bytes) {
        assert IdFieldMapper.NAME.equals(fieldInfo.name) == false : "_id field must go through binaryField";
        assert sourceFieldName.equals(fieldInfo.name) == false : "source field must go through binaryField";
        final String value = new String(bytes, StandardCharsets.UTF_8);
        addValue(fieldInfo.name, value);
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

    public void objectField(FieldInfo fieldInfo, Object object) {
        assert IdFieldMapper.NAME.equals(fieldInfo.name) == false : "_id field must go through binaryField";
        assert sourceFieldName.equals(fieldInfo.name) == false : "source field must go through binaryField";
        addValue(fieldInfo.name, object);
    }

    public BytesReference source() {
        return source;
    }

    public String id() {
        return id;
    }

    public String routing() {
        if (fieldsValues == null) {
            return null;
        }
        List<Object> values = fieldsValues.get(RoutingFieldMapper.NAME);
        if (values == null || values.isEmpty()) {
            return null;
        }
        assert values.size() == 1;
        return values.get(0).toString();
    }

    public Map<String, List<Object>> fields() {
        return fieldsValues != null ? fieldsValues : emptyMap();
    }

    public void reset() {
        if (fieldsValues != null) fieldsValues.clear();
        source = null;
        id = null;

        requiredFields.addAll(BASE_REQUIRED_FIELDS);
        if (loadSource) {
            requiredFields.add(sourceFieldName);
        }
    }

    void addValue(String name, Object value) {
        if (fieldsValues == null) {
            fieldsValues = new HashMap<>();
        }

        List<Object> values = fieldsValues.get(name);
        if (values == null) {
            values = new ArrayList<>(2);
            fieldsValues.put(name, values);
        }
        values.add(value);
    }
}
