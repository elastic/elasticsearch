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

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 */
public abstract class FieldsVisitor extends StoredFieldVisitor {

    protected BytesReference source;
    protected Uid uid;
    protected Map<String, List<Object>> fieldsValues;

    public void postProcess(MapperService mapperService) {
        if (uid != null) {
            DocumentMapper documentMapper = mapperService.documentMapper(uid.type());
            if (documentMapper != null) {
                // we can derive the exact type for the mapping
                postProcess(documentMapper);
                return;
            }
        }
        // can't derive exact mapping type
        for (Map.Entry<String, List<Object>> entry : fields().entrySet()) {
            FieldMappers fieldMappers = mapperService.indexName(entry.getKey());
            if (fieldMappers == null) {
                continue;
            }
            List<Object> fieldValues = entry.getValue();
            for (int i = 0; i < fieldValues.size(); i++) {
                fieldValues.set(i, fieldMappers.mapper().valueForSearch(fieldValues.get(i)));
            }
        }
    }

    public void postProcess(DocumentMapper documentMapper) {
        for (Map.Entry<String, List<Object>> entry : fields().entrySet()) {
            FieldMapper<?> fieldMapper = documentMapper.mappers().indexName(entry.getKey()).mapper();
            if (fieldMapper == null) {
                continue;
            }
            List<Object> fieldValues = entry.getValue();
            for (int i = 0; i < fieldValues.size(); i++) {
                fieldValues.set(i, fieldMapper.valueForSearch(fieldValues.get(i)));
            }
        }
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
            source = new BytesArray(value);
        } else {
            addValue(fieldInfo.name, new BytesRef(value));
        }
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] bytes) throws IOException {
        final String value = new String(bytes, StandardCharsets.UTF_8);
        if (UidFieldMapper.NAME.equals(fieldInfo.name)) {
            uid = Uid.createUid(value);
        } else {
            addValue(fieldInfo.name, value);
        }
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        addValue(fieldInfo.name, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        addValue(fieldInfo.name, value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        addValue(fieldInfo.name, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        addValue(fieldInfo.name, value);
    }

    public BytesReference source() {
        return source;
    }

    public Uid uid() {
        return uid;
    }

    public Map<String, List<Object>> fields() {
        return fieldsValues != null
                ? fieldsValues
                : ImmutableMap.<String, List<Object>>of();
    }

    public void reset() {
        if (fieldsValues != null) fieldsValues.clear();
        source = null;
        uid = null;
    }

    void addValue(String name, Object value) {
        if (fieldsValues == null) {
            fieldsValues = newHashMap();
        }

        List<Object> values = fieldsValues.get(name);
        if (values == null) {
            values = new ArrayList<>(2);
            fieldsValues.put(name, values);
        }
        values.add(value);
    }
}
