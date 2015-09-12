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
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.List;

/**
 */
public class SingleFieldsVisitor extends FieldsVisitor {

    private String field;

    public SingleFieldsVisitor(String field) {
        super(false);
        this.field = field;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (fieldInfo.name.equals(field)) {
            return Status.YES;
        }
        if (fieldInfo.name.equals(UidFieldMapper.NAME)) {
            if (TypeFieldMapper.NAME.equals(field) || IdFieldMapper.NAME.equals(field)) {
                return Status.YES;
            }
        }
        return Status.NO;
    }

    public void reset(String field) {
        this.field = field;
        super.reset();
    }

    public void postProcess(MappedFieldType fieldType) {
        if (uid != null) {
            // TODO: this switch seems very wrong...either each case should be breaking, or this should not be a switch
            switch (field) {
                case UidFieldMapper.NAME: addValue(field, uid.toString());
                case IdFieldMapper.NAME: addValue(field, uid.id());
                case TypeFieldMapper.NAME: addValue(field, uid.type());
            }
        }

        if (fieldsValues == null) {
            return;
        }
        List<Object> fieldValues = fieldsValues.get(fieldType.names().indexName());
        if (fieldValues == null) {
            return;
        }
        for (int i = 0; i < fieldValues.size(); i++) {
            fieldValues.set(i, fieldType.valueForSearch(fieldValues.get(i)));
        }
    }
}
