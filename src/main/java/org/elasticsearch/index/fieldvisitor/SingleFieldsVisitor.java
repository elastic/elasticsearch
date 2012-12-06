/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.index.mapper.FieldMapper;

import java.io.IOException;
import java.util.List;

/**
 */
public class SingleFieldsVisitor extends FieldsVisitor {

    private String field;

    public SingleFieldsVisitor(String field) {
        this.field = field;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        // TODO we can potentially skip if we processed a field, the question is if it works for multi valued fields
        if (fieldInfo.name.equals(field)) {
            return Status.YES;
        } else {
            return Status.NO;
        }
    }

    public void reset(String field) {
        this.field = field;
        super.reset();
    }

    public void postProcess(FieldMapper mapper) {
        if (fieldsValues == null) {
            return;
        }
        List<Object> fieldValues = fieldsValues.get(mapper.names().indexName());
        if (fieldValues == null) {
            return;
        }
        for (int i = 0; i < fieldValues.size(); i++) {
            fieldValues.set(i, mapper.valueForSearch(fieldValues.get(i)));
        }
    }
}
