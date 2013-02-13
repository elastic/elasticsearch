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
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.Set;

/**
 */
public class CustomFieldsVisitor extends FieldsVisitor {

    private final boolean loadAllFields;
    private final boolean loadSource;
    private final Set<String> fields;

    public CustomFieldsVisitor(Set<String> fields, boolean loadSource) {
        this.loadAllFields = false;
        this.loadSource = loadSource;
        this.fields = fields;
    }

    public CustomFieldsVisitor(boolean loadAllFields, boolean loadSource) {
        this.loadAllFields = loadAllFields;
        this.loadSource = loadSource;
        this.fields = null;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (loadAllFields) {
            return Status.YES;
        }
        if (loadSource && SourceFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        }
        if (UidFieldMapper.NAME.equals(fieldInfo.name)) {
            return Status.YES;
        }

        return fields.contains(fieldInfo.name) ? Status.YES : Status.NO;
    }
}
