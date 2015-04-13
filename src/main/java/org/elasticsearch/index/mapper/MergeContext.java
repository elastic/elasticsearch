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

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.util.Collection;
import java.util.List;

/**
 *
 */
public abstract class MergeContext {

    private final DocumentMapper.MergeFlags mergeFlags;

    public MergeContext(DocumentMapper.MergeFlags mergeFlags) {
        this.mergeFlags = mergeFlags;
    }

    public abstract void addFieldMappers(List<FieldMapper<?>> fieldMappers);

    public abstract void addObjectMappers(Collection<ObjectMapper> objectMappers);

    public DocumentMapper.MergeFlags mergeFlags() {
        return mergeFlags;
    }

    public abstract void addConflict(String mergeFailure);

    public abstract boolean hasConflicts();

    public abstract String[] buildConflicts();
}
