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

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.object.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** A container for tracking results of a mapping merge. */
public class MergeResult {

    private final boolean simulate;
    private final boolean updateAllTypes;

    private final List<String> conflicts = new ArrayList<>();
    private final List<FieldMapper> newFieldMappers = new ArrayList<>();
    private final List<ObjectMapper> newObjectMappers = new ArrayList<>();

    public MergeResult(boolean simulate, boolean updateAllTypes) {
        this.simulate = simulate;
        this.updateAllTypes = updateAllTypes;
    }

    public void addFieldMappers(Collection<FieldMapper> fieldMappers) {
        assert simulate() == false;
        newFieldMappers.addAll(fieldMappers);
    }

    public void addObjectMappers(Collection<ObjectMapper> objectMappers) {
        assert simulate() == false;
        newObjectMappers.addAll(objectMappers);
    }

    public Collection<FieldMapper> getNewFieldMappers() {
        return newFieldMappers;
    }

    public Collection<ObjectMapper> getNewObjectMappers() {
        return newObjectMappers;
    }

    public boolean simulate() {
        return simulate;
    }

    public boolean updateAllTypes() {
        return updateAllTypes;
    }

    public void addConflict(String mergeFailure) {
        conflicts.add(mergeFailure);
    }

    public boolean hasConflicts() {
        return conflicts.isEmpty() == false;
    }

    public String[] buildConflicts() {
        return conflicts.toArray(Strings.EMPTY_ARRAY);
    }
}