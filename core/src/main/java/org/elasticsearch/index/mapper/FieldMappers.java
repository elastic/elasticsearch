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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A holder for several {@link FieldMapper}.
 */
public class FieldMappers implements Iterable<FieldMapper> {

    private final FieldMapper[] fieldMappers;
    private final List<FieldMapper> fieldMappersAsList;

    public FieldMappers() {
        this.fieldMappers = new FieldMapper[0];
        this.fieldMappersAsList = Arrays.asList(fieldMappers);
    }

    public FieldMappers(FieldMapper fieldMapper) {
        this.fieldMappers = new FieldMapper[]{fieldMapper};
        this.fieldMappersAsList = Arrays.asList(this.fieldMappers);
    }

    private FieldMappers(FieldMapper[] fieldMappers) {
        this.fieldMappers = fieldMappers;
        this.fieldMappersAsList = Arrays.asList(this.fieldMappers);
    }

    public FieldMapper mapper() {
        if (fieldMappers.length == 0) {
            return null;
        }
        return fieldMappers[0];
    }

    public boolean isEmpty() {
        return fieldMappers.length == 0;
    }

    public List<FieldMapper> mappers() {
        return this.fieldMappersAsList;
    }

    @Override
    public Iterator<FieldMapper> iterator() {
        return fieldMappersAsList.iterator();
    }

    /**
     * Concats and returns a new {@link FieldMappers}.
     */
    public FieldMappers concat(FieldMapper mapper) {
        FieldMapper[] newMappers = new FieldMapper[fieldMappers.length + 1];
        System.arraycopy(fieldMappers, 0, newMappers, 0, fieldMappers.length);
        newMappers[fieldMappers.length] = mapper;
        return new FieldMappers(newMappers);
    }

    public FieldMappers remove(FieldMapper mapper) {
        ArrayList<FieldMapper> list = new ArrayList<>(fieldMappers.length);
        for (FieldMapper fieldMapper : fieldMappers) {
            if (!fieldMapper.equals(mapper)) { // identify equality
                list.add(fieldMapper);
            }
        }
        return new FieldMappers(list.toArray(new FieldMapper[list.size()]));
    }
}
