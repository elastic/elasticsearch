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

import com.google.common.collect.Lists;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.regex.Regex;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A class that holds a map of field mappers from name, index name, and full name.
 */
class FieldMappersLookup implements Iterable<FieldMapper<?>> {

    /** Full field name to mappers */
    private final CopyOnWriteHashMap<String, FieldMappers> mappers;

    /** Create a new empty instance. */
    public FieldMappersLookup() {
        mappers = new CopyOnWriteHashMap<>();
    }

    private FieldMappersLookup(CopyOnWriteHashMap<String, FieldMappers> map) {
        mappers = map;
    }

    /**
     * Return a new instance that contains the union of this instance and the provided mappers.
     */
    public FieldMappersLookup copyAndAddAll(Collection<FieldMapper<?>> newMappers) {
        CopyOnWriteHashMap<String, FieldMappers> map = this.mappers;

        for (FieldMapper<?> mapper : newMappers) {
            String key = mapper.names().fullName();
            FieldMappers mappers = map.get(key);

            if (mappers == null) {
                mappers = new FieldMappers(mapper);
            } else {
                mappers = mappers.concat(mapper);
            }
            map = map.copyAndPut(key, mappers);
        }
        return new FieldMappersLookup(map);
    }

    /**
     * Returns the field mappers based on the mapper index name.
     * NOTE: this only exists for backcompat support and if the index name
     * does not match it's field name, this is a linear time operation
     * @deprecated Use {@link #get(String)}
     */
    @Deprecated
    public FieldMappers indexName(String indexName) {
        FieldMappers fieldMappers = fullName(indexName);
        if (fieldMappers != null) {
            if (fieldMappers.mapper().names().indexName().equals(indexName)) {
                return fieldMappers;
            }
        }
        fieldMappers = new FieldMappers();
        for (FieldMapper mapper : this) {
            if (mapper.names().indexName().equals(indexName)) {
                fieldMappers = fieldMappers.concat(mapper);
            }
        }
        if (fieldMappers.isEmpty()) {
            return null;
        }
        return fieldMappers;
    }

    /**
     * Returns the field mappers based on the mapper full name.
     */
    public FieldMappers fullName(String fullName) {
        return mappers.get(fullName);
    }

    /** Returns the mapper for the given field */
    public FieldMapper get(String field) {
        FieldMappers fieldMappers = mappers.get(field);
        if (fieldMappers == null) {
            return null;
        }
        if (fieldMappers.mappers().size() != 1) {
            throw new IllegalStateException("Mapper for field [" + field + "] should be unique");
        }
        return fieldMappers.mapper();
    }

    /**
     * Returns a list of the index names of a simple match regex like pattern against full name and index name.
     */
    public List<String> simpleMatchToIndexNames(String pattern) {
        List<String> fields = Lists.newArrayList();
        for (FieldMapper<?> fieldMapper : this) {
            if (Regex.simpleMatch(pattern, fieldMapper.names().fullName())) {
                fields.add(fieldMapper.names().indexName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().indexName())) {
                fields.add(fieldMapper.names().indexName());
            }
        }
        return fields;
    }

    /**
     * Returns a list of the full names of a simple match regex like pattern against full name and index name.
     */
    public List<String> simpleMatchToFullName(String pattern) {
        List<String> fields = Lists.newArrayList();
        for (FieldMapper<?> fieldMapper : this) {
            if (Regex.simpleMatch(pattern, fieldMapper.names().fullName())) {
                fields.add(fieldMapper.names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().indexName())) {
                fields.add(fieldMapper.names().fullName());
            }
        }
        return fields;
    }

    /**
     * Tries to find first based on {@link #fullName(String)}, then by {@link #indexName(String)}.
     */
    @Nullable
    FieldMappers smartName(String name) {
        FieldMappers fieldMappers = fullName(name);
        if (fieldMappers != null) {
            return fieldMappers;
        }
        return indexName(name);
    }

    /**
     * Tries to find first based on {@link #fullName(String)}, then by {@link #indexName(String)}
     * and return the first mapper for it (see {@link org.elasticsearch.index.mapper.FieldMappers#mapper()}).
     */
    @Nullable
    public FieldMapper<?> smartNameFieldMapper(String name) {
        FieldMappers fieldMappers = smartName(name);
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
    }

    public Iterator<FieldMapper<?>> iterator() {
        final Iterator<FieldMappers> fieldsItr = mappers.values().iterator();
        if (fieldsItr.hasNext() == false) {
            return Collections.emptyIterator();
        }
        return new Iterator<FieldMapper<?>>() {
            Iterator<FieldMapper> fieldValuesItr = fieldsItr.next().iterator();
            @Override
            public boolean hasNext() {
                return fieldsItr.hasNext() || fieldValuesItr.hasNext();
            }
            @Override
            public FieldMapper next() {
                if (fieldValuesItr.hasNext() == false && fieldsItr.hasNext()) {
                    fieldValuesItr = fieldsItr.next().iterator();
                }
                return fieldValuesItr.next();
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException("cannot remove field mapper from lookup");
            }
        };
    }
}
