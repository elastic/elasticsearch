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

    private static CopyOnWriteHashMap<String, FieldMappers> add(CopyOnWriteHashMap<String, FieldMappers> map, String key, FieldMapper<?> mapper) {
        FieldMappers mappers = map.get(key);
        if (mappers == null) {
            mappers = new FieldMappers(mapper);
        } else {
            mappers = mappers.concat(mapper);
        }
        return map.copyAndPut(key, mappers);
    }

    private static class MappersLookup {

        final CopyOnWriteHashMap<String, FieldMappers> indexName, fullName;

        MappersLookup(CopyOnWriteHashMap<String, FieldMappers> indexName, CopyOnWriteHashMap<String, FieldMappers> fullName) {
            this.indexName = indexName;
            this.fullName = fullName;
        }

        MappersLookup addNewMappers(Iterable<? extends FieldMapper<?>> mappers) {
            CopyOnWriteHashMap<String, FieldMappers> indexName = this.indexName;
            CopyOnWriteHashMap<String, FieldMappers> fullName = this.fullName;
            for (FieldMapper<?> mapper : mappers) {
                indexName = add(indexName, mapper.names().indexName(), mapper);
                fullName = add(fullName, mapper.names().fullName(), mapper);
            }
            return new MappersLookup(indexName, fullName);
        }
        
    }

    private final MappersLookup lookup;

    /** Create a new empty instance. */
    public FieldMappersLookup() {
        this(new MappersLookup(new CopyOnWriteHashMap<String, FieldMappers>(),
                               new CopyOnWriteHashMap<String, FieldMappers>()));
    }

    private FieldMappersLookup(MappersLookup lookup) {
        this.lookup = lookup;
    }

    /**
     * Return a new instance that contains the union of this instance and the provided mappers.
     */
    public FieldMappersLookup copyAndAddAll(Collection<? extends FieldMapper<?>> newMappers) {
        return new FieldMappersLookup(lookup.addNewMappers(newMappers));
    }

    /**
     * Returns the field mappers based on the mapper index name.
     */
    public FieldMappers indexName(String indexName) {
        return lookup.indexName.get(indexName);
    }

    /**
     * Returns the field mappers based on the mapper full name.
     */
    public FieldMappers fullName(String fullName) {
        return lookup.fullName.get(fullName);
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
        final Iterator<FieldMappers> fieldsItr = lookup.fullName.values().iterator();
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
