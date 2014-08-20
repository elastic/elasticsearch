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
import com.google.common.collect.Sets;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.regex.Regex;

import java.util.*;

/**
 * A class that holds a map of field mappers from name, index name, and full name.
 */
public class FieldMappersLookup extends AbstractList<FieldMapper<?>> {

    private static CopyOnWriteHashMap<String, FieldMappers> add(CopyOnWriteHashMap<String, FieldMappers> map, String key, FieldMapper<?> mapper) {
        FieldMappers mappers = map.get(key);
        if (mappers == null) {
            mappers = new FieldMappers(mapper);
        } else {
            mappers = mappers.concat(mapper);
        }
        return map.copyAndPut(key, mappers);
    }

    private static CopyOnWriteHashMap<String, FieldMappers> remove(CopyOnWriteHashMap<String, FieldMappers> map, String key, FieldMapper<?> mapper) {
        FieldMappers mappers = map.get(key);
        if (mappers == null) {
            return map;
        }
        mappers = mappers.remove(mapper);
        if (mappers.isEmpty()) {
            return map.copyAndRemove(key);
        } else {
            return map.copyAndPut(key, mappers);
        }
    }

    private static class MappersLookup {

        final CopyOnWriteHashMap<String, FieldMappers> name, indexName, fullName;

        MappersLookup(CopyOnWriteHashMap<String, FieldMappers> name, CopyOnWriteHashMap<String,
                FieldMappers> indexName, CopyOnWriteHashMap<String, FieldMappers> fullName) {
            this.name = name;
            this.indexName = indexName;
            this.fullName = fullName;
        }

        MappersLookup addNewMappers(Iterable<? extends FieldMapper<?>> mappers) {
            CopyOnWriteHashMap<String, FieldMappers> name = this.name;
            CopyOnWriteHashMap<String, FieldMappers> indexName = this.indexName;
            CopyOnWriteHashMap<String, FieldMappers> fullName = this.fullName;
            for (FieldMapper<?> mapper : mappers) {
                name = add(name, mapper.names().name(), mapper);
                indexName = add(indexName, mapper.names().indexName(), mapper);
                fullName = add(fullName, mapper.names().fullName(), mapper);
            }
            return new MappersLookup(name, indexName, fullName);
        }

        MappersLookup removeMappers(Iterable<?> mappers) {
            CopyOnWriteHashMap<String, FieldMappers> name = this.name;
            CopyOnWriteHashMap<String, FieldMappers> indexName = this.indexName;
            CopyOnWriteHashMap<String, FieldMappers> fullName = this.fullName;
            for (Object o : mappers) {
                if (!(o instanceof FieldMapper)) {
                    continue;
                }
                FieldMapper<?> mapper = (FieldMapper<?>) o;
                name = remove(name, mapper.names().name(), mapper);
                indexName = remove(indexName, mapper.names().indexName(), mapper);
                fullName = remove(fullName, mapper.names().fullName(), mapper);
            }
            return new MappersLookup(name, indexName, fullName);
        }
    }

    private final FieldMapper<?>[] mappers;
    private final MappersLookup lookup;

    /** Create a new empty instance. */
    public FieldMappersLookup() {
        this(new FieldMapper[0], new MappersLookup(new CopyOnWriteHashMap<String, FieldMappers>(), new CopyOnWriteHashMap<String, FieldMappers>(), new CopyOnWriteHashMap<String, FieldMappers>()));
    }

    private FieldMappersLookup(FieldMapper<?>[] mappers, MappersLookup lookup) {
        this.mappers = mappers;
        this.lookup = lookup;
    }

    /**
     * Return a new instance that contains the union of this instance and the provided mappers.
     */
    public FieldMappersLookup copyAndAddAll(Collection<? extends FieldMapper<?>> newMappers) {
        FieldMapper<?>[] tempMappers = Arrays.copyOf(mappers, mappers.length + newMappers.size());
        int i = mappers.length;
        for (FieldMapper<?> mapper : newMappers) {
            tempMappers[i++] = mapper;
        }
        return new FieldMappersLookup(tempMappers, lookup.addNewMappers(newMappers));
    }

    /**
     * Return a new instance that contains this instance minus the provided mappers.
     */
    public FieldMappersLookup copyAndRemoveAll(Collection<?> mappersToRemove) {
        List<FieldMapper<?>> tempMappers = Lists.newArrayList(mappers);
        tempMappers.removeAll(mappersToRemove);
        if (tempMappers.size() != mappers.length) {
            return new FieldMappersLookup(tempMappers.toArray(new FieldMapper[tempMappers.size()]), lookup.removeMappers(mappersToRemove));
        } else {
            return this;
        }
    }

    /**
     * Returns the field mappers based on the mapper name.
     */
    public FieldMappers name(String name) {
        return lookup.name.get(name);
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
     * Returns a set of the index names of a simple match regex like pattern against full name, name and index name.
     */
    public Set<String> simpleMatchToIndexNames(String pattern) {
        Set<String> fields = Sets.newHashSet();
        for (FieldMapper<?> fieldMapper : mappers) {
            if (Regex.simpleMatch(pattern, fieldMapper.names().fullName())) {
                fields.add(fieldMapper.names().indexName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().indexName())) {
                fields.add(fieldMapper.names().indexName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().name())) {
                fields.add(fieldMapper.names().indexName());
            }
        }
        return fields;
    }

    /**
     * Returns a set of the full names of a simple match regex like pattern against full name, name and index name.
     */
    public Set<String> simpleMatchToFullName(String pattern) {
        Set<String> fields = Sets.newHashSet();
        for (FieldMapper<?> fieldMapper : mappers) {
            if (Regex.simpleMatch(pattern, fieldMapper.names().fullName())) {
                fields.add(fieldMapper.names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().indexName())) {
                fields.add(fieldMapper.names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().name())) {
                fields.add(fieldMapper.names().fullName());
            }
        }
        return fields;
    }

    /**
     * Tries to find first based on {@link #fullName(String)}, then by {@link #indexName(String)}, and last
     * by {@link #name(String)}.
     */
    @Nullable
    public FieldMappers smartName(String name) {
        FieldMappers fieldMappers = fullName(name);
        if (fieldMappers != null) {
            return fieldMappers;
        }
        fieldMappers = indexName(name);
        if (fieldMappers != null) {
            return fieldMappers;
        }
        return name(name);
    }

    /**
     * Tries to find first based on {@link #fullName(String)}, then by {@link #indexName(String)}, and last
     * by {@link #name(String)} and return the first mapper for it (see {@link org.elasticsearch.index.mapper.FieldMappers#mapper()}).
     */
    @Nullable
    public FieldMapper<?> smartNameFieldMapper(String name) {
        FieldMappers fieldMappers = smartName(name);
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
    }

    @Override
    public FieldMapper<?> get(int index) {
        return mappers[index];
    }

    @Override
    public int size() {
        return mappers.length;
    }
}
