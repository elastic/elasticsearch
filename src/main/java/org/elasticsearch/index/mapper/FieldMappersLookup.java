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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A class that holds a map of field mappers from name, index name, and full name.
 */
public class FieldMappersLookup implements Iterable<FieldMapper> {

    private volatile ImmutableList<FieldMapper> mappers;
    private volatile ImmutableOpenMap<String, FieldMappers> name;
    private volatile ImmutableOpenMap<String, FieldMappers> indexName;
    private volatile ImmutableOpenMap<String, FieldMappers> fullName;

    public FieldMappersLookup() {
        this.mappers = ImmutableList.of();
        this.fullName = ImmutableOpenMap.of();
        this.name = ImmutableOpenMap.of();
        this.indexName = ImmutableOpenMap.of();
    }

    /**
     * Adds a new set of mappers.
     */
    public void addNewMappers(Iterable<FieldMapper> newMappers) {
        final ImmutableOpenMap.Builder<String, FieldMappers> tempName = ImmutableOpenMap.builder(name);
        final ImmutableOpenMap.Builder<String, FieldMappers> tempIndexName = ImmutableOpenMap.builder(indexName);
        final ImmutableOpenMap.Builder<String, FieldMappers> tempFullName = ImmutableOpenMap.builder(fullName);

        for (FieldMapper fieldMapper : newMappers) {
            FieldMappers mappers = tempName.get(fieldMapper.names().name());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            tempName.put(fieldMapper.names().name(), mappers);

            mappers = tempIndexName.get(fieldMapper.names().indexName());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            tempIndexName.put(fieldMapper.names().indexName(), mappers);

            mappers = tempFullName.get(fieldMapper.names().fullName());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            tempFullName.put(fieldMapper.names().fullName(), mappers);
        }
        this.mappers = ImmutableList.<FieldMapper>builder().addAll(this.mappers).addAll(newMappers).build();
        this.name = tempName.build();
        this.indexName = tempIndexName.build();
        this.fullName = tempFullName.build();
    }

    /**
     * Removes the set of mappers.
     */
    public void removeMappers(Iterable<FieldMapper> mappersToRemove) {
        List<FieldMapper> tempMappers = new ArrayList<>(this.mappers);
        ImmutableOpenMap.Builder<String, FieldMappers> tempName = ImmutableOpenMap.builder(this.name);
        ImmutableOpenMap.Builder<String, FieldMappers> tempIndexName = ImmutableOpenMap.builder(this.indexName);
        ImmutableOpenMap.Builder<String, FieldMappers> tempFullName = ImmutableOpenMap.builder(this.fullName);

        for (FieldMapper mapper : mappersToRemove) {
            FieldMappers mappers = tempName.get(mapper.names().name());
            if (mappers != null) {
                mappers = mappers.remove(mapper);
                if (mappers.isEmpty()) {
                    tempName.remove(mapper.names().name());
                } else {
                    tempName.put(mapper.names().name(), mappers);
                }
            }

            mappers = tempIndexName.get(mapper.names().indexName());
            if (mappers != null) {
                mappers = mappers.remove(mapper);
                if (mappers.isEmpty()) {
                    tempIndexName.remove(mapper.names().indexName());
                } else {
                    tempIndexName.put(mapper.names().indexName(), mappers);
                }
            }

            mappers = tempFullName.get(mapper.names().fullName());
            if (mappers != null) {
                mappers = mappers.remove(mapper);
                if (mappers.isEmpty()) {
                    tempFullName.remove(mapper.names().fullName());
                } else {
                    tempFullName.put(mapper.names().fullName(), mappers);
                }
            }

            tempMappers.remove(mapper);
        }


        this.mappers = ImmutableList.copyOf(tempMappers);
        this.name = tempName.build();
        this.indexName = tempIndexName.build();
        this.fullName = tempFullName.build();
    }

    @Override
    public UnmodifiableIterator<FieldMapper> iterator() {
        return mappers.iterator();
    }

    /**
     * The list of all mappers.
     */
    public ImmutableList<FieldMapper> mappers() {
        return this.mappers;
    }

    /**
     * Is there a mapper (based on unique {@link FieldMapper} identity)?
     */
    public boolean hasMapper(FieldMapper fieldMapper) {
        return mappers.contains(fieldMapper);
    }

    /**
     * Returns the field mappers based on the mapper name.
     */
    public FieldMappers name(String name) {
        return this.name.get(name);
    }

    /**
     * Returns the field mappers based on the mapper index name.
     */
    public FieldMappers indexName(String indexName) {
        return this.indexName.get(indexName);
    }

    /**
     * Returns the field mappers based on the mapper full name.
     */
    public FieldMappers fullName(String fullName) {
        return this.fullName.get(fullName);
    }

    /**
     * Returns a set of the index names of a simple match regex like pattern against full name, name and index name.
     */
    public Set<String> simpleMatchToIndexNames(String pattern) {
        Set<String> fields = Sets.newHashSet();
        for (FieldMapper fieldMapper : mappers) {
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
        for (FieldMapper fieldMapper : mappers) {
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
    public FieldMapper smartNameFieldMapper(String name) {
        FieldMappers fieldMappers = smartName(name);
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
    }
}
