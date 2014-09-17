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

import com.google.common.collect.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.UpdateInPlaceMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * A class that holds a map of field mappers from name, index name, and full name.
 */
public class FieldMappersLookup implements Iterable<FieldMapper> {

    private volatile FieldMapper[] mappers;
    private volatile List<FieldMapper> mappersAsList;
    private final UpdateInPlaceMap<String, FieldMappers> name;
    private final UpdateInPlaceMap<String, FieldMappers> indexName;
    private final UpdateInPlaceMap<String, FieldMappers> fullName;

    public FieldMappersLookup(Settings settings) {
        this.mappers = new FieldMapper[0];
        this.mappersAsList = ImmutableList.of();
        this.fullName = UpdateInPlaceMap.of(MapperService.getFieldMappersCollectionSwitch(settings));
        this.name = UpdateInPlaceMap.of(MapperService.getFieldMappersCollectionSwitch(settings));
        this.indexName = UpdateInPlaceMap.of(MapperService.getFieldMappersCollectionSwitch(settings));
    }

    /**
     * Adds a new set of mappers.
     */
    public void addNewMappers(List<FieldMapper> newMappers) {
        final UpdateInPlaceMap<String, FieldMappers>.Mutator mutatorName = name.mutator();
        final UpdateInPlaceMap<String, FieldMappers>.Mutator mutatorIndexName = indexName.mutator();
        final UpdateInPlaceMap<String, FieldMappers>.Mutator mutatorFullName = fullName.mutator();

        for (FieldMapper fieldMapper : newMappers) {
            FieldMappers mappers = mutatorName.get(fieldMapper.names().name());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            mutatorName.put(fieldMapper.names().name(), mappers);

            mappers = mutatorIndexName.get(fieldMapper.names().indexName());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            mutatorIndexName.put(fieldMapper.names().indexName(), mappers);

            mappers = mutatorFullName.get(fieldMapper.names().fullName());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            mutatorFullName.put(fieldMapper.names().fullName(), mappers);
        }
        FieldMapper[] tempMappers = new FieldMapper[this.mappers.length + newMappers.size()];
        System.arraycopy(mappers, 0, tempMappers, 0, mappers.length);
        int counter = 0;
        for (int i = mappers.length; i < tempMappers.length; i++) {
            tempMappers[i] = newMappers.get(counter++);
        }
        this.mappers = tempMappers;
        this.mappersAsList = Arrays.asList(this.mappers);

        mutatorName.close();
        mutatorIndexName.close();
        mutatorFullName.close();
    }

    /**
     * Removes the set of mappers.
     */
    public void removeMappers(Iterable<FieldMapper> mappersToRemove) {
        List<FieldMapper> tempMappers = Lists.newArrayList(this.mappers);
        final UpdateInPlaceMap<String, FieldMappers>.Mutator mutatorName = name.mutator();
        final UpdateInPlaceMap<String, FieldMappers>.Mutator mutatorIndexName = indexName.mutator();
        final UpdateInPlaceMap<String, FieldMappers>.Mutator mutatorFullName = fullName.mutator();

        for (FieldMapper mapper : mappersToRemove) {
            FieldMappers mappers = mutatorName.get(mapper.names().name());
            if (mappers != null) {
                mappers = mappers.remove(mapper);
                if (mappers.isEmpty()) {
                    mutatorName.remove(mapper.names().name());
                } else {
                    mutatorName.put(mapper.names().name(), mappers);
                }
            }

            mappers = mutatorIndexName.get(mapper.names().indexName());
            if (mappers != null) {
                mappers = mappers.remove(mapper);
                if (mappers.isEmpty()) {
                    mutatorIndexName.remove(mapper.names().indexName());
                } else {
                    mutatorIndexName.put(mapper.names().indexName(), mappers);
                }
            }

            mappers = mutatorFullName.get(mapper.names().fullName());
            if (mappers != null) {
                mappers = mappers.remove(mapper);
                if (mappers.isEmpty()) {
                    mutatorFullName.remove(mapper.names().fullName());
                } else {
                    mutatorFullName.put(mapper.names().fullName(), mappers);
                }
            }

            tempMappers.remove(mapper);
        }


        this.mappers = tempMappers.toArray(new FieldMapper[tempMappers.size()]);
        this.mappersAsList = Arrays.asList(this.mappers);
        mutatorName.close();
        mutatorIndexName.close();
        mutatorFullName.close();
    }

    @Override
    public UnmodifiableIterator<FieldMapper> iterator() {
        return Iterators.unmodifiableIterator(mappersAsList.iterator());
    }

    /**
     * The list of all mappers.
     */
    public List<FieldMapper> mappers() {
        return this.mappersAsList;
    }

    /**
     * Is there a mapper (based on unique {@link FieldMapper} identity)?
     */
    public boolean hasMapper(FieldMapper fieldMapper) {
        return mappersAsList.contains(fieldMapper);
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
