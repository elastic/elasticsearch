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

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class MappingLookup implements Iterable<Mapper> {

    /** Full field name to mapper */
    private final Map<String, Mapper> fieldMappers;
    private final Map<String, ObjectMapper> objectMappers;
    private final boolean hasNested;
    private final FieldTypeLookup fieldTypeLookup;
    private final int metadataFieldCount;
    private final FieldNameAnalyzer indexAnalyzer;

    private static void put(Map<String, Analyzer> analyzers, String key, Analyzer value, Analyzer defaultValue) {
        if (value == null) {
            value = defaultValue;
        }
        analyzers.put(key, value);
    }

    public static MappingLookup fromMapping(Mapping mapping, Analyzer defaultIndex) {
        List<ObjectMapper> newObjectMappers = new ArrayList<>();
        List<FieldMapper> newFieldMappers = new ArrayList<>();
        List<FieldAliasMapper> newFieldAliasMappers = new ArrayList<>();
        for (MetadataFieldMapper metadataMapper : mapping.metadataMappers) {
            if (metadataMapper != null) {
                newFieldMappers.add(metadataMapper);
            }
        }
        collect(mapping.root, newObjectMappers, newFieldMappers, newFieldAliasMappers);
        return new MappingLookup(newFieldMappers, newObjectMappers, newFieldAliasMappers, mapping.metadataMappers.length, defaultIndex);
    }

    private static void collect(Mapper mapper, Collection<ObjectMapper> objectMappers,
                               Collection<FieldMapper> fieldMappers,
                               Collection<FieldAliasMapper> fieldAliasMappers) {
        if (mapper instanceof RootObjectMapper) {
            // root mapper isn't really an object mapper
        } else if (mapper instanceof ObjectMapper) {
            objectMappers.add((ObjectMapper)mapper);
        } else if (mapper instanceof FieldMapper) {
            fieldMappers.add((FieldMapper)mapper);
        } else if (mapper instanceof FieldAliasMapper) {
            fieldAliasMappers.add((FieldAliasMapper) mapper);
        } else {
            throw new IllegalStateException("Unrecognized mapper type [" +
                mapper.getClass().getSimpleName() + "].");
        }

        for (Mapper child : mapper) {
            collect(child, objectMappers, fieldMappers, fieldAliasMappers);
        }
    }

    public MappingLookup(Collection<FieldMapper> mappers,
                         Collection<ObjectMapper> objectMappers,
                         Collection<FieldAliasMapper> aliasMappers,
                         int metadataFieldCount,
                         Analyzer defaultIndex) {
        Map<String, Mapper> fieldMappers = new HashMap<>();
        Map<String, Analyzer> indexAnalyzers = new HashMap<>();
        Map<String, ObjectMapper> objects = new HashMap<>();

        boolean hasNested = false;
        for (ObjectMapper mapper : objectMappers) {
            if (objects.put(mapper.fullPath(), mapper) != null) {
                throw new MapperParsingException("Object mapper [" + mapper.fullPath() + "] is defined more than once");
            }
            if (mapper.nested().isNested()) {
                hasNested = true;
            }
        }
        this.hasNested = hasNested;

        for (FieldMapper mapper : mappers) {
            if (objects.containsKey(mapper.name())) {
                throw new MapperParsingException("Field [" + mapper.name() + "] is defined both as an object and a field");
            }
            if (fieldMappers.put(mapper.name(), mapper) != null) {
                throw new MapperParsingException("Field [" + mapper.name() + "] is defined more than once");
            }
            MappedFieldType fieldType = mapper.fieldType();
            put(indexAnalyzers, fieldType.name(), fieldType.indexAnalyzer(), defaultIndex);
        }
        this.metadataFieldCount = metadataFieldCount;

        for (FieldAliasMapper aliasMapper : aliasMappers) {
            if (objects.containsKey(aliasMapper.name())) {
                throw new MapperParsingException("Alias [" + aliasMapper.name() + "] is defined both as an object and an alias");
            }
            if (fieldMappers.put(aliasMapper.name(), aliasMapper) != null) {
                throw new MapperParsingException("Alias [" + aliasMapper.name() + "] is defined both as an alias and a concrete field");
            }
        }

        this.fieldTypeLookup = new FieldTypeLookup(mappers, aliasMappers);

        this.fieldMappers = Collections.unmodifiableMap(fieldMappers);
        this.indexAnalyzer = new FieldNameAnalyzer(indexAnalyzers);
        this.objectMappers = Collections.unmodifiableMap(objects);
    }

    /**
     * Returns the leaf mapper associated with this field name. Note that the returned mapper
     * could be either a concrete {@link FieldMapper}, or a {@link FieldAliasMapper}.
     *
     * To access a field's type information, {@link MapperService#fieldType} should be used instead.
     */
    public Mapper getMapper(String field) {
        return fieldMappers.get(field);
    }

    public FieldTypeLookup fieldTypes() {
        return fieldTypeLookup;
    }

    /**
     * A smart analyzer used for indexing that takes into account specific analyzers configured
     * per {@link FieldMapper}.
     */
    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override
    public Iterator<Mapper> iterator() {
        return fieldMappers.values().iterator();
    }

    public void checkLimits(IndexSettings settings) {
        checkFieldLimit(settings.getMappingTotalFieldsLimit());
        checkObjectDepthLimit(settings.getMappingDepthLimit());
        checkFieldNameLengthLimit(settings.getMappingFieldNameLengthLimit());
        checkNestedLimit(settings.getMappingNestedFieldsLimit());
    }

    private void checkFieldLimit(long limit) {
        if (fieldMappers.size() + objectMappers.size() - metadataFieldCount > limit) {
            throw new IllegalArgumentException("Limit of total fields [" + limit + "] has been exceeded");
        }
    }

    private void checkObjectDepthLimit(long limit) {
        for (String objectPath : objectMappers.keySet()) {
            int numDots = 0;
            for (int i = 0; i < objectPath.length(); ++i) {
                if (objectPath.charAt(i) == '.') {
                    numDots += 1;
                }
            }
            final int depth = numDots + 2;
            if (depth > limit) {
                throw new IllegalArgumentException("Limit of mapping depth [" + limit +
                    "] has been exceeded due to object field [" + objectPath + "]");
            }
        }
    }

    private void checkFieldNameLengthLimit(long limit) {
        Stream.of(objectMappers.values().stream(), fieldMappers.values().stream())
            .reduce(Stream::concat)
            .orElseGet(Stream::empty)
            .forEach(mapper -> {
                String name = mapper.simpleName();
                if (name.length() > limit) {
                    throw new IllegalArgumentException("Field name [" + name + "] is longer than the limit of [" + limit + "] characters");
                }
            });
    }

    private void checkNestedLimit(long limit) {
        long actualNestedFields = 0;
        for (ObjectMapper objectMapper : objectMappers.values()) {
            if (objectMapper.nested().isNested()) {
                actualNestedFields++;
            }
        }
        if (actualNestedFields > limit) {
            throw new IllegalArgumentException("Limit of nested fields [" + limit + "] has been exceeded");
        }
    }

    public boolean hasNested() {
        return hasNested;
    }

    public Map<String, ObjectMapper> objectMappers() {
        return objectMappers;
    }

    public boolean isMultiField(String field) {
        String sourceParent = parentObject(field);
        return sourceParent != null && fieldMappers.containsKey(sourceParent);
    }

    public boolean isObjectField(String field) {
        return objectMappers.containsKey(field);
    }

    public String getNestedScope(String path) {
        for (String parentPath = parentObject(path); parentPath != null; parentPath = parentObject(parentPath)) {
            ObjectMapper objectMapper = objectMappers.get(parentPath);
            if (objectMapper != null && objectMapper.nested().isNested()) {
                return parentPath;
            }
        }
        return null;
    }

    private static String parentObject(String field) {
        int lastDot = field.lastIndexOf('.');
        if (lastDot == -1) {
            return null;
        }
        return field.substring(0, lastDot);
    }
}
