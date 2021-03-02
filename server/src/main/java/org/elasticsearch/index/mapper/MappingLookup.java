/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A (mostly) immutable snapshot of the current mapping of an index with
 * access to everything we need for the search phase.
 */
public final class MappingLookup {
    /**
     * Key for the lookup to be used in caches.
     */
    public static class CacheKey {
        private CacheKey() {}
    }

    /**
     * A lookup representing an empty mapping.
     */
    public static final MappingLookup EMPTY = new MappingLookup(
        Mapping.EMPTY,
        List.of(),
        List.of(),
        List.of(),
        null,
        null,
        null
    );

    private final CacheKey cacheKey = new CacheKey();

    /** Full field name to mapper */
    private final Map<String, Mapper> fieldMappers;
    private final Map<String, ObjectMapper> objectMappers;
    private final boolean hasNested;
    private final FieldTypeLookup fieldTypeLookup;
    private final Map<String, NamedAnalyzer> indexAnalyzersMap = new HashMap<>();
    private final DocumentParser documentParser;
    private final Mapping mapping;
    private final IndexSettings indexSettings;
    private final IndexAnalyzers indexAnalyzers;

    public static MappingLookup fromMapping(Mapping mapping,
                                            DocumentParser documentParser,
                                            IndexSettings indexSettings,
                                            IndexAnalyzers indexAnalyzers) {
        List<ObjectMapper> newObjectMappers = new ArrayList<>();
        List<FieldMapper> newFieldMappers = new ArrayList<>();
        List<FieldAliasMapper> newFieldAliasMappers = new ArrayList<>();
        for (MetadataFieldMapper metadataMapper : mapping.getSortedMetadataMappers()) {
            if (metadataMapper != null) {
                newFieldMappers.add(metadataMapper);
            }
        }
        for (Mapper child : mapping.getRoot()) {
            collect(child, newObjectMappers, newFieldMappers, newFieldAliasMappers);
        }
        return new MappingLookup(
            mapping,
            newFieldMappers,
            newObjectMappers,
            newFieldAliasMappers,
            documentParser,
            indexSettings,
            indexAnalyzers
        );
    }

    private static void collect(Mapper mapper, Collection<ObjectMapper> objectMappers,
                               Collection<FieldMapper> fieldMappers,
                               Collection<FieldAliasMapper> fieldAliasMappers) {
        if (mapper instanceof ObjectMapper) {
            objectMappers.add((ObjectMapper)mapper);
        } else if (mapper instanceof FieldMapper) {
            fieldMappers.add((FieldMapper)mapper);
        } else if (mapper instanceof FieldAliasMapper) {
            fieldAliasMappers.add((FieldAliasMapper) mapper);
        } else {
            throw new IllegalStateException("Unrecognized mapper type [" + mapper.getClass().getSimpleName() + "].");
        }

        for (Mapper child : mapper) {
            collect(child, objectMappers, fieldMappers, fieldAliasMappers);
        }
    }

    public MappingLookup(Mapping mapping,
                         Collection<FieldMapper> mappers,
                         Collection<ObjectMapper> objectMappers,
                         Collection<FieldAliasMapper> aliasMappers,
                         DocumentParser documentParser,
                         IndexSettings indexSettings,
                         IndexAnalyzers indexAnalyzers) {
        this.mapping = mapping;
        this.indexSettings = indexSettings;
        this.indexAnalyzers = indexAnalyzers;
        this.documentParser = documentParser;
        Map<String, Mapper> fieldMappers = new HashMap<>();
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
            indexAnalyzersMap.putAll(mapper.indexAnalyzers());
        }

        for (FieldAliasMapper aliasMapper : aliasMappers) {
            if (objects.containsKey(aliasMapper.name())) {
                throw new MapperParsingException("Alias [" + aliasMapper.name() + "] is defined both as an object and an alias");
            }
            if (fieldMappers.put(aliasMapper.name(), aliasMapper) != null) {
                throw new MapperParsingException("Alias [" + aliasMapper.name() + "] is defined both as an alias and a concrete field");
            }
        }

        this.fieldTypeLookup = new FieldTypeLookup(mappers, aliasMappers, mapping.getRoot().runtimeFieldTypes());
        this.fieldMappers = Collections.unmodifiableMap(fieldMappers);
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

    FieldTypeLookup fieldTypes() {
        return fieldTypeLookup;
    }

    public NamedAnalyzer indexAnalyzer(String field, Function<String, NamedAnalyzer> unmappedFieldAnalyzer) {
        if (this.indexAnalyzersMap.containsKey(field)) {
            return this.indexAnalyzersMap.get(field);
        }
        return unmappedFieldAnalyzer.apply(field);
    }

    /**
     * Returns an iterable over all the registered field mappers (including alias mappers)
     */
    public Iterable<Mapper> fieldMappers() {
        return fieldMappers.values();
    }

    void checkLimits(IndexSettings settings) {
        checkFieldLimit(settings.getMappingTotalFieldsLimit());
        checkObjectDepthLimit(settings.getMappingDepthLimit());
        checkFieldNameLengthLimit(settings.getMappingFieldNameLengthLimit());
        checkNestedLimit(settings.getMappingNestedFieldsLimit());
    }

    private void checkFieldLimit(long limit) {
        if (fieldMappers.size() + objectMappers.size() - mapping.getSortedMetadataMappers().length > limit) {
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

    public Set<String> simpleMatchToFullName(String pattern) {
        return fieldTypes().simpleMatchToFullName(pattern);
    }

    /**
     * Returns the mapped field type for the given field name.
     */
    public MappedFieldType getFieldType(String field) {
        return fieldTypes().get(field);
    }

    /**
     * Given a concrete field name, return its paths in the _source.
     *
     * For most fields, the source path is the same as the field itself. However
     * there are cases where a field's values are found elsewhere in the _source:
     *   - For a multi-field, the source path is the parent field.
     *   - One field's content could have been copied to another through copy_to.
     *
     * @param field The field for which to look up the _source path. Note that the field
     *              should be a concrete field and *not* an alias.
     * @return A set of paths in the _source that contain the field's values.
     */
    public Set<String> sourcePaths(String field) {
        return fieldTypes().sourcePaths(field);
    }

    public ParsedDocument parseDocument(SourceToParse source) {
        return documentParser.parseDocument(source, this);
    }

    public boolean hasMappings() {
        return this != EMPTY;
    }

    public boolean isSourceEnabled() {
        SourceFieldMapper sfm = mapping.getMetadataMapperByClass(SourceFieldMapper.class);
        return sfm != null && sfm.enabled();
    }

    /**
     * Key for the lookup to be used in caches.
     */
    public CacheKey cacheKey() {
        return cacheKey;
    }

    Mapping getMapping() {
        return mapping;
    }

    IndexSettings getIndexSettings() {
        return indexSettings;
    }

    IndexAnalyzers getIndexAnalyzers() {
        return indexAnalyzers;
    }

    /**
     * Given an object path, checks to see if any of its parents are non-nested objects
     */
    public boolean hasNonNestedParent(String path) {
        ObjectMapper mapper = objectMappers().get(path);
        if (mapper == null) {
            return false;
        }
        while (mapper != null) {
            if (mapper.nested().isNested() == false) {
                return true;
            }
            if (path.contains(".") == false) {
                return false;
            }
            path = path.substring(0, path.lastIndexOf("."));
            mapper = objectMappers().get(path);
        }
        return false;
    }

    /**
     * Returns all nested object mappers
     */
    public List<ObjectMapper> getNestedMappers() {
        List<ObjectMapper> childMappers = new ArrayList<>();
        for (ObjectMapper mapper : objectMappers().values()) {
            if (mapper.nested().isNested() == false) {
                continue;
            }
            childMappers.add(mapper);
        }
        return childMappers;
    }

    /**
     * Returns all nested object mappers which contain further nested object mappers
     *
     * Used by BitSetProducerWarmer
     */
    public List<ObjectMapper> getNestedParentMappers() {
        List<ObjectMapper> parents = new ArrayList<>();
        for (ObjectMapper mapper : objectMappers().values()) {
            String nestedParentPath = getNestedParent(mapper.fullPath());
            if (nestedParentPath == null) {
                continue;
            }
            ObjectMapper parent = objectMappers().get(nestedParentPath);
            if (parent.nested().isNested()) {
                parents.add(parent);
            }
        }
        return parents;
    }

    /**
     * Given a nested object path, returns the path to its nested parent
     *
     * In particular, if a nested field `foo` contains an object field
     * `bar.baz`, then calling this method with `foo.bar.baz` will return
     * the path `foo`, skipping over the object-but-not-nested `foo.bar`
     */
    public String getNestedParent(String path) {
        ObjectMapper mapper = objectMappers().get(path);
        if (mapper == null) {
            return null;
        }
        if (path.contains(".") == false) {
            return null;
        }
        do {
            path = path.substring(0, path.lastIndexOf("."));
            mapper = objectMappers().get(path);
            if (mapper == null) {
                return null;
            }
            if (mapper.nested().isNested()) {
                return path;
            }
            if (path.contains(".") == false) {
                return null;
            }
        } while(true);
    }
}
