/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
     * A lookup representing an empty mapping. It can be used to look up fields, although it won't hold any, but it does not
     * hold a valid {@link DocumentParser}, {@link IndexSettings} or {@link IndexAnalyzers}.
     */
    public static final MappingLookup EMPTY = fromMappers(Mapping.EMPTY, List.of(), List.of(), List.of());

    private final CacheKey cacheKey = new CacheKey();

    /** Full field name to mapper */
    private final Map<String, Mapper> fieldMappers;
    private final Map<String, ObjectMapper> objectMappers;
    private final int runtimeFieldMappersCount;
    private final NestedLookup nestedLookup;
    private final FieldTypeLookup fieldTypeLookup;
    private final FieldTypeLookup indexTimeLookup;  // for index-time scripts, a lookup that does not include runtime fields
    private final Map<String, NamedAnalyzer> indexAnalyzersMap;
    private final List<FieldMapper> indexTimeScriptMappers;
    private final Mapping mapping;
    private final Set<String> completionFields;

    /**
     * Creates a new {@link MappingLookup} instance by parsing the provided mapping and extracting its field definitions.
     *
     * @param mapping the mapping source
     * @return the newly created lookup instance
     */
    public static MappingLookup fromMapping(Mapping mapping) {
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
        return new MappingLookup(mapping, newFieldMappers, newObjectMappers, newFieldAliasMappers);
    }

    private static void collect(
        Mapper mapper,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldMapper> fieldMappers,
        Collection<FieldAliasMapper> fieldAliasMappers
    ) {
        if (mapper instanceof ObjectMapper) {
            objectMappers.add((ObjectMapper) mapper);
        } else if (mapper instanceof FieldMapper) {
            fieldMappers.add((FieldMapper) mapper);
        } else if (mapper instanceof FieldAliasMapper) {
            fieldAliasMappers.add((FieldAliasMapper) mapper);
        } else {
            throw new IllegalStateException("Unrecognized mapper type [" + mapper.getClass().getSimpleName() + "].");
        }

        for (Mapper child : mapper) {
            collect(child, objectMappers, fieldMappers, fieldAliasMappers);
        }
    }

    /**
     * Creates a new {@link MappingLookup} instance given the provided mappers and mapping.
     * Note that the provided mappings are not re-parsed but only exposed as-is. No consistency is enforced between
     * the provided mappings and set of mappers.
     * This is a commodity method to be used in tests, or whenever no mappings are defined for an index.
     * When creating a MappingLookup through this method, its exposed functionalities are limited as it does not
     * hold a valid {@link DocumentParser}, {@link IndexSettings} or {@link IndexAnalyzers}.
     *
     * @param mapping the mapping
     * @param mappers the field mappers
     * @param objectMappers the object mappers
     * @param aliasMappers the field alias mappers
     * @return the newly created lookup instance
     */
    public static MappingLookup fromMappers(
        Mapping mapping,
        Collection<FieldMapper> mappers,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldAliasMapper> aliasMappers
    ) {
        return new MappingLookup(mapping, mappers, objectMappers, aliasMappers);
    }

    private MappingLookup(
        Mapping mapping,
        Collection<FieldMapper> mappers,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldAliasMapper> aliasMappers
    ) {
        this.mapping = mapping;
        Map<String, Mapper> fieldMappers = new HashMap<>();
        Map<String, ObjectMapper> objects = new HashMap<>();

        List<NestedObjectMapper> nestedMappers = new ArrayList<>();
        for (ObjectMapper mapper : objectMappers) {
            if (objects.put(mapper.fullPath(), mapper) != null) {
                throw new MapperParsingException("Object mapper [" + mapper.fullPath() + "] is defined more than once");
            }
            if (mapper.isNested()) {
                nestedMappers.add((NestedObjectMapper) mapper);
            }
        }
        this.nestedLookup = NestedLookup.build(nestedMappers);

        final Map<String, NamedAnalyzer> indexAnalyzersMap = new HashMap<>();
        final Set<String> completionFields = new HashSet<>();
        final List<FieldMapper> indexTimeScriptMappers = new ArrayList<>();
        for (FieldMapper mapper : mappers) {
            if (objects.containsKey(mapper.name())) {
                throw new MapperParsingException("Field [" + mapper.name() + "] is defined both as an object and a field");
            }
            if (fieldMappers.put(mapper.name(), mapper) != null) {
                throw new MapperParsingException("Field [" + mapper.name() + "] is defined more than once");
            }
            indexAnalyzersMap.putAll(mapper.indexAnalyzers());
            if (mapper.hasScript()) {
                indexTimeScriptMappers.add(mapper);
            }
            if (mapper instanceof CompletionFieldMapper) {
                completionFields.add(mapper.name());
            }
        }

        for (FieldAliasMapper aliasMapper : aliasMappers) {
            if (objects.containsKey(aliasMapper.name())) {
                throw new MapperParsingException("Alias [" + aliasMapper.name() + "] is defined both as an object and an alias");
            }
            if (fieldMappers.put(aliasMapper.name(), aliasMapper) != null) {
                throw new MapperParsingException("Alias [" + aliasMapper.name() + "] is defined both as an alias and a concrete field");
            }
        }

        final Collection<RuntimeField> runtimeFields = mapping.getRoot().runtimeFields();
        this.fieldTypeLookup = new FieldTypeLookup(mappers, aliasMappers, runtimeFields);
        if (runtimeFields.isEmpty()) {
            // without runtime fields this is the same as the field type lookup
            this.indexTimeLookup = fieldTypeLookup;
        } else {
            this.indexTimeLookup = new FieldTypeLookup(mappers, aliasMappers, Collections.emptyList());
        }
        // make all fields into compact+fast immutable maps
        this.fieldMappers = Map.copyOf(fieldMappers);
        this.objectMappers = Map.copyOf(objects);
        this.runtimeFieldMappersCount = runtimeFields.size();
        this.indexAnalyzersMap = Map.copyOf(indexAnalyzersMap);
        this.completionFields = Set.copyOf(completionFields);
        this.indexTimeScriptMappers = List.copyOf(indexTimeScriptMappers);

        runtimeFields.stream().flatMap(RuntimeField::asMappedFieldTypes).map(MappedFieldType::name).forEach(this::validateDoesNotShadow);
        assert assertMapperNamesInterned(this.fieldMappers, this.objectMappers);
    }

    private static boolean assertMapperNamesInterned(Map<String, Mapper> mappers, Map<String, ObjectMapper> objectMappers) {
        mappers.forEach(MappingLookup::assertNamesInterned);
        objectMappers.forEach(MappingLookup::assertNamesInterned);
        return true;
    }

    private static void assertNamesInterned(String name, Mapper mapper) {
        assert name == name.intern();
        assert mapper.name() == mapper.name().intern();
        assert mapper.simpleName() == mapper.simpleName().intern();
        if (mapper instanceof ObjectMapper) {
            ((ObjectMapper) mapper).mappers.forEach(MappingLookup::assertNamesInterned);
        }
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

    FieldTypeLookup fieldTypesLookup() {
        return fieldTypeLookup;
    }

    FieldTypeLookup indexTimeLookup() {
        return indexTimeLookup;
    }

    List<FieldMapper> indexTimeScriptMappers() {
        return indexTimeScriptMappers;
    }

    public NamedAnalyzer indexAnalyzer(String field, Function<String, NamedAnalyzer> unmappedFieldAnalyzer) {
        final NamedAnalyzer analyzer = indexAnalyzersMap.get(field);
        if (analyzer != null) {
            return analyzer;
        }
        return unmappedFieldAnalyzer.apply(field);
    }

    /**
     * Returns an iterable over all the registered field mappers (including alias mappers)
     */
    public Iterable<Mapper> fieldMappers() {
        return fieldMappers.values();
    }

    /**
     * Gets the postings format for a particular field
     * @param field the field to retrieve a postings format for
     * @return the postings format for the field, or {@code null} if the default format should be used
     */
    public PostingsFormat getPostingsFormat(String field) {
        return completionFields.contains(field) ? CompletionFieldMapper.postingsFormat() : null;
    }

    void checkLimits(IndexSettings settings) {
        checkFieldLimit(settings.getMappingTotalFieldsLimit());
        checkObjectDepthLimit(settings.getMappingDepthLimit());
        checkFieldNameLengthLimit(settings.getMappingFieldNameLengthLimit());
        checkNestedLimit(settings.getMappingNestedFieldsLimit());
        checkDimensionFieldLimit(settings.getMappingDimensionFieldsLimit());
    }

    private void checkFieldLimit(long limit) {
        checkFieldLimit(limit, 0);
    }

    void checkFieldLimit(long limit, int additionalFieldsToAdd) {
        if (fieldMappers.size() + objectMappers.size() + runtimeFieldMappersCount + additionalFieldsToAdd - mapping
            .getSortedMetadataMappers().length > limit) {
            throw new IllegalArgumentException(
                "Limit of total fields ["
                    + limit
                    + "] has been exceeded"
                    + (additionalFieldsToAdd > 0 ? " while adding new fields [" + additionalFieldsToAdd + "]" : "")
            );
        }
    }

    private void checkDimensionFieldLimit(long limit) {
        long dimensionFieldCount = fieldMappers.values()
            .stream()
            .filter(m -> m instanceof FieldMapper && ((FieldMapper) m).fieldType().isDimension())
            .count();
        if (dimensionFieldCount > limit) {
            throw new IllegalArgumentException("Limit of total dimension fields [" + limit + "] has been exceeded");
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
                throw new IllegalArgumentException(
                    "Limit of mapping depth [" + limit + "] has been exceeded due to object field [" + objectPath + "]"
                );
            }
        }
    }

    private void checkFieldNameLengthLimit(long limit) {
        validateMapperNameIn(objectMappers.values(), limit);
        validateMapperNameIn(fieldMappers.values(), limit);
    }

    private static void validateMapperNameIn(Collection<? extends Mapper> mappers, long limit) {
        for (Mapper mapper : mappers) {
            String name = mapper.simpleName();
            if (name.length() > limit) {
                throw new IllegalArgumentException("Field name [" + name + "] is longer than the limit of [" + limit + "] characters");
            }
        }
    }

    private void checkNestedLimit(long limit) {
        long actualNestedFields = 0;
        for (ObjectMapper objectMapper : objectMappers.values()) {
            if (objectMapper.isNested()) {
                actualNestedFields++;
            }
        }
        if (actualNestedFields > limit) {
            throw new IllegalArgumentException("Limit of nested fields [" + limit + "] has been exceeded");
        }
    }

    public Map<String, ObjectMapper> objectMappers() {
        return objectMappers;
    }

    public NestedLookup nestedLookup() {
        return nestedLookup;
    }

    public boolean isMultiField(String field) {
        if (fieldMappers.containsKey(field) == false) {
            return false;
        }
        // Is it a runtime field?
        if (indexTimeLookup.get(field) != fieldTypeLookup.get(field)) {
            return false;
        }
        String sourceParent = parentObject(field);
        return sourceParent != null && fieldMappers.containsKey(sourceParent);
    }

    public boolean isObjectField(String field) {
        return objectMappers.containsKey(field);
    }

    private static String parentObject(String field) {
        int lastDot = field.lastIndexOf('.');
        if (lastDot == -1) {
            return null;
        }
        return field.substring(0, lastDot);
    }

    /**
     * Returns a set of field names that match a regex-like pattern
     *
     * All field names in the returned set are guaranteed to resolve to a field
     *
     * @param pattern the pattern to match field names against
     */
    public Set<String> getMatchingFieldNames(String pattern) {
        return fieldTypeLookup.getMatchingFieldNames(pattern);
    }

    /**
     * Returns the mapped field type for the given field name.
     */
    public MappedFieldType getFieldType(String field) {
        return fieldTypesLookup().get(field);
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
        return fieldTypesLookup().sourcePaths(field);
    }

    /**
     * Returns true if the index has mappings. An index does not have mappings only if it was created
     * without providing mappings explicitly, and no documents have yet been indexed in it.
     * @return true if the current index has mappings, false otherwise
     */
    public boolean hasMappings() {
        return this != EMPTY;
    }

    public boolean isSourceEnabled() {
        SourceFieldMapper sfm = mapping.getMetadataMapperByClass(SourceFieldMapper.class);
        return sfm != null && sfm.enabled();
    }

    /**
     * Build something to load source {@code _source}.
     */
    public SourceLoader newSourceLoader() {
        SourceFieldMapper sfm = mapping.getMetadataMapperByClass(SourceFieldMapper.class);
        return sfm == null ? SourceLoader.FROM_STORED_SOURCE : sfm.newSourceLoader(mapping);
    }

    /**
     * Returns if this mapping contains a data-stream's timestamp meta-field and this field is enabled.
     * Only indices that are a part of a data-stream have this meta-field enabled.
     * @return {@code true} if contains an enabled data-stream's timestamp meta-field, {@code false} otherwise.
     */
    public boolean isDataStreamTimestampFieldEnabled() {
        DataStreamTimestampFieldMapper dtfm = mapping.getMetadataMapperByClass(DataStreamTimestampFieldMapper.class);
        return dtfm != null && dtfm.isEnabled();
    }

    /**
     * Returns if this mapping contains a timestamp field that is of type date, indexed and has doc values.
     * @return {@code true} if contains a timestamp field of type date that is indexed and has doc values, {@code false} otherwise.
     */
    public boolean hasTimestampField() {
        final MappedFieldType mappedFieldType = fieldTypesLookup().get(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD);
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType) {
            return mappedFieldType.isIndexed() && mappedFieldType.hasDocValues();
        } else {
            return false;
        }
    }

    /**
     * Key for the lookup to be used in caches.
     */
    public CacheKey cacheKey() {
        return cacheKey;
    }

    /**
     * Returns the mapping source that this lookup originated from
     * @return the mapping source
     */
    public Mapping getMapping() {
        return mapping;
    }

    /**
     * Check if the provided {@link MappedFieldType} shadows a dimension
     * or metric field.
     */
    public void validateDoesNotShadow(String name) {
        MappedFieldType shadowed = indexTimeLookup.get(name);
        if (shadowed == null) {
            return;
        }
        if (shadowed.isDimension()) {
            throw new MapperParsingException("Field [" + name + "] attempted to shadow a time_series_dimension");
        }
        if (shadowed.getMetricType() != null) {
            throw new MapperParsingException("Field [" + name + "] attempted to shadow a time_series_metric");
        }
    }

    /**
     * Returns a SourceProvider describing how to read the source. If using synthetic source, returns the null source provider
     * expecting the source to either be provided later by a fetch phase or not be accessed at all (as in scripts).
     * @return
     */
    public SourceLookup.SourceProvider getSourceProvider() {
        SourceFieldMapper sourceMapper = (SourceFieldMapper) getMapper("_source");
        if (sourceMapper == null || sourceMapper.isSynthetic() == false) {
            return new SourceLookup.ReaderSourceProvider();
        } else {
            return new SourceLookup.NullSourceProvider();
        }
    }
}
