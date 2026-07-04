/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.search.lookup.SourceFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
    public static final MappingLookup EMPTY = fromMappers(Mapping.EMPTY, List.of(), List.of(), IndexMode.STANDARD);

    private final CacheKey cacheKey = new CacheKey();

    /** Full field name to mapper */
    private final Map<String, Mapper> fieldMappers;
    private final Map<String, ObjectMapper> objectMappers;
    private final Map<String, InferenceFieldMetadata> inferenceFields;
    private final Set<String> syntheticVectorFields;
    private final Map<String, FieldMapper> dimensionFieldMappers;
    private final Map<String, FieldMapper> metricFieldMappers;
    private final int runtimeFieldMappersCount;
    private final NestedLookup nestedLookup;
    // [nullability=false] field paths partitioned by their nearest nested parent path ("" = the root document). Each Lucene doc is checked
    // against only its own partition: the root doc against "", each nested instance against its nested path. Empty when no required fields.
    private final Map<String, Set<String>> requiredFieldsByNestedParent;
    private final FieldTypeLookup fieldTypeLookup;
    private final FieldTypeLookup indexTimeLookup;  // for index-time scripts, a lookup that does not include runtime fields
    private final Map<String, NamedAnalyzer> indexAnalyzers;
    private final List<FieldMapper> indexTimeScriptMappers;
    private final Mapping mapping;
    private final int totalFieldsCount;
    private final IndexMode indexMode;

    // cached booleans from the _source field mapper
    private final boolean isSourceEnabled;
    private final boolean isSourceSynthetic;
    private final boolean isSourceColumnarStored;

    private final boolean isColumnarId;

    /**
     * Creates a new {@link MappingLookup} instance by parsing the provided mapping and extracting its field definitions.
     *
     * @param mapping the mapping source
     * @param indexMode the mode of the index
     * @return the newly created lookup instance
     */
    public static MappingLookup fromMapping(Mapping mapping, IndexMode indexMode) {
        List<ObjectMapper> newObjectMappers = new ArrayList<>();
        List<FieldMapper> newFieldMappers = new ArrayList<>();
        List<FieldAliasMapper> newFieldAliasMappers = new ArrayList<>();
        List<PassThroughFieldSource> newPassThroughSources = new ArrayList<>();
        for (MetadataFieldMapper metadataMapper : mapping.getSortedMetadataMappers()) {
            if (metadataMapper != null) {
                newFieldMappers.add(metadataMapper);
            }
        }
        for (Mapper child : mapping.getRoot()) {
            collect(child, newObjectMappers, newFieldMappers, newFieldAliasMappers, newPassThroughSources);
        }
        return new MappingLookup(mapping, newFieldMappers, newObjectMappers, newFieldAliasMappers, newPassThroughSources, indexMode);
    }

    private static void collect(
        Mapper mapper,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldMapper> fieldMappers,
        Collection<FieldAliasMapper> fieldAliasMappers,
        Collection<PassThroughFieldSource> passThroughSources
    ) {
        if (mapper instanceof PassThroughObjectMapper passThroughObjectMapper) {
            passThroughSources.add(passThroughObjectMapper);
            objectMappers.add(passThroughObjectMapper);
        } else if (mapper instanceof ObjectMapper objectMapper) {
            objectMappers.add(objectMapper);
        } else if (mapper instanceof FieldMapper fieldMapper) {
            fieldMappers.add(fieldMapper);
            if (fieldMapper instanceof PassThroughFieldSource pts && pts.passThroughSubFields().isEmpty() == false) {
                passThroughSources.add(pts);
            }
        } else if (mapper instanceof FieldAliasMapper fieldAliasMapper) {
            fieldAliasMappers.add(fieldAliasMapper);
        } else {
            throw new IllegalStateException("Unrecognized mapper type [" + mapper.getClass().getSimpleName() + "].");
        }

        for (Mapper child : mapper) {
            collect(child, objectMappers, fieldMappers, fieldAliasMappers, passThroughSources);
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
     * @param passThroughSources the pass-through field sources
     * @param indexMode the mode of the index
     * @return the newly created lookup instance
     */
    public static MappingLookup fromMappers(
        Mapping mapping,
        Collection<FieldMapper> mappers,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldAliasMapper> aliasMappers,
        Collection<PassThroughFieldSource> passThroughSources,
        IndexMode indexMode
    ) {
        return new MappingLookup(mapping, mappers, objectMappers, aliasMappers, passThroughSources, indexMode);
    }

    public static MappingLookup fromMappers(
        Mapping mapping,
        Collection<FieldMapper> mappers,
        Collection<ObjectMapper> objectMappers,
        IndexMode indexMode
    ) {
        return new MappingLookup(mapping, mappers, objectMappers, List.of(), List.of(), indexMode);
    }

    private MappingLookup(
        Mapping mapping,
        Collection<FieldMapper> mappers,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldAliasMapper> aliasMappers,
        Collection<PassThroughFieldSource> passThroughSources,
        IndexMode indexMode
    ) {
        this.totalFieldsCount = mapping.getRoot().getTotalFieldsCount();
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

        final Map<String, NamedAnalyzer> indexAnalyzers = new HashMap<>();
        final List<FieldMapper> indexTimeScriptMappers = new ArrayList<>();
        final Map<String, FieldMapper> dimensionMappers = new LinkedHashMap<>();
        final Map<String, FieldMapper> metricMappers = new LinkedHashMap<>();
        this.requiredFieldsByNestedParent = new HashMap<>();
        for (FieldMapper mapper : mappers) {
            if (objects.containsKey(mapper.fullPath())) {
                throw new MapperParsingException("Field [" + mapper.fullPath() + "] is defined both as an object and a field");
            }
            if (fieldMappers.put(mapper.fullPath(), mapper) != null) {
                throw new MapperParsingException("Field [" + mapper.fullPath() + "] is defined more than once");
            }
            indexAnalyzers.putAll(mapper.indexAnalyzers());
            if (mapper.hasScript()) {
                indexTimeScriptMappers.add(mapper);
            }
            if (mapper.isNullable() == false) {
                // Partition by nearest nested parent (null -> "" root document) so every Lucene doc is checked against only its own fields.
                String nestedParent = nestedLookup.getNestedParent(mapper.fullPath());
                requiredFieldsByNestedParent.computeIfAbsent(nestedParent == null ? "" : nestedParent, k -> new HashSet<>())
                    .add(mapper.fullPath());
            }
            MappedFieldType fieldType = mapper.fieldType();
            if (fieldType.isDimension()) {
                dimensionMappers.put(mapper.fullPath(), mapper);
            }
            if (fieldType.getMetricType() != null) {
                metricMappers.put(mapper.fullPath(), mapper);
            }
        }
        // Freeze inner sets only: requiredFields(...) returns them directly to per doc enforcement so they must be immutable. The outer Map
        // never escapes MappingLookup, so it stays a plain HashMap that is simply never mutated again, once this constructor finishes here.
        requiredFieldsByNestedParent.replaceAll((nestedParent, fields) -> Set.copyOf(fields));

        for (FieldAliasMapper aliasMapper : aliasMappers) {
            if (objects.containsKey(aliasMapper.fullPath())) {
                throw new MapperParsingException("Alias [" + aliasMapper.fullPath() + "] is defined both as an object and an alias");
            }
            if (fieldMappers.put(aliasMapper.fullPath(), aliasMapper) != null) {
                throw new MapperParsingException("Alias [" + aliasMapper.fullPath() + "] is defined both as an alias and a concrete field");
            }
        }

        PassThroughObjectMapper.checkForDuplicatePriorities(passThroughSources);
        final Collection<RuntimeField> runtimeFields = mapping.getRoot().runtimeFields();
        final Map<String, PrefixProperties> prefixProperties = mapping.getRoot().getPrefixProperties();
        this.fieldTypeLookup = new FieldTypeLookup(mappers, aliasMappers, passThroughSources, runtimeFields, prefixProperties);

        Map<String, InferenceFieldMetadata> inferenceFields = new HashMap<>();
        Set<String> syntheticVectorFields = new LinkedHashSet<>();
        for (FieldMapper mapper : mappers) {
            if (mapper instanceof InferenceFieldMapper inferenceFieldMapper) {
                inferenceFields.put(mapper.fullPath(), inferenceFieldMapper.getMetadata(fieldTypeLookup.sourcePaths(mapper.fullPath())));
            }
            if (mapper.syntheticVectorsLoader() != null) {
                syntheticVectorFields.add(mapper.fullPath());
            }
        }
        this.inferenceFields = Collections.unmodifiableMap(inferenceFields);
        this.syntheticVectorFields = Collections.unmodifiableSet(syntheticVectorFields);

        if (runtimeFields.isEmpty()) {
            // without runtime fields this is the same as the field type lookup
            this.indexTimeLookup = fieldTypeLookup;
        } else {
            this.indexTimeLookup = new FieldTypeLookup(
                mappers,
                aliasMappers,
                passThroughSources,
                Collections.emptyList(),
                prefixProperties
            );
        }
        // make all fields into compact+fast immutable maps
        this.fieldMappers = Collections.unmodifiableMap(fieldMappers);
        this.dimensionFieldMappers = Collections.unmodifiableMap(dimensionMappers);
        this.metricFieldMappers = Collections.unmodifiableMap(metricMappers);
        this.objectMappers = Collections.unmodifiableMap(objects);
        this.runtimeFieldMappersCount = runtimeFields.size();
        this.indexAnalyzers = Collections.unmodifiableMap(indexAnalyzers);
        this.indexTimeScriptMappers = Collections.unmodifiableList(indexTimeScriptMappers);
        this.indexMode = indexMode;

        runtimeFields.stream().flatMap(RuntimeField::asMappedFieldTypes).map(MappedFieldType::name).forEach(this::validateDoesNotShadow);
        assert assertMapperNamesInterned(this.fieldMappers, this.objectMappers);

        // cache these properties of the _source field for instantaneous access
        SourceFieldMapper sfm = mapping.getMetadataMapperByClass(SourceFieldMapper.class);
        this.isSourceEnabled = sfm != null && sfm.enabled();
        this.isSourceSynthetic = sfm != null && sfm.isSynthetic();
        this.isSourceColumnarStored = sfm != null && sfm.isColumnarStored();

        var idFieldMapper = mapping.getMetadataMapperByClass(ProvidedIdFieldMapper.class);
        this.isColumnarId = idFieldMapper != null && idFieldMapper.isColumnarMode();
    }

    private static boolean assertMapperNamesInterned(Map<String, Mapper> mappers, Map<String, ObjectMapper> objectMappers) {
        mappers.forEach(MappingLookup::assertNamesInterned);
        objectMappers.forEach(MappingLookup::assertNamesInterned);
        return true;
    }

    private static void assertNamesInterned(String name, Mapper mapper) {
        assert name == name.intern();
        assert mapper.fullPath() == mapper.fullPath().intern();
        assert mapper.leafName() == mapper.leafName().intern();
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

    /**
     * Returns the total number of fields defined in the mappings, including field mappers, object mappers as well as runtime fields.
     */
    public long getTotalFieldsCount() {
        return totalFieldsCount;
    }

    /**
     * Returns the total number of mappers defined in the mappings, including field mappers and their sub-fields
     * (which are not explicitly defined in the mappings), multi-fields, object mappers, runtime fields and metadata field mappers.
     */
    public long getTotalMapperCount() {
        return fieldMappers.size() + objectMappers.size() + runtimeFieldMappersCount;
    }

    FieldTypeLookup indexTimeLookup() {
        return indexTimeLookup;
    }

    List<FieldMapper> indexTimeScriptMappers() {
        return indexTimeScriptMappers;
    }

    public NamedAnalyzer indexAnalyzer(String field, Function<String, NamedAnalyzer> unmappedFieldAnalyzer) {
        final NamedAnalyzer analyzer = indexAnalyzers.get(field);
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
     * Returns the full path of the first field whose {@code _source} cannot be reconstructed from doc-value columns —
     * i.e. whose {@link FieldMapper.SyntheticSourceMode} is {@link FieldMapper.SyntheticSourceMode#FALLBACK} — or
     * {@code null} if every field is reconstructable. Columnar index modes rebuild {@code _source} purely from
     * doc-value columns and never keep a generic source fallback, so a fallback field (no doc values, or a type whose
     * doc-value encoding cannot rebuild its own source) has no columnar representation. The check covers every field
     * mapper, including those nested inside object and nested fields, since {@link #fieldMappers()} is the flattened set
     * of all field mappers by full path. Metadata fields are exempt (reconstructed by their own machinery), as are
     * multi-fields and the internal sub-fields of an {@link InferenceFieldMapper} (e.g. a {@code semantic_text} field's
     * chunk embeddings and offsets) - none of these appear in {@code _source}.
     */
    @Nullable
    public String firstFieldNotReconstructableFromDocValues() {
        for (Mapper mapper : fieldMappers()) {
            if (mapper instanceof FieldMapper fieldMapper
                && mapper instanceof MetadataFieldMapper == false
                && isMultiField(fieldMapper.fullPath()) == false
                && isInferenceFieldInternal(fieldMapper.fullPath()) == false
                && fieldMapper.syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK) {
                return fieldMapper.fullPath();
            }
        }
        return null;
    }

    /**
     * Whether the field is an internal sub-field of an {@link InferenceFieldMapper} (it lives under an inference field's path).
     * Such fields are not part of {@code _source}; the inference field reconstructs them into {@code _inference_fields} itself.
     */
    private boolean isInferenceFieldInternal(String fieldPath) {
        for (String inferenceFieldPath : inferenceFields.keySet()) {
            if (fieldPath.length() > inferenceFieldPath.length()
                && fieldPath.startsWith(inferenceFieldPath)
                && fieldPath.charAt(inferenceFieldPath.length()) == '.') {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the field mappers marked as time-series dimensions, keyed by full path in insertion order.
     */
    public Map<String, FieldMapper> dimensionFieldMappers() {
        return dimensionFieldMappers;
    }

    /**
     * Returns the field mappers that carry a time-series metric type, keyed by full path in insertion order.
     */
    public Map<String, FieldMapper> metricFieldMappers() {
        return metricFieldMappers;
    }

    void checkLimits(IndexSettings settings) {
        checkFieldLimit(settings.getMappingTotalFieldsLimit());
        checkObjectDepthLimit(settings.getMappingDepthLimit());
        checkFieldNameLengthLimit(settings.getMappingFieldNameLengthLimit());
        checkNestedFieldsLimit(settings.getMappingNestedFieldsLimit());
        checkNestedParentsLimit(settings.getMappingNestedParentsLimit());
        checkDimensionFieldLimit(settings.getMappingDimensionFieldsLimit());
    }

    private void checkFieldLimit(long limit) {
        checkFieldLimit(limit, 0);
    }

    void checkFieldLimit(long limit, int additionalFieldsToAdd) {
        if (exceedsLimit(limit, additionalFieldsToAdd)) {
            throw new IllegalArgumentException(
                "Limit of total fields ["
                    + limit
                    + "] has been exceeded"
                    + (additionalFieldsToAdd > 0 ? " while adding new fields [" + additionalFieldsToAdd + "]" : "")
            );
        }
    }

    boolean exceedsLimit(long limit, int additionalFieldsToAdd) {
        return remainingFieldsUntilLimit(limit) < additionalFieldsToAdd;
    }

    long remainingFieldsUntilLimit(long mappingTotalFieldsLimit) {
        return mappingTotalFieldsLimit - totalFieldsCount;
    }

    private void checkDimensionFieldLimit(long limit) {
        if (dimensionFieldMappers.size() > limit) {
            throw new IllegalArgumentException("Limit of total dimension fields [" + limit + "] has been exceeded");
        }
    }

    private void checkObjectDepthLimit(long limit) {
        for (String objectPath : objectMappers.keySet()) {
            checkObjectDepthLimit(limit, objectPath);
        }
    }

    static void checkObjectDepthLimit(long limit, String objectPath) {
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

    void checkFieldNameLengthLimit(long limit) {
        validateMapperNameIn(objectMappers.values(), limit);
        validateMapperNameIn(fieldMappers.values(), limit);
    }

    private static void validateMapperNameIn(Collection<? extends Mapper> mappers, long limit) {
        for (Mapper mapper : mappers) {
            String name = mapper.leafName();
            if (name.length() > limit) {
                throw new IllegalArgumentException("Field name [" + name + "] is longer than the limit of [" + limit + "] characters");
            }
        }
    }

    private void checkNestedFieldsLimit(long limit) {
        if (nestedLookup.getNestedMappers().size() > limit) {
            throw new IllegalArgumentException("Limit of nested fields [" + limit + "] has been exceeded");
        }
    }

    private void checkNestedParentsLimit(long limit) {
        if (nestedLookup.getNestedMappers().size() < limit) {
            return;
        }

        Set<Query> nestedPaths = new HashSet<>();
        for (var nested : nestedLookup.getNestedMappers().values()) {
            nestedPaths.add(nested.parentTypeFilter());
        }
        if (nestedPaths.size() > limit) {
            throw new IllegalArgumentException("Limit of nested parents [" + limit + "] has been exceeded");
        }
    }

    public Map<String, ObjectMapper> objectMappers() {
        return objectMappers;
    }

    /**
     * Returns a map containing all fields that require to run inference (through the {@link InferenceService} prior to indexation.
     */
    public Map<String, InferenceFieldMetadata> inferenceFields() {
        return inferenceFields;
    }

    public Set<String> syntheticVectorFields() {
        return syntheticVectorFields;
    }

    public NestedLookup nestedLookup() {
        return nestedLookup;
    }

    /**
     * Whether the mapping has any {@code [nullability=false]} fields. When {@code false}, enforcement is skipped entirely.
     */
    public boolean hasRequiredFields() {
        return requiredFieldsByNestedParent.isEmpty() == false;
    }

    /**
     * The {@code [nullability=false]} field paths whose nearest nested parent is {@code nestedParent} ({@code ""} for the root document);
     * the set of fields a Lucene doc in that partition must carry a non-null value for. Returns an empty set when none are required there.
     */
    public Set<String> requiredFields(String nestedParent) {
        return requiredFieldsByNestedParent.getOrDefault(nestedParent, Set.of());
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
     * @return A map from field name to the MappedFieldType
     */
    public Map<String, MappedFieldType> getFullNameToFieldType() {
        return fieldTypeLookup.getFullNameToFieldType();
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
     * If field is a leaf multi-field return the path to the parent field. Otherwise, return null.
     */
    public String parentField(String field) {
        return fieldTypesLookup().parentField(field);
    }

    /**
     * Returns true if the index has mappings. An index does not have mappings only if it was created
     * without providing mappings explicitly, and no documents have yet been indexed in it.
     * @return true if the current index has mappings, false otherwise
     */
    public boolean hasMappings() {
        return this != EMPTY;
    }

    /**
     * Will there be {@code _source}.
     */
    public boolean isSourceEnabled() {
        return isSourceEnabled;
    }

    /**
     * Does the source need to be rebuilt on the fly?
     */
    public boolean isSourceSynthetic() {
        return isSourceSynthetic;
    }

    public boolean isSourceColumnarStored() {
        return isSourceColumnarStored;
    }

    public boolean isColumnarId() {
        return isColumnarId;
    }

    /**
     * Build something to load source {@code _source}.
     */
    public SourceLoader newSourceLoader(@Nullable SourceFilter filter, SourceFieldMetrics metrics) {
        if (isSourceSynthetic() || isSourceColumnarStored()) {
            return new SourceLoader.Synthetic(
                filter,
                () -> mapping.syntheticFieldLoader(filter, isSourceColumnarStored()),
                metrics,
                mapping.ignoredSourceFormat()
            );
        }
        var syntheticVectorsLoader = mapping.syntheticVectorsLoader(filter);
        if (syntheticVectorsLoader != null) {
            return new SourceLoader.SyntheticVectors(removeExcludedSyntheticVectorFields(filter), syntheticVectorsLoader);
        }
        return filter == null ? SourceLoader.FROM_STORED_SOURCE : new SourceLoader.Stored(filter);
    }

    private SourceFilter removeExcludedSyntheticVectorFields(@Nullable SourceFilter filter) {
        if (filter == null || filter.getExcludes().length == 0) {
            return filter;
        }
        List<String> newExcludes = new ArrayList<>();
        for (var exclude : filter.getExcludes()) {
            if (syntheticVectorFields().contains(exclude) == false) {
                newExcludes.add(exclude);
            }
        }
        if (newExcludes.isEmpty() && filter.getIncludes().length == 0) {
            return null;
        }
        return new SourceFilter(filter.getIncludes(), newExcludes.toArray(String[]::new));
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
     * If this mapping contains a timestamp field that is of type date, has doc values, and is either indexed or uses a doc values
     * skipper, this returns the field type for it.
     */
    public @Nullable DateFieldType getTimestampFieldType() {
        final MappedFieldType mappedFieldType = fieldTypesLookup().get(DataStream.TIMESTAMP_FIELD_NAME);
        if (mappedFieldType instanceof DateFieldType dateMappedFieldType) {
            IndexType indexType = dateMappedFieldType.indexType();
            if (indexType.hasPoints() || indexType.hasDocValuesSkipper()) {
                return dateMappedFieldType;
            }
        }
        return null;
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
        if (indexMode.isTsdb()) {
            if (shadowed.isDimension()) {
                throw new MapperParsingException("Field [" + name + "] attempted to shadow a time_series_dimension");
            }
            if (shadowed.getMetricType() != null) {
                throw new MapperParsingException("Field [" + name + "] attempted to shadow a time_series_metric");
            }
        }
    }
}
