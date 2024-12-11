/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

public class ObjectMapper extends Mapper {
    private static final Logger logger = LogManager.getLogger(ObjectMapper.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ObjectMapper.class);
    public static final FeatureFlag SUB_OBJECTS_AUTO_FEATURE_FLAG = new FeatureFlag("sub_objects_auto");

    public static final String CONTENT_TYPE = "object";
    static final String STORE_ARRAY_SOURCE_PARAM = "store_array_source";
    static final NodeFeature SUBOBJECTS_AUTO = new NodeFeature("mapper.subobjects_auto");
    // No-op. All uses of this feature were reverted but node features can't be removed.
    static final NodeFeature SUBOBJECTS_AUTO_FIXES = new NodeFeature("mapper.subobjects_auto_fixes");

    /**
     * Enhances the previously boolean option for subobjects support with an intermediate mode `auto` that uses
     * any objects that are present in the mappings and flattens any fields defined outside the predefined objects.
     */
    public enum Subobjects {
        ENABLED(Boolean.TRUE),
        DISABLED(Boolean.FALSE),
        AUTO("auto");

        private final Object printedValue;

        Subobjects(Object printedValue) {
            this.printedValue = printedValue;
        }

        public static Subobjects from(Object node) {
            if (node instanceof Boolean value) {
                return value ? Subobjects.ENABLED : Subobjects.DISABLED;
            }
            if (node instanceof String value) {
                if (value.equalsIgnoreCase("true")) {
                    return ENABLED;
                }
                if (value.equalsIgnoreCase("false")) {
                    return DISABLED;
                }
                if (SUB_OBJECTS_AUTO_FEATURE_FLAG.isEnabled() && value.equalsIgnoreCase("auto")) {
                    return AUTO;
                }
            }
            throw new ElasticsearchParseException("unknown subobjects value: " + node);
        }

        @Override
        public String toString() {
            return printedValue.toString();
        }
    }

    public static class Defaults {
        public static final boolean ENABLED = true;
        public static final Optional<Subobjects> SUBOBJECTS = Optional.empty();
        public static final Explicit<Boolean> STORE_ARRAY_SOURCE = Explicit.IMPLICIT_FALSE;
        public static final Dynamic DYNAMIC = Dynamic.TRUE;
    }

    public enum Dynamic {
        TRUE {
            @Override
            DynamicFieldsBuilder getDynamicFieldsBuilder() {
                return DynamicFieldsBuilder.DYNAMIC_TRUE;
            }
        },
        FALSE,
        STRICT,
        RUNTIME {
            @Override
            DynamicFieldsBuilder getDynamicFieldsBuilder() {
                return DynamicFieldsBuilder.DYNAMIC_RUNTIME;
            }
        };

        DynamicFieldsBuilder getDynamicFieldsBuilder() {
            throw new UnsupportedOperationException("Cannot create dynamic fields when dynamic is set to [" + this + "]");
        }

        /**
         * Get the root-level dynamic setting for a Mapping
         *
         * If no dynamic settings are explicitly configured, we default to {@link #TRUE}
         */
        static Dynamic getRootDynamic(MappingLookup mappingLookup) {
            Dynamic rootDynamic = mappingLookup.getMapping().getRoot().dynamic;
            return rootDynamic == null ? Defaults.DYNAMIC : rootDynamic;
        }
    }

    public static class Builder extends Mapper.Builder {
        protected Optional<Subobjects> subobjects;
        protected Explicit<Boolean> enabled = Explicit.IMPLICIT_TRUE;
        protected Optional<SourceKeepMode> sourceKeepMode = Optional.empty();
        protected Dynamic dynamic;
        protected final List<Mapper.Builder> mappersBuilders = new ArrayList<>();

        public Builder(String name, Optional<Subobjects> subobjects) {
            super(name);
            this.subobjects = subobjects;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = Explicit.explicitBoolean(enabled);
            return this;
        }

        public Builder sourceKeepMode(SourceKeepMode sourceKeepMode) {
            this.sourceKeepMode = Optional.of(sourceKeepMode);
            return this;
        }

        public Builder dynamic(Dynamic dynamic) {
            this.dynamic = dynamic;
            return this;
        }

        public Builder add(Mapper.Builder builder) {
            mappersBuilders.add(builder);
            return this;
        }

        private void add(String name, Mapper mapper) {
            add(new Mapper.Builder(name) {
                @Override
                public Mapper build(MapperBuilderContext context) {
                    return mapper;
                }
            });
        }

        /**
         * Adds a dynamically created {@link Mapper} to this builder.
         *
         * @param name      the name of the Mapper, including object prefixes
         * @param prefix    the object prefix of this mapper
         * @param mapper    the mapper to add
         * @param context   the DocumentParserContext in which the mapper has been built
         */
        public final void addDynamic(String name, String prefix, Mapper mapper, DocumentParserContext context) {
            // If the mapper to add has no dots, or the current object mapper has subobjects set to false,
            // we just add it as it is for sure a leaf mapper
            if (name.contains(".") == false || (subobjects.isPresent() && (subobjects.get() == Subobjects.DISABLED))) {
                add(name, mapper);
            } else {
                // We strip off the first object path of the mapper name, load or create
                // the relevant object mapper, and then recurse down into it, passing the remainder
                // of the mapper name. So for a mapper 'foo.bar.baz', we locate 'foo' and then
                // call addDynamic on it with the name 'bar.baz', and next call addDynamic on 'bar' with the name 'baz'.
                int firstDotIndex = name.indexOf('.');
                String immediateChild = name.substring(0, firstDotIndex);
                String immediateChildFullName = prefix == null ? immediateChild : prefix + "." + immediateChild;
                Builder parentBuilder = findObjectBuilder(immediateChildFullName, context);
                if (parentBuilder != null) {
                    parentBuilder.addDynamic(name.substring(firstDotIndex + 1), immediateChildFullName, mapper, context);
                    add(parentBuilder);
                } else if (subobjects.isPresent() && subobjects.get() == Subobjects.AUTO) {
                    // No matching parent object was found, the mapper is added as a leaf - similar to subobjects false.
                    add(name, mapper);
                } else {
                    // Expected to find a matching parent object but got null.
                    throw new IllegalStateException("Missing intermediate object " + immediateChildFullName);
                }
            }
        }

        private static Builder findObjectBuilder(String fullName, DocumentParserContext context) {
            // does the object mapper already exist? if so, use that
            ObjectMapper objectMapper = context.mappingLookup().objectMappers().get(fullName);
            if (objectMapper != null) {
                return objectMapper.newBuilder(context.indexSettings().getIndexVersionCreated());
            }
            // has the object mapper been added as a dynamic update already?
            objectMapper = context.getDynamicObjectMapper(fullName);
            if (objectMapper != null) {
                return objectMapper.newBuilder(context.indexSettings().getIndexVersionCreated());
            }
            // no object mapper found
            return null;
        }

        protected final Map<String, Mapper> buildMappers(MapperBuilderContext mapperBuilderContext) {
            Map<String, Mapper> mappers = new HashMap<>();
            for (Mapper.Builder builder : mappersBuilders) {
                Mapper mapper = builder.build(mapperBuilderContext);
                Mapper existing = mappers.get(mapper.leafName());
                if (existing != null) {
                    // The same mappings or document may hold the same field twice, either because duplicated JSON keys are allowed or
                    // the same field is provided using the object notation as well as the dot notation at the same time.
                    // This can also happen due to multiple index templates being merged into a single mappings definition using
                    // XContentHelper#mergeDefaults, again in case some index templates contained mappings for the same field using a
                    // mix of object notation and dot notation.
                    mapper = existing.merge(mapper, MapperMergeContext.from(mapperBuilderContext, Long.MAX_VALUE));
                }
                if (subobjects.isPresent() && subobjects.get() == Subobjects.DISABLED && mapper instanceof ObjectMapper objectMapper) {
                    // We're parsing a mapping that has set `subobjects: false` but has defined sub-objects
                    objectMapper.asFlattenedFieldMappers(mapperBuilderContext).forEach(m -> mappers.put(m.leafName(), m));
                } else {
                    mappers.put(mapper.leafName(), mapper);
                }
            }
            return mappers;
        }

        @Override
        public ObjectMapper build(MapperBuilderContext context) {
            return new ObjectMapper(
                leafName(),
                context.buildFullName(leafName()),
                enabled,
                subobjects,
                sourceKeepMode,
                dynamic,
                buildMappers(context.createChildContext(leafName(), dynamic))
            );
        }
    }

    @Override
    public int getTotalFieldsCount() {
        return 1 + mappers.values().stream().mapToInt(Mapper::getTotalFieldsCount).sum();
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public boolean supportsVersion(IndexVersion indexCreatedVersion) {
            return true;
        }

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            parserContext.incrementMappingObjectDepth(); // throws MapperParsingException if depth limit is exceeded
            Optional<Subobjects> subobjects = parseSubobjects(node);
            Builder builder = new Builder(name, subobjects);
            parseObjectFields(node, parserContext, builder);
            parserContext.decrementMappingObjectDepth();
            return builder;
        }

        static void parseObjectFields(Map<String, Object> node, MappingParserContext parserContext, Builder builder) {
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)) {
                    iterator.remove();
                }
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        protected static boolean parseObjectOrDocumentTypeProperties(
            String fieldName,
            Object fieldNode,
            MappingParserContext parserContext,
            Builder builder
        ) {
            if (fieldName.equals("dynamic")) {
                String value = fieldNode.toString();
                if (value.equalsIgnoreCase("strict")) {
                    builder.dynamic(Dynamic.STRICT);
                } else if (value.equalsIgnoreCase("runtime")) {
                    builder.dynamic(Dynamic.RUNTIME);
                } else {
                    boolean dynamic = XContentMapValues.nodeBooleanValue(fieldNode, fieldName + ".dynamic");
                    builder.dynamic(dynamic ? Dynamic.TRUE : Dynamic.FALSE);
                }
                return true;
            } else if (fieldName.equals("enabled")) {
                builder.enabled(XContentMapValues.nodeBooleanValue(fieldNode, fieldName + ".enabled"));
                return true;
            } else if (fieldName.equals(STORE_ARRAY_SOURCE_PARAM)) {
                builder.sourceKeepMode(SourceKeepMode.ARRAYS);
                return true;
            } else if (fieldName.equals(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM)) {
                builder.sourceKeepMode(SourceKeepMode.from(fieldNode.toString()));
                return true;
            } else if (fieldName.equals("properties")) {
                if (fieldNode instanceof Collection && ((Collection) fieldNode).isEmpty()) {
                    // nothing to do here, empty (to support "properties: []" case)
                } else if ((fieldNode instanceof Map) == false) {
                    throw new ElasticsearchParseException("properties must be a map type");
                } else {
                    parseProperties(builder, (Map<String, Object>) fieldNode, parserContext);
                }
                return true;
            } else if (fieldName.equals("include_in_all")) {
                deprecationLogger.warn(
                    DeprecationCategory.MAPPINGS,
                    "include_in_all",
                    "[include_in_all] is deprecated, the _all field have been removed in this version"
                );
                return true;
            }
            return false;
        }

        protected static Optional<Subobjects> parseSubobjects(Map<String, Object> node) {
            Object subobjectsNode = node.remove("subobjects");
            if (subobjectsNode != null) {
                return Optional.of(Subobjects.from(subobjectsNode));
            }
            return Defaults.SUBOBJECTS;
        }

        protected static void parseProperties(Builder objBuilder, Map<String, Object> propsNode, MappingParserContext parserContext) {
            Iterator<Map.Entry<String, Object>> iterator = propsNode.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                validateFieldName(fieldName, parserContext.indexVersionCreated());
                // Should accept empty arrays, as a work around for when the
                // user can't provide an empty Map. (PHP for example)
                boolean isEmptyList = entry.getValue() instanceof List && ((List<?>) entry.getValue()).isEmpty();

                if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> propNode = (Map<String, Object>) entry.getValue();
                    String type;
                    Object typeNode = propNode.get("type");
                    if (typeNode != null) {
                        type = typeNode.toString();
                    } else {
                        // lets see if we can derive this...
                        if (propNode.get("properties") != null) {
                            type = ObjectMapper.CONTENT_TYPE;
                        } else if (propNode.size() == 1 && propNode.get("enabled") != null) {
                            // if there is a single property with the enabled
                            // flag on it, make it an object
                            // (usually, setting enabled to false to not index
                            // any type, including core values, which
                            type = ObjectMapper.CONTENT_TYPE;
                        } else {
                            throw new MapperParsingException("No type specified for field [" + fieldName + "]");
                        }
                    }

                    if (objBuilder.subobjects.isPresent()
                        && objBuilder.subobjects.get() == Subobjects.DISABLED
                        && type.equals(NestedObjectMapper.CONTENT_TYPE)) {
                        throw new MapperParsingException(
                            "Tried to add nested object ["
                                + fieldName
                                + "] to object ["
                                + objBuilder.leafName()
                                + "] which does not support subobjects"
                        );
                    }
                    Mapper.TypeParser typeParser = parserContext.typeParser(type);
                    if (typeParser == null) {
                        throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + fieldName + "]");
                    }
                    Mapper.Builder fieldBuilder;
                    if (objBuilder.subobjects.isPresent() && objBuilder.subobjects.get() != Subobjects.ENABLED) {
                        fieldBuilder = typeParser.parse(fieldName, propNode, parserContext);
                    } else {
                        String[] fieldNameParts = fieldName.split("\\.");
                        if (fieldNameParts.length == 0) {
                            throw new IllegalArgumentException("field name cannot contain only dots");
                        }
                        String realFieldName = fieldNameParts[fieldNameParts.length - 1];
                        validateFieldName(realFieldName, parserContext.indexVersionCreated());
                        fieldBuilder = typeParser.parse(realFieldName, propNode, parserContext);
                        for (int i = fieldNameParts.length - 2; i >= 0; --i) {
                            String intermediateObjectName = fieldNameParts[i];
                            validateFieldName(intermediateObjectName, parserContext.indexVersionCreated());
                            Builder intermediate = new Builder(intermediateObjectName, Defaults.SUBOBJECTS);
                            intermediate.add(fieldBuilder);
                            fieldBuilder = intermediate;
                        }
                    }
                    objBuilder.add(fieldBuilder);
                    propNode.remove("type");
                    MappingParser.checkNoRemainingFields(fieldName, propNode);
                    iterator.remove();
                } else if (isEmptyList) {
                    iterator.remove();
                } else {
                    throw new MapperParsingException(
                        "Expected map for property [fields] on field [" + fieldName + "] but got a " + fieldName.getClass()
                    );
                }
            }

            MappingParser.checkNoRemainingFields(propsNode, "DocType mapping definition has unsupported parameters: ");
        }
    }

    private static void validateFieldName(String fieldName, IndexVersion indexCreatedVersion) {
        if (fieldName.isEmpty()) {
            throw new IllegalArgumentException("field name cannot be an empty string");
        }
        if (fieldName.isBlank() & indexCreatedVersion.onOrAfter(IndexVersions.V_8_6_0)) {
            // blank field names were previously accepted in mappings, but not in documents.
            throw new IllegalArgumentException("field name cannot contain only whitespaces");
        }
    }

    private final String fullPath;

    protected final Explicit<Boolean> enabled;
    protected final Optional<Subobjects> subobjects;
    protected final Optional<SourceKeepMode> sourceKeepMode;
    protected final Dynamic dynamic;

    protected final Map<String, Mapper> mappers;

    ObjectMapper(
        String name,
        String fullPath,
        Explicit<Boolean> enabled,
        Optional<Subobjects> subobjects,
        Optional<SourceKeepMode> sourceKeepMode,
        Dynamic dynamic,
        Map<String, Mapper> mappers
    ) {
        super(name);
        // could be blank but not empty on indices created < 8.6.0
        assert name.isEmpty() == false;
        this.fullPath = internFieldName(fullPath);
        this.enabled = enabled;
        this.subobjects = subobjects;
        this.sourceKeepMode = sourceKeepMode;
        this.dynamic = dynamic;
        if (mappers == null) {
            this.mappers = Map.of();
        } else {
            this.mappers = Map.copyOf(mappers);
        }
        assert subobjects.isEmpty()
            || subobjects.get() != Subobjects.DISABLED
            || this.mappers.values().stream().noneMatch(m -> m instanceof ObjectMapper)
            : "When subobjects is false, mappers must not contain an ObjectMapper";
    }

    /**
     * @return a Builder that will produce an empty ObjectMapper with the same configuration as this one
     */
    public Builder newBuilder(IndexVersion indexVersionCreated) {
        Builder builder = new Builder(leafName(), subobjects);
        builder.enabled = this.enabled;
        builder.dynamic = this.dynamic;
        return builder;
    }

    /**
     * Returns a copy of this object mapper that doesn't have any fields and runtime fields.
     * This is typically used in the context of a mapper merge when there's not enough budget to add the entire object.
     */
    ObjectMapper withoutMappers() {
        return new ObjectMapper(leafName(), fullPath, enabled, subobjects, sourceKeepMode, dynamic, Map.of());
    }

    @Override
    public String fullPath() {
        return this.fullPath;
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    public boolean isEnabled() {
        return this.enabled.value();
    }

    public boolean isNested() {
        return false;
    }

    public Mapper getMapper(String field) {
        return mappers.get(field);
    }

    @Override
    public Iterator<Mapper> iterator() {
        return mappers.values().iterator();
    }

    public final Dynamic dynamic() {
        return dynamic;
    }

    public final Subobjects subobjects() {
        return subobjects.orElse(Subobjects.ENABLED);
    }

    public final Optional<SourceKeepMode> sourceKeepMode() {
        return sourceKeepMode;
    }

    @Override
    public void validate(MappingLookup mappers) {
        for (Mapper mapper : this.mappers.values()) {
            mapper.validate(mappers);
        }
    }

    protected MapperMergeContext createChildContext(MapperMergeContext mapperMergeContext, String name) {
        return mapperMergeContext.createChildContext(name, dynamic);
    }

    @Override
    public ObjectMapper merge(Mapper mergeWith, MapperMergeContext parentMergeContext) {
        if (mergeWith instanceof ObjectMapper == false) {
            MapperErrors.throwObjectMappingConflictError(mergeWith.fullPath());
        }
        if (this instanceof NestedObjectMapper == false && mergeWith instanceof NestedObjectMapper) {
            // TODO stop NestedObjectMapper extending ObjectMapper?
            MapperErrors.throwNestedMappingConflictError(mergeWith.fullPath());
        }
        var mergeResult = MergeResult.build(this, (ObjectMapper) mergeWith, parentMergeContext);
        return new ObjectMapper(
            leafName(),
            fullPath,
            mergeResult.enabled,
            mergeResult.subObjects,
            mergeResult.sourceKeepMode,
            mergeResult.dynamic,
            mergeResult.mappers
        );
    }

    protected record MergeResult(
        Explicit<Boolean> enabled,
        Optional<Subobjects> subObjects,
        Optional<SourceKeepMode> sourceKeepMode,
        Dynamic dynamic,
        Map<String, Mapper> mappers
    ) {
        static MergeResult build(ObjectMapper existing, ObjectMapper mergeWithObject, MapperMergeContext parentMergeContext) {
            final Explicit<Boolean> enabled;
            final MergeReason reason = parentMergeContext.getMapperBuilderContext().getMergeReason();
            if (mergeWithObject.enabled.explicit()) {
                if (reason == MergeReason.INDEX_TEMPLATE) {
                    enabled = mergeWithObject.enabled;
                } else if (existing.isEnabled() != mergeWithObject.isEnabled()) {
                    throw new MapperException(
                        "the [enabled] parameter can't be updated for the object mapping [" + existing.fullPath() + "]"
                    );
                } else {
                    enabled = existing.enabled;
                }
            } else {
                enabled = existing.enabled;
            }
            final Optional<Subobjects> subObjects;
            if (mergeWithObject.subobjects.isPresent()) {
                if (reason == MergeReason.INDEX_TEMPLATE) {
                    subObjects = mergeWithObject.subobjects;
                } else if (existing.subobjects() != mergeWithObject.subobjects()) {
                    throw new MapperException(
                        "the [subobjects] parameter can't be updated for the object mapping [" + existing.fullPath() + "]"
                    );
                } else {
                    subObjects = existing.subobjects;
                }
            } else {
                subObjects = existing.subobjects;
            }
            final Optional<SourceKeepMode> sourceKeepMode;
            if (mergeWithObject.sourceKeepMode.isPresent()) {
                if (reason == MergeReason.INDEX_TEMPLATE) {
                    sourceKeepMode = mergeWithObject.sourceKeepMode;
                } else if (existing.sourceKeepMode.isEmpty() || existing.sourceKeepMode.get() != mergeWithObject.sourceKeepMode.get()) {
                    throw new MapperException(
                        "the [ "
                            + Mapper.SYNTHETIC_SOURCE_KEEP_PARAM
                            + " ] parameter can't be updated for the object mapping ["
                            + existing.fullPath()
                            + "]"
                    );
                } else {
                    sourceKeepMode = existing.sourceKeepMode;
                }
            } else {
                sourceKeepMode = existing.sourceKeepMode;
            }

            MapperMergeContext objectMergeContext = existing.createChildContext(parentMergeContext, existing.leafName());
            Map<String, Mapper> mergedMappers = buildMergedMappers(existing, mergeWithObject, objectMergeContext, subObjects);
            return new MergeResult(
                enabled,
                subObjects,
                sourceKeepMode,
                mergeWithObject.dynamic != null ? mergeWithObject.dynamic : existing.dynamic,
                mergedMappers
            );
        }

        private static Map<String, Mapper> buildMergedMappers(
            ObjectMapper existing,
            ObjectMapper mergeWithObject,
            MapperMergeContext objectMergeContext,
            Optional<Subobjects> subobjects
        ) {
            Map<String, Mapper> mergedMappers = new HashMap<>();
            for (Mapper childOfExistingMapper : existing.mappers.values()) {
                if (subobjects.isPresent()
                    && subobjects.get() == Subobjects.DISABLED
                    && childOfExistingMapper instanceof ObjectMapper objectMapper) {
                    // An existing mapping with sub-objects is merged with a mapping that has set `subobjects: false`
                    objectMapper.asFlattenedFieldMappers(objectMergeContext.getMapperBuilderContext())
                        .forEach(m -> mergedMappers.put(m.leafName(), m));
                } else {
                    putMergedMapper(mergedMappers, childOfExistingMapper);
                }
            }
            for (Mapper mergeWithMapper : mergeWithObject) {
                Mapper mergeIntoMapper = mergedMappers.get(mergeWithMapper.leafName());
                if (mergeIntoMapper == null) {
                    if (subobjects.isPresent()
                        && subobjects.get() == Subobjects.DISABLED
                        && mergeWithMapper instanceof ObjectMapper objectMapper) {
                        // An existing mapping that has set `subobjects: false` is merged with a mapping with sub-objects
                        objectMapper.asFlattenedFieldMappers(objectMergeContext.getMapperBuilderContext())
                            .stream()
                            .filter(m -> objectMergeContext.decrementFieldBudgetIfPossible(m.getTotalFieldsCount()))
                            .forEach(m -> putMergedMapper(mergedMappers, m));
                    } else if (objectMergeContext.decrementFieldBudgetIfPossible(mergeWithMapper.getTotalFieldsCount())) {
                        putMergedMapper(mergedMappers, mergeWithMapper);
                    } else if (mergeWithMapper instanceof ObjectMapper om) {
                        putMergedMapper(mergedMappers, truncateObjectMapper(objectMergeContext, om));
                    }
                } else if (mergeIntoMapper instanceof ObjectMapper objectMapper) {
                    assert subobjects.isEmpty() || subobjects.get() != Subobjects.DISABLED
                        : "existing object mappers are supposed to be flattened if subobjects is false";
                    putMergedMapper(mergedMappers, objectMapper.merge(mergeWithMapper, objectMergeContext));
                } else {
                    assert mergeIntoMapper instanceof FieldMapper || mergeIntoMapper instanceof FieldAliasMapper;
                    if (mergeWithMapper instanceof NestedObjectMapper) {
                        MapperErrors.throwNestedMappingConflictError(mergeWithMapper.fullPath());
                    } else if (mergeWithMapper instanceof ObjectMapper) {
                        MapperErrors.throwObjectMappingConflictError(mergeWithMapper.fullPath());
                    }

                    // If we're merging template mappings when creating an index, then a field definition always
                    // replaces an existing one.
                    if (objectMergeContext.getMapperBuilderContext().getMergeReason() == MergeReason.INDEX_TEMPLATE) {
                        putMergedMapper(mergedMappers, mergeWithMapper);
                    } else if (isConflictingDynamicMapping(objectMergeContext, mergeWithMapper, mergeIntoMapper)) {
                        logger.trace(
                            "ignoring conflicting dynamic mapping update for field={} current_type={} new_type={}",
                            mergeIntoMapper.fullPath(),
                            mergeIntoMapper.typeName(),
                            mergeWithMapper.typeName()
                        );
                    } else {
                        putMergedMapper(mergedMappers, mergeIntoMapper.merge(mergeWithMapper, objectMergeContext));
                    }
                }
            }
            return Map.copyOf(mergedMappers);
        }

        /*
         * We're ignoring the field if a dynamic mapping update tries to define a conflicting field type.
         * This is caused by another index request with a different value racing to update the mappings.
         * After updating the mappings, the index request will be re-tried and sees the updated mappings for this field.
         * The updated mappings will then be taken into account when parsing the document
         * (for example by coercing the value, ignore_malformed values, or failing the index request due to a type conflict).
         */
        private static boolean isConflictingDynamicMapping(
            MapperMergeContext objectMergeContext,
            Mapper mergeWithMapper,
            Mapper mergeIntoMapper
        ) {
            return objectMergeContext.getMapperBuilderContext().getMergeReason().isAutoUpdate()
                && mergeIntoMapper.typeName().equals(mergeWithMapper.typeName()) == false;
        }

        private static void putMergedMapper(Map<String, Mapper> mergedMappers, @Nullable Mapper merged) {
            if (merged != null) {
                mergedMappers.put(merged.leafName(), merged);
            }
        }

        private static ObjectMapper truncateObjectMapper(MapperMergeContext context, ObjectMapper objectMapper) {
            // there's not enough capacity for the whole object mapper,
            // so we're just trying to add the shallow object, without it's sub-fields
            ObjectMapper shallowObjectMapper = objectMapper.withoutMappers();
            if (context.decrementFieldBudgetIfPossible(shallowObjectMapper.getTotalFieldsCount())) {
                // now trying to add the sub-fields one by one via a merge, until we hit the limit
                return shallowObjectMapper.merge(objectMapper, context);
            }
            return null;
        }
    }

    /**
     * Returns all FieldMappers this ObjectMapper or its children hold.
     * The name of the FieldMappers will be updated to reflect the hierarchy.
     *
     * @throws IllegalArgumentException if the mapper cannot be flattened
     */
    List<FieldMapper> asFlattenedFieldMappers(MapperBuilderContext context) {
        List<FieldMapper> flattenedMappers = new ArrayList<>();
        ContentPath path = new ContentPath();
        asFlattenedFieldMappers(context, flattenedMappers, path);
        return flattenedMappers;
    }

    private void asFlattenedFieldMappers(MapperBuilderContext context, List<FieldMapper> flattenedMappers, ContentPath path) {
        ensureFlattenable(context, path);
        path.add(leafName());
        for (Mapper mapper : mappers.values()) {
            if (mapper instanceof FieldMapper fieldMapper) {
                FieldMapper.Builder fieldBuilder = fieldMapper.getMergeBuilder();
                fieldBuilder.setLeafName(path.pathAsText(mapper.leafName()));
                flattenedMappers.add(fieldBuilder.build(context));
            } else if (mapper instanceof ObjectMapper objectMapper) {
                objectMapper.asFlattenedFieldMappers(context, flattenedMappers, path);
            }
        }
        path.remove();
    }

    private void ensureFlattenable(MapperBuilderContext context, ContentPath path) {
        if (dynamic != null && context.getDynamic() != dynamic) {
            throwAutoFlatteningException(
                path,
                "the value of [dynamic] ("
                    + dynamic
                    + ") is not compatible with the value from its parent context ("
                    + context.getDynamic()
                    + ")"
            );
        }
        if (sourceKeepMode.isPresent()) {
            throwAutoFlatteningException(
                path,
                "the value of [" + Mapper.SYNTHETIC_SOURCE_KEEP_PARAM + "] is [ " + sourceKeepMode.get() + " ]"
            );
        }
        if (isEnabled() == false) {
            throwAutoFlatteningException(path, "the value of [enabled] is [false]");
        }
        if (subobjects.isPresent() && subobjects.get() == Subobjects.ENABLED) {
            throwAutoFlatteningException(path, "the value of [subobjects] is [true]");
        }
    }

    private void throwAutoFlatteningException(ContentPath path, String reason) {
        throw new IllegalArgumentException(
            "Object mapper ["
                + path.pathAsText(leafName())
                + "] was found in a context where subobjects is set to false. "
                + "Auto-flattening ["
                + path.pathAsText(leafName())
                + "] failed because "
                + reason
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        toXContent(builder, params, null);
        return builder;
    }

    void toXContent(XContentBuilder builder, Params params, ToXContent custom) throws IOException {
        builder.startObject(leafName());
        if (mappers.isEmpty() && custom == null) {
            // only write the object content type if there are no properties, otherwise, it is automatically detected
            builder.field("type", CONTENT_TYPE);
        }
        if (dynamic != null) {
            builder.field("dynamic", dynamic.name().toLowerCase(Locale.ROOT));
        }
        if (isEnabled() != Defaults.ENABLED) {
            builder.field("enabled", enabled.value());
        }
        if (subobjects.isPresent()) {
            builder.field("subobjects", subobjects.get().printedValue);
        }
        if (sourceKeepMode.isPresent()) {
            builder.field("synthetic_source_keep", sourceKeepMode.get());
        }
        if (custom != null) {
            custom.toXContent(builder, params);
        }

        doXContent(builder, params);
        serializeMappers(builder, params);
        builder.endObject();
    }

    protected void serializeMappers(XContentBuilder builder, Params params) throws IOException {
        // sort the mappers so we get consistent serialization format
        Mapper[] sortedMappers = mappers.values().toArray(Mapper[]::new);
        Arrays.sort(sortedMappers, Comparator.comparing(Mapper::fullPath));

        int count = 0;
        for (Mapper mapper : sortedMappers) {
            if ((mapper instanceof MetadataFieldMapper) == false) {
                if (count++ == 0) {
                    builder.startObject("properties");
                }
                mapper.toXContent(builder, params);
            }
        }
        if (count > 0) {
            builder.endObject();
        }
    }

    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }

    ObjectMapper findParentMapper(String leafFieldPath) {
        var pathComponents = leafFieldPath.split("\\.");
        int startPathComponent = 0;

        ObjectMapper current = this;
        String pathInCurrent = leafFieldPath;

        while (current != null) {
            if (current.mappers.containsKey(pathInCurrent)) {
                return current;
            }

            // Go one level down if possible
            var parent = current;
            current = null;

            var childMapperName = new StringBuilder();
            for (int i = startPathComponent; i < pathComponents.length - 1; i++) {
                if (childMapperName.isEmpty() == false) {
                    childMapperName.append(".");
                }
                childMapperName.append(pathComponents[i]);

                var childMapper = parent.mappers.get(childMapperName.toString());
                if (childMapper instanceof ObjectMapper objectMapper) {
                    current = objectMapper;
                    startPathComponent = i + 1;
                    pathInCurrent = pathInCurrent.substring(childMapperName.length() + 1);
                    break;
                }
            }
        }

        return null;
    }

    SourceLoader.SyntheticFieldLoader syntheticFieldLoader(SourceFilter filter, Collection<Mapper> mappers, boolean isFragment) {
        var fields = mappers.stream()
            .sorted(Comparator.comparing(Mapper::fullPath))
            .map(m -> innerSyntheticFieldLoader(filter, m))
            .filter(l -> l != SourceLoader.SyntheticFieldLoader.NOTHING)
            .toList();
        return new SyntheticSourceFieldLoader(filter, fields, isFragment);
    }

    final SourceLoader.SyntheticFieldLoader syntheticFieldLoader(@Nullable SourceFilter filter) {
        return syntheticFieldLoader(filter, mappers.values(), false);
    }

    private SourceLoader.SyntheticFieldLoader innerSyntheticFieldLoader(SourceFilter filter, Mapper mapper) {
        if (mapper instanceof MetadataFieldMapper metaMapper) {
            return metaMapper.syntheticFieldLoader();
        }
        if (filter != null && filter.isPathFiltered(mapper.fullPath(), mapper instanceof ObjectMapper)) {
            return SourceLoader.SyntheticFieldLoader.NOTHING;
        }

        if (mapper instanceof ObjectMapper objectMapper) {
            return objectMapper.syntheticFieldLoader(filter);
        }

        if (mapper instanceof FieldMapper fieldMapper) {
            return fieldMapper.syntheticFieldLoader();
        }
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }

    private class SyntheticSourceFieldLoader implements SourceLoader.SyntheticFieldLoader {
        private final SourceFilter filter;
        private final XContentParserConfiguration parserConfig;
        private final List<SourceLoader.SyntheticFieldLoader> fields;
        private final boolean isFragment;

        private boolean storedFieldLoadersHaveValues;
        private boolean docValuesLoadersHaveValues;
        private boolean ignoredValuesPresent;
        private List<IgnoredSourceFieldMapper.NameValue> ignoredValues;
        // If this loader has anything to write.
        // In special cases this can be false even if doc values loaders or stored field loaders
        // have values.
        // F.e. objects that only contain fields that are destinations of copy_to.
        private boolean writersHaveValues;
        // Use an ordered map between field names and writers to order writing by field name.
        private TreeMap<String, FieldWriter> currentWriters;

        private SyntheticSourceFieldLoader(SourceFilter filter, List<SourceLoader.SyntheticFieldLoader> fields, boolean isFragment) {
            this.fields = fields;
            this.isFragment = isFragment;
            this.filter = filter;
            String fullPath = ObjectMapper.this.isRoot() ? null : fullPath();
            this.parserConfig = filter == null
                ? XContentParserConfiguration.EMPTY
                : XContentParserConfiguration.EMPTY.withFiltering(
                    fullPath,
                    filter.getIncludes() != null ? Set.of(filter.getIncludes()) : null,
                    filter.getExcludes() != null ? Set.of(filter.getExcludes()) : null,
                    true
                );
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return fields.stream()
                .flatMap(SourceLoader.SyntheticFieldLoader::storedFieldLoaders)
                .map(e -> Map.entry(e.getKey(), newValues -> {
                    storedFieldLoadersHaveValues = true;
                    e.getValue().load(newValues);
                }));
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            List<DocValuesLoader> loaders = new ArrayList<>();
            for (SourceLoader.SyntheticFieldLoader field : fields) {
                DocValuesLoader loader = field.docValuesLoader(leafReader, docIdsInLeaf);
                if (loader != null) {
                    loaders.add(loader);
                }
            }
            if (loaders.isEmpty()) {
                return null;
            }
            return new ObjectDocValuesLoader(loaders);
        }

        private class ObjectDocValuesLoader implements DocValuesLoader {
            private final List<DocValuesLoader> loaders;

            private ObjectDocValuesLoader(List<DocValuesLoader> loaders) {
                this.loaders = loaders;
            }

            @Override
            public boolean advanceToDoc(int docId) throws IOException {
                boolean anyLeafHasDocValues = false;
                for (DocValuesLoader docValueLoader : loaders) {
                    boolean leafHasValue = docValueLoader.advanceToDoc(docId);
                    anyLeafHasDocValues |= leafHasValue;
                }
                docValuesLoadersHaveValues = anyLeafHasDocValues;
                return anyLeafHasDocValues;
            }
        }

        @Override
        public void prepare() {
            if ((storedFieldLoadersHaveValues || docValuesLoadersHaveValues || ignoredValuesPresent) == false) {
                writersHaveValues = false;
                return;
            }

            for (var loader : fields) {
                // Currently this logic is only relevant for object loaders.
                if (loader instanceof ObjectMapper.SyntheticSourceFieldLoader objectSyntheticFieldLoader) {
                    objectSyntheticFieldLoader.prepare();
                }
            }

            currentWriters = new TreeMap<>();

            if (ignoredValues != null && ignoredValues.isEmpty() == false) {
                for (IgnoredSourceFieldMapper.NameValue value : ignoredValues) {
                    if (value.hasValue()) {
                        writersHaveValues |= true;
                    }

                    var existing = currentWriters.get(value.name());
                    if (existing == null) {
                        currentWriters.put(value.name(), new FieldWriter.IgnoredSource(filter, value));
                    } else if (existing instanceof FieldWriter.IgnoredSource isw) {
                        isw.mergeWith(value);
                    }
                }
            }

            for (SourceLoader.SyntheticFieldLoader field : fields) {
                if (field.hasValue()) {
                    if (currentWriters.containsKey(field.fieldName()) == false) {
                        writersHaveValues |= true;
                        currentWriters.put(field.fieldName(), new FieldWriter.FieldLoader(field));
                    } else {
                        // Skip if the field source is stored separately, to avoid double-printing.
                        // Make sure to reset the state of loader so that values stored inside will not
                        // be used after this document is finished.
                        field.reset();
                    }
                }
            }
        }

        @Override
        public boolean hasValue() {
            return writersHaveValues;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (hasValue() == false) {
                return;
            }

            if (isRoot() && isEnabled() == false) {
                // If the root object mapper is disabled, it is expected to contain
                // the source encapsulated within a single ignored source value.
                assert ignoredValues.size() == 1 : ignoredValues.size();
                var value = ignoredValues.get(0).value();
                var type = XContentDataHelper.decodeType(value);
                assert type.isPresent();
                XContentDataHelper.decodeAndWriteXContent(parserConfig, b, type.get(), ignoredValues.get(0).value());
                softReset();
                return;
            }

            if (isRoot() || isFragment) {
                b.startObject();
            } else {
                b.startObject(leafName());
            }

            for (var writer : currentWriters.values()) {
                if (writer.hasValue()) {
                    writer.writeTo(b);
                }
            }

            b.endObject();
            softReset();
        }

        /**
         * reset() is expensive since it will descend the hierarchy and reset the loader
         * of every field.
         * We perform a reset of a child field inside write() only when it is needed.
         * We know that either write() or reset() was called for every field,
         * so in the end of write() we can do this soft reset only.
         */
        private void softReset() {
            storedFieldLoadersHaveValues = false;
            docValuesLoadersHaveValues = false;
            ignoredValuesPresent = false;
            ignoredValues = null;
            writersHaveValues = false;
        }

        @Override
        public void reset() {
            softReset();
            fields.forEach(SourceLoader.SyntheticFieldLoader::reset);
        }

        @Override
        public boolean setIgnoredValues(Map<String, List<IgnoredSourceFieldMapper.NameValue>> objectsWithIgnoredFields) {
            if (objectsWithIgnoredFields == null || objectsWithIgnoredFields.isEmpty()) {
                return false;
            }
            ignoredValues = objectsWithIgnoredFields.remove(ObjectMapper.this.fullPath());
            ignoredValuesPresent |= ignoredValues != null;
            for (SourceLoader.SyntheticFieldLoader loader : fields) {
                ignoredValuesPresent |= loader.setIgnoredValues(objectsWithIgnoredFields);
            }
            return this.ignoredValues != null;
        }

        @Override
        public String fieldName() {
            return ObjectMapper.this.fullPath();
        }

        interface FieldWriter {
            void writeTo(XContentBuilder builder) throws IOException;

            boolean hasValue();

            record FieldLoader(SourceLoader.SyntheticFieldLoader loader) implements FieldWriter {
                @Override
                public void writeTo(XContentBuilder builder) throws IOException {
                    loader.write(builder);
                }

                @Override
                public boolean hasValue() {
                    return loader.hasValue();
                }
            }

            class IgnoredSource implements FieldWriter {
                private final XContentParserConfiguration parserConfig;
                private final String fieldName;
                private final String leafName;
                private final List<BytesRef> encodedValues;

                IgnoredSource(SourceFilter filter, IgnoredSourceFieldMapper.NameValue initialValue) {
                    parserConfig = filter == null
                        ? XContentParserConfiguration.EMPTY
                        : XContentParserConfiguration.EMPTY.withFiltering(
                            initialValue.name(),
                            filter.getIncludes() != null ? Set.of(filter.getIncludes()) : null,
                            filter.getExcludes() != null ? Set.of(filter.getExcludes()) : null,
                            true
                        );
                    this.fieldName = initialValue.name();
                    this.leafName = initialValue.getFieldName();
                    this.encodedValues = new ArrayList<>();
                    if (initialValue.hasValue()) {
                        this.encodedValues.add(initialValue.value());
                    }
                }

                @Override
                public void writeTo(XContentBuilder builder) throws IOException {
                    XContentDataHelper.writeMerged(parserConfig, builder, leafName, encodedValues);
                }

                @Override
                public boolean hasValue() {
                    return encodedValues.isEmpty() == false;
                }

                public FieldWriter mergeWith(IgnoredSourceFieldMapper.NameValue nameValue) {
                    assert Objects.equals(nameValue.name(), fieldName) : "IgnoredSource is merged with wrong field data";
                    if (nameValue.hasValue()) {
                        encodedValues.add(nameValue.value());
                    }
                    return this;
                }
            }
        }
    }

    protected boolean isRoot() {
        return false;
    }
}
