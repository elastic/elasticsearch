/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

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
import java.util.stream.Stream;

public class ObjectMapper extends Mapper {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ObjectMapper.class);

    public static final String CONTENT_TYPE = "object";

    public static class Defaults {
        public static final boolean ENABLED = true;
        public static final Explicit<Boolean> SUBOBJECTS = Explicit.IMPLICIT_TRUE;
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
            ObjectMapper.Dynamic rootDynamic = mappingLookup.getMapping().getRoot().dynamic;
            return rootDynamic == null ? Defaults.DYNAMIC : rootDynamic;
        }
    }

    public static class Builder extends Mapper.Builder {
        protected final Explicit<Boolean> subobjects;
        protected Explicit<Boolean> enabled = Explicit.IMPLICIT_TRUE;
        protected Dynamic dynamic;
        protected final List<Mapper.Builder> mappersBuilders = new ArrayList<>();

        public Builder(String name, Explicit<Boolean> subobjects) {
            super(name);
            this.subobjects = subobjects;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = Explicit.explicitBoolean(enabled);
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
            if (name.contains(".") == false || subobjects.value() == false) {
                add(name, mapper);
            }
            // otherwise we strip off the first object path of the mapper name, load or create
            // the relevant object mapper, and then recurse down into it, passing the remainder
            // of the mapper name. So for a mapper 'foo.bar.baz', we locate 'foo' and then
            // call addDynamic on it with the name 'bar.baz', and next call addDynamic on 'bar' with the name 'baz'.
            else {
                int firstDotIndex = name.indexOf('.');
                String immediateChild = name.substring(0, firstDotIndex);
                String immediateChildFullName = prefix == null ? immediateChild : prefix + "." + immediateChild;
                ObjectMapper.Builder parentBuilder = findObjectBuilder(immediateChildFullName, context);
                parentBuilder.addDynamic(name.substring(firstDotIndex + 1), immediateChildFullName, mapper, context);
                add(parentBuilder);
            }
        }

        private static ObjectMapper.Builder findObjectBuilder(String fullName, DocumentParserContext context) {
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
            throw new IllegalStateException("Missing intermediate object " + fullName);
        }

        protected final Map<String, Mapper> buildMappers(MapperBuilderContext mapperBuilderContext) {
            Map<String, Mapper> mappers = new HashMap<>();
            for (Mapper.Builder builder : mappersBuilders) {
                Mapper mapper = builder.build(mapperBuilderContext);
                Mapper existing = mappers.get(mapper.simpleName());
                if (existing != null) {
                    // The same mappings or document may hold the same field twice, either because duplicated JSON keys are allowed or
                    // the same field is provided using the object notation as well as the dot notation at the same time.
                    // This can also happen due to multiple index templates being merged into a single mappings definition using
                    // XContentHelper#mergeDefaults, again in case some index templates contained mappings for the same field using a
                    // mix of object notation and dot notation.
                    mapper = existing.merge(mapper, MapperMergeContext.from(mapperBuilderContext, Long.MAX_VALUE));
                }
                if (subobjects.value() == false && mapper instanceof ObjectMapper objectMapper) {
                    // We're parsing a mapping that has set `subobjects: false` but has defined sub-objects
                    objectMapper.asFlattenedFieldMappers(mapperBuilderContext).forEach(m -> mappers.put(m.simpleName(), m));
                } else {
                    mappers.put(mapper.simpleName(), mapper);
                }
            }
            return mappers;
        }

        @Override
        public ObjectMapper build(MapperBuilderContext context) {
            return new ObjectMapper(
                name(),
                context.buildFullName(name()),
                enabled,
                subobjects,
                dynamic,
                buildMappers(context.createChildContext(name(), dynamic))
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
            Explicit<Boolean> subobjects = parseSubobjects(node);
            ObjectMapper.Builder builder = new Builder(name, subobjects);
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
            ObjectMapper.Builder builder
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

        protected static Explicit<Boolean> parseSubobjects(Map<String, Object> node) {
            Object subobjectsNode = node.remove("subobjects");
            if (subobjectsNode != null) {
                return Explicit.explicitBoolean(XContentMapValues.nodeBooleanValue(subobjectsNode, "subobjects.subobjects"));
            }
            return Defaults.SUBOBJECTS;
        }

        protected static void parseProperties(
            ObjectMapper.Builder objBuilder,
            Map<String, Object> propsNode,
            MappingParserContext parserContext
        ) {
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

                    if (objBuilder.subobjects.value() == false && type.equals(NestedObjectMapper.CONTENT_TYPE)) {
                        throw new MapperParsingException(
                            "Tried to add nested object ["
                                + fieldName
                                + "] to object ["
                                + objBuilder.name()
                                + "] which does not support subobjects"
                        );
                    }
                    Mapper.TypeParser typeParser = parserContext.typeParser(type);
                    if (typeParser == null) {
                        throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + fieldName + "]");
                    }
                    Mapper.Builder fieldBuilder;
                    if (objBuilder.subobjects.value() == false) {
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
                            ObjectMapper.Builder intermediate = new ObjectMapper.Builder(intermediateObjectName, Defaults.SUBOBJECTS);
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
    protected final Explicit<Boolean> subobjects;
    protected final Dynamic dynamic;

    protected final Map<String, Mapper> mappers;

    ObjectMapper(
        String name,
        String fullPath,
        Explicit<Boolean> enabled,
        Explicit<Boolean> subobjects,
        Dynamic dynamic,
        Map<String, Mapper> mappers
    ) {
        super(name);
        // could be blank but not empty on indices created < 8.6.0
        assert name.isEmpty() == false;
        this.fullPath = internFieldName(fullPath);
        this.enabled = enabled;
        this.subobjects = subobjects;
        this.dynamic = dynamic;
        if (mappers == null) {
            this.mappers = Map.of();
        } else {
            this.mappers = Map.copyOf(mappers);
        }
        assert subobjects.value() || this.mappers.values().stream().noneMatch(m -> m instanceof ObjectMapper)
            : "When subobjects is false, mappers must not contain an ObjectMapper";
    }

    /**
     * @return a Builder that will produce an empty ObjectMapper with the same configuration as this one
     */
    public ObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        ObjectMapper.Builder builder = new ObjectMapper.Builder(simpleName(), subobjects);
        builder.enabled = this.enabled;
        builder.dynamic = this.dynamic;
        return builder;
    }

    /**
     * Returns a copy of this object mapper that doesn't have any fields and runtime fields.
     * This is typically used in the context of a mapper merge when there's not enough budget to add the entire object.
     */
    ObjectMapper withoutMappers() {
        return new ObjectMapper(simpleName(), fullPath, enabled, subobjects, dynamic, Map.of());
    }

    @Override
    public String name() {
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

    public String fullPath() {
        return this.fullPath;
    }

    public final Dynamic dynamic() {
        return dynamic;
    }

    public final boolean subobjects() {
        return subobjects.value();
    }

    @Override
    public ObjectMapper merge(Mapper mergeWith, MapperMergeContext mapperMergeContext) {
        return merge(mergeWith, MergeReason.MAPPING_UPDATE, mapperMergeContext);
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

    public ObjectMapper merge(Mapper mergeWith, MergeReason reason, MapperMergeContext parentMergeContext) {
        if (mergeWith instanceof ObjectMapper == false) {
            MapperErrors.throwObjectMappingConflictError(mergeWith.name());
        }
        if (this instanceof NestedObjectMapper == false && mergeWith instanceof NestedObjectMapper) {
            // TODO stop NestedObjectMapper extending ObjectMapper?
            MapperErrors.throwNestedMappingConflictError(mergeWith.name());
        }
        return merge((ObjectMapper) mergeWith, reason, parentMergeContext);
    }

    ObjectMapper merge(ObjectMapper mergeWith, MergeReason reason, MapperMergeContext parentMergeContext) {
        var mergeResult = MergeResult.build(this, mergeWith, reason, parentMergeContext);
        return new ObjectMapper(
            simpleName(),
            fullPath,
            mergeResult.enabled,
            mergeResult.subObjects,
            mergeResult.dynamic,
            mergeResult.mappers
        );
    }

    protected record MergeResult(
        Explicit<Boolean> enabled,
        Explicit<Boolean> subObjects,
        ObjectMapper.Dynamic dynamic,
        Map<String, Mapper> mappers
    ) {
        static MergeResult build(
            ObjectMapper existing,
            ObjectMapper mergeWithObject,
            MergeReason reason,
            MapperMergeContext parentMergeContext
        ) {
            final Explicit<Boolean> enabled;
            if (mergeWithObject.enabled.explicit()) {
                if (reason == MergeReason.INDEX_TEMPLATE) {
                    enabled = mergeWithObject.enabled;
                } else if (existing.isEnabled() != mergeWithObject.isEnabled()) {
                    throw new MapperException("the [enabled] parameter can't be updated for the object mapping [" + existing.name() + "]");
                } else {
                    enabled = existing.enabled;
                }
            } else {
                enabled = existing.enabled;
            }
            final Explicit<Boolean> subObjects;
            if (mergeWithObject.subobjects.explicit()) {
                if (reason == MergeReason.INDEX_TEMPLATE) {
                    subObjects = mergeWithObject.subobjects;
                } else if (existing.subobjects != mergeWithObject.subobjects) {
                    throw new MapperException(
                        "the [subobjects] parameter can't be updated for the object mapping [" + existing.name() + "]"
                    );
                } else {
                    subObjects = existing.subobjects;
                }
            } else {
                subObjects = existing.subobjects;
            }
            MapperMergeContext objectMergeContext = existing.createChildContext(parentMergeContext, existing.simpleName());
            Map<String, Mapper> mergedMappers = buildMergedMappers(
                existing,
                mergeWithObject,
                reason,
                objectMergeContext,
                subObjects.value()
            );
            return new MergeResult(
                enabled,
                subObjects,
                mergeWithObject.dynamic != null ? mergeWithObject.dynamic : existing.dynamic,
                mergedMappers
            );
        }

        private static Map<String, Mapper> buildMergedMappers(
            ObjectMapper existing,
            ObjectMapper mergeWithObject,
            MergeReason reason,
            MapperMergeContext objectMergeContext,
            boolean subobjects
        ) {
            Map<String, Mapper> mergedMappers = new HashMap<>();
            for (Mapper childOfExistingMapper : existing.mappers.values()) {
                if (subobjects == false && childOfExistingMapper instanceof ObjectMapper objectMapper) {
                    // An existing mapping with sub-objects is merged with a mapping that has set `subobjects: false`
                    objectMapper.asFlattenedFieldMappers(objectMergeContext.getMapperBuilderContext())
                        .forEach(m -> mergedMappers.put(m.simpleName(), m));
                } else {
                    putMergedMapper(mergedMappers, childOfExistingMapper);
                }
            }
            for (Mapper mergeWithMapper : mergeWithObject) {
                Mapper mergeIntoMapper = mergedMappers.get(mergeWithMapper.simpleName());
                if (mergeIntoMapper == null) {
                    if (subobjects == false && mergeWithMapper instanceof ObjectMapper objectMapper) {
                        // An existing mapping that has set `subobjects: false` is merged with a mapping with sub-objects
                        objectMapper.asFlattenedFieldMappers(objectMergeContext.getMapperBuilderContext())
                            .stream()
                            .filter(m -> objectMergeContext.decrementFieldBudgetIfPossible(m.getTotalFieldsCount()))
                            .forEach(m -> putMergedMapper(mergedMappers, m));
                    } else if (objectMergeContext.decrementFieldBudgetIfPossible(mergeWithMapper.getTotalFieldsCount())) {
                        putMergedMapper(mergedMappers, mergeWithMapper);
                    } else if (mergeWithMapper instanceof ObjectMapper om) {
                        putMergedMapper(mergedMappers, truncateObjectMapper(reason, objectMergeContext, om));
                    }
                } else if (mergeIntoMapper instanceof ObjectMapper objectMapper) {
                    assert subobjects : "existing object mappers are supposed to be flattened if subobjects is false";
                    putMergedMapper(mergedMappers, objectMapper.merge(mergeWithMapper, reason, objectMergeContext));
                } else {
                    assert mergeIntoMapper instanceof FieldMapper || mergeIntoMapper instanceof FieldAliasMapper;
                    if (mergeWithMapper instanceof NestedObjectMapper) {
                        MapperErrors.throwNestedMappingConflictError(mergeWithMapper.name());
                    } else if (mergeWithMapper instanceof ObjectMapper) {
                        MapperErrors.throwObjectMappingConflictError(mergeWithMapper.name());
                    }

                    // If we're merging template mappings when creating an index, then a field definition always
                    // replaces an existing one.
                    if (reason == MergeReason.INDEX_TEMPLATE) {
                        putMergedMapper(mergedMappers, mergeWithMapper);
                    } else {
                        putMergedMapper(mergedMappers, mergeIntoMapper.merge(mergeWithMapper, objectMergeContext));
                    }
                }
            }
            return Map.copyOf(mergedMappers);
        }

        private static void putMergedMapper(Map<String, Mapper> mergedMappers, @Nullable Mapper merged) {
            if (merged != null) {
                mergedMappers.put(merged.simpleName(), merged);
            }
        }

        private static ObjectMapper truncateObjectMapper(MergeReason reason, MapperMergeContext context, ObjectMapper objectMapper) {
            // there's not enough capacity for the whole object mapper,
            // so we're just trying to add the shallow object, without it's sub-fields
            ObjectMapper shallowObjectMapper = objectMapper.withoutMappers();
            if (context.decrementFieldBudgetIfPossible(shallowObjectMapper.getTotalFieldsCount())) {
                // now trying to add the sub-fields one by one via a merge, until we hit the limit
                return shallowObjectMapper.merge(objectMapper, reason, context);
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
        path.add(simpleName());
        for (Mapper mapper : mappers.values()) {
            if (mapper instanceof FieldMapper fieldMapper) {
                FieldMapper.Builder fieldBuilder = fieldMapper.getMergeBuilder();
                fieldBuilder.setName(path.pathAsText(mapper.simpleName()));
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
        if (isEnabled() == false) {
            throwAutoFlatteningException(path, "the value of [enabled] is [false]");
        }
        if (subobjects.explicit() && subobjects()) {
            throwAutoFlatteningException(path, "the value of [subobjects] is [true]");
        }
    }

    private void throwAutoFlatteningException(ContentPath path, String reason) {
        throw new IllegalArgumentException(
            "Object mapper ["
                + path.pathAsText(simpleName())
                + "] was found in a context where subobjects is set to false. "
                + "Auto-flattening ["
                + path.pathAsText(simpleName())
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
        builder.startObject(simpleName());
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
        if (subobjects != Defaults.SUBOBJECTS) {
            builder.field("subobjects", subobjects.value());
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
        Arrays.sort(sortedMappers, Comparator.comparing(Mapper::name));

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

    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader(Stream<Mapper> extra) {
        return new SyntheticSourceFieldLoader(
            Stream.concat(extra, mappers.values().stream())
                .sorted(Comparator.comparing(Mapper::name))
                .map(Mapper::syntheticFieldLoader)
                .filter(l -> l != null)
                .toList()
        );
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return syntheticFieldLoader(Stream.empty());
    }

    private class SyntheticSourceFieldLoader implements SourceLoader.SyntheticFieldLoader {
        private final List<SourceLoader.SyntheticFieldLoader> fields;
        private boolean hasValue;

        private SyntheticSourceFieldLoader(List<SourceLoader.SyntheticFieldLoader> fields) {
            this.fields = fields;
        }

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return fields.stream().flatMap(SourceLoader.SyntheticFieldLoader::storedFieldLoaders).map(e -> Map.entry(e.getKey(), values -> {
                hasValue = true;
                e.getValue().load(values);
            }));
        }

        @Override
        public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
            List<SourceLoader.SyntheticFieldLoader.DocValuesLoader> loaders = new ArrayList<>();
            for (SourceLoader.SyntheticFieldLoader field : fields) {
                SourceLoader.SyntheticFieldLoader.DocValuesLoader loader = field.docValuesLoader(leafReader, docIdsInLeaf);
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
            private final List<SourceLoader.SyntheticFieldLoader.DocValuesLoader> loaders;

            private ObjectDocValuesLoader(List<DocValuesLoader> loaders) {
                this.loaders = loaders;
            }

            @Override
            public boolean advanceToDoc(int docId) throws IOException {
                boolean anyLeafHasDocValues = false;
                for (SourceLoader.SyntheticFieldLoader.DocValuesLoader docValueLoader : loaders) {
                    boolean leafHasValue = docValueLoader.advanceToDoc(docId);
                    anyLeafHasDocValues |= leafHasValue;
                }
                hasValue |= anyLeafHasDocValues;
                return anyLeafHasDocValues;
            }
        }

        @Override
        public boolean hasValue() {
            return hasValue;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (hasValue == false) {
                return;
            }
            startSyntheticField(b);
            for (SourceLoader.SyntheticFieldLoader field : fields) {
                if (field.hasValue()) {
                    field.write(b);
                }
            }
            b.endObject();
            hasValue = false;
        }
    }

    protected void startSyntheticField(XContentBuilder b) throws IOException {
        b.startObject(simpleName());
    }
}
