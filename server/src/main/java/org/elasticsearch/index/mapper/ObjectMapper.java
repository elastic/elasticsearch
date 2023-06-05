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
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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

public class ObjectMapper extends Mapper implements Cloneable {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ObjectMapper.class);

    public static final String CONTENT_TYPE = "object";

    public static class Defaults {
        public static final boolean ENABLED = true;
        public static final Explicit<Boolean> SUBOBJECTS = Explicit.IMPLICIT_TRUE;
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
        };

        /**
         * Get the root-level dynamic setting for a Mapping
         *
         * If no dynamic settings are explicitly configured, we default to {@link #TRUE}
         */
        static Dynamic getRootDynamic(MappingLookup mappingLookup) {
            ObjectMapper.Dynamic rootDynamic = mappingLookup.getMapping().getRoot().dynamic;
            return rootDynamic == null ? ObjectMapper.Dynamic.TRUE : rootDynamic;
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
                int firstDotIndex = name.indexOf(".");
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
                assert mapper instanceof ObjectMapper == false || subobjects.value() : "unexpected object while subobjects are disabled";
                Mapper existing = mappers.get(mapper.simpleName());
                if (existing != null) {
                    // The same mappings or document may hold the same field twice, either because duplicated JSON keys are allowed or
                    // the same field is provided using the object notation as well as the dot notation at the same time.
                    // This can also happen due to multiple index templates being merged into a single mappings definition using
                    // XContentHelper#mergeDefaults, again in case some index templates contained mappings for the same field using a
                    // mix of object notation and dot notation.
                    mapper = existing.merge(mapper, mapperBuilderContext);
                }
                mappers.put(mapper.simpleName(), mapper);
            }
            return mappers;
        }

        @Override
        public ObjectMapper build(MapperBuilderContext context) {
            return new ObjectMapper(
                name,
                context.buildFullName(name),
                enabled,
                subobjects,
                dynamic,
                buildMappers(context.createChildContext(name))
            );
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public boolean supportsVersion(Version indexCreatedVersion) {
            return true;
        }

        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            parserContext.incrementMappingObjectDepth(); // throws MapperParsingException if depth limit is exceeded
            Explicit<Boolean> subobjects = parseSubobjects(node);
            ObjectMapper.Builder builder = new Builder(name, subobjects);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)) {
                    iterator.remove();
                }
            }
            parserContext.decrementMappingObjectDepth();
            return builder;
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
            return Explicit.IMPLICIT_TRUE;
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

                    if (objBuilder.subobjects.value() == false && type.equals(ObjectMapper.CONTENT_TYPE)) {
                        throw new MapperParsingException(
                            "Tried to add subobject ["
                                + fieldName
                                + "] to object ["
                                + objBuilder.name()
                                + "] which does not support subobjects"
                        );
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

    private static void validateFieldName(String fieldName, Version indexCreatedVersion) {
        if (fieldName.isEmpty()) {
            throw new IllegalArgumentException("field name cannot be an empty string");
        }
        if (fieldName.isBlank() & indexCreatedVersion.onOrAfter(Version.V_8_6_0)) {
            // blank field names were previously accepted in mappings, but not in documents.
            throw new IllegalArgumentException("field name cannot contain only whitespaces");
        }
    }

    private final String fullPath;

    protected Explicit<Boolean> enabled;
    protected Explicit<Boolean> subobjects;
    protected volatile Dynamic dynamic;

    protected Map<String, Mapper> mappers;

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
    }

    @Override
    protected ObjectMapper clone() {
        ObjectMapper clone;
        try {
            clone = (ObjectMapper) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.mappers = Map.copyOf(clone.mappers);
        return clone;
    }

    /**
     * @return a Builder that will produce an empty ObjectMapper with the same configuration as this one
     */
    public ObjectMapper.Builder newBuilder(Version indexVersionCreated) {
        ObjectMapper.Builder builder = new ObjectMapper.Builder(simpleName(), subobjects);
        builder.enabled = this.enabled;
        builder.dynamic = this.dynamic;
        return builder;
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
    public ObjectMapper merge(Mapper mergeWith, MapperBuilderContext mapperBuilderContext) {
        return merge(mergeWith, MergeReason.MAPPING_UPDATE, mapperBuilderContext);
    }

    @Override
    public void validate(MappingLookup mappers) {
        for (Mapper mapper : this.mappers.values()) {
            mapper.validate(mappers);
        }
    }

    protected MapperBuilderContext createChildContext(MapperBuilderContext mapperBuilderContext, String name) {
        return mapperBuilderContext.createChildContext(name);
    }

    public ObjectMapper merge(Mapper mergeWith, MergeReason reason, MapperBuilderContext parentBuilderContext) {
        if ((mergeWith instanceof ObjectMapper) == false) {
            throw new IllegalArgumentException("can't merge a non object mapping [" + mergeWith.name() + "] with an object mapping");
        }
        if (mergeWith instanceof NestedObjectMapper) {
            // TODO stop NestedObjectMapper extending ObjectMapper?
            throw new IllegalArgumentException("can't merge a nested mapping [" + mergeWith.name() + "] with a non-nested mapping");
        }
        ObjectMapper mergeWithObject = (ObjectMapper) mergeWith;
        ObjectMapper merged = clone();
        merged.doMerge(mergeWithObject, reason, parentBuilderContext);
        return merged;
    }

    protected void doMerge(final ObjectMapper mergeWith, MergeReason reason, MapperBuilderContext parentBuilderContext) {
        if (mergeWith.dynamic != null) {
            this.dynamic = mergeWith.dynamic;
        }

        if (mergeWith.enabled.explicit()) {
            if (reason == MergeReason.INDEX_TEMPLATE) {
                this.enabled = mergeWith.enabled;
            } else if (isEnabled() != mergeWith.isEnabled()) {
                throw new MapperException("the [enabled] parameter can't be updated for the object mapping [" + name() + "]");
            }
        }

        if (mergeWith.subobjects.explicit()) {
            if (reason == MergeReason.INDEX_TEMPLATE) {
                this.subobjects = mergeWith.subobjects;
            } else if (subobjects != mergeWith.subobjects) {
                throw new MapperException("the [subobjects] parameter can't be updated for the object mapping [" + name() + "]");
            }
        }

        MapperBuilderContext objectBuilderContext = createChildContext(parentBuilderContext, simpleName());
        Map<String, Mapper> mergedMappers = null;
        for (Mapper mergeWithMapper : mergeWith) {
            Mapper mergeIntoMapper = (mergedMappers == null ? mappers : mergedMappers).get(mergeWithMapper.simpleName());

            Mapper merged;
            if (mergeIntoMapper == null) {
                merged = mergeWithMapper;
            } else if (mergeIntoMapper instanceof ObjectMapper objectMapper) {
                merged = objectMapper.merge(mergeWithMapper, reason, objectBuilderContext);
            } else {
                assert mergeIntoMapper instanceof FieldMapper || mergeIntoMapper instanceof FieldAliasMapper;
                if (mergeWithMapper instanceof ObjectMapper) {
                    throw new IllegalArgumentException(
                        "can't merge a non object mapping [" + mergeWithMapper.name() + "] with an object mapping"
                    );
                }

                // If we're merging template mappings when creating an index, then a field definition always
                // replaces an existing one.
                if (reason == MergeReason.INDEX_TEMPLATE) {
                    merged = mergeWithMapper;
                } else {
                    merged = mergeIntoMapper.merge(mergeWithMapper, objectBuilderContext);
                }
            }
            if (mergedMappers == null) {
                mergedMappers = new HashMap<>(mappers);
            }
            mergedMappers.put(merged.simpleName(), merged);
        }
        if (mergedMappers != null) {
            mappers = Map.copyOf(mergedMappers);
        }
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
