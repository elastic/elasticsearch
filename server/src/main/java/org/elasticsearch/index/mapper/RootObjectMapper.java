/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.ObjectMapper.TypeParser.parseObjectOrDocumentTypeProperties;
import static org.elasticsearch.index.mapper.ObjectMapper.TypeParser.parseSubobjects;
import static org.elasticsearch.index.mapper.TypeParsers.parseDateTimeFormatter;

public class RootObjectMapper extends ObjectMapper {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RootObjectMapper.class);
    private static final int MAX_NESTING_LEVEL_FOR_PASS_THROUGH_OBJECTS = 20;

    /**
     * Parameter used when serializing {@link RootObjectMapper} and request that the runtime section is skipped.
     * This is only needed internally when we compare different versions of mappings and assert that they are the same.
     * Runtime fields break these assertions as they can be removed: the master node sends the merged mappings without the runtime fields
     * that needed to be removed. Then each local node as part of its assertions merges the incoming mapping with the current mapping,
     *  and the previously removed runtime fields appear again, which is not desirable. The expectation is that those two versions of the
     *  mappings are the same, besides runtime fields.
     */
    static final String TOXCONTENT_SKIP_RUNTIME = "skip_runtime";

    public static class Defaults {
        public static final Explicit<DateFormatter[]> DYNAMIC_DATE_TIME_FORMATTERS = new Explicit<>(
            new DateFormatter[] {
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                DateFormatter.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis") },
            false
        );
        public static final Explicit<Boolean> DATE_DETECTION = Explicit.IMPLICIT_TRUE;
        public static final Explicit<Boolean> NUMERIC_DETECTION = Explicit.IMPLICIT_FALSE;
        private static final Explicit<DynamicTemplate[]> DYNAMIC_TEMPLATES = new Explicit<>(new DynamicTemplate[0], false);
    }

    public static class Builder extends ObjectMapper.Builder {
        protected Explicit<DynamicTemplate[]> dynamicTemplates = Defaults.DYNAMIC_TEMPLATES;
        protected Explicit<DateFormatter[]> dynamicDateTimeFormatters = Defaults.DYNAMIC_DATE_TIME_FORMATTERS;

        protected final Map<String, RuntimeField> runtimeFields = new HashMap<>();
        protected Explicit<Boolean> dateDetection = Defaults.DATE_DETECTION;
        protected Explicit<Boolean> numericDetection = Defaults.NUMERIC_DETECTION;

        private static final Logger logger = LogManager.getLogger(RootObjectMapper.Builder.class);

        public Builder(String name, Explicit<Boolean> subobjects) {
            super(name, subobjects);
        }

        public Builder dynamicDateTimeFormatter(Collection<DateFormatter> dateTimeFormatters) {
            this.dynamicDateTimeFormatters = new Explicit<>(dateTimeFormatters.toArray(new DateFormatter[0]), true);
            return this;
        }

        public Builder dynamicTemplates(Collection<DynamicTemplate> templates) {
            this.dynamicTemplates = new Explicit<>(templates.toArray(new DynamicTemplate[0]), true);
            return this;
        }

        @Override
        public RootObjectMapper.Builder add(Mapper.Builder builder) {
            super.add(builder);
            return this;
        }

        public RootObjectMapper.Builder addRuntimeField(RuntimeField runtimeField) {
            this.runtimeFields.put(runtimeField.name(), runtimeField);
            return this;
        }

        public RootObjectMapper.Builder addRuntimeFields(Map<String, RuntimeField> runtimeFields) {
            this.runtimeFields.putAll(runtimeFields);
            return this;
        }

        @Override
        public RootObjectMapper build(MapperBuilderContext context) {
            Map<String, Mapper> mappers = buildMappers(context.createChildContext(null, dynamic));
            mappers.putAll(getAliasMappers(mappers, context));
            return new RootObjectMapper(
                name(),
                enabled,
                subobjects,
                dynamic,
                mappers,
                new HashMap<>(runtimeFields),
                dynamicDateTimeFormatters,
                dynamicTemplates,
                dateDetection,
                numericDetection
            );
        }

        Map<String, Mapper> getAliasMappers(Map<String, Mapper> mappers, MapperBuilderContext context) {
            Map<String, Mapper> newMappers = new HashMap<>();
            Map<String, ObjectMapper.Builder> objectIntermediates = new HashMap<>(1);
            Map<String, ObjectMapper.Builder> objectIntermediatesFullName = new HashMap<>(1);
            getAliasMappers(mappers, mappers, newMappers, objectIntermediates, objectIntermediatesFullName, context, 0);
            for (var entry : objectIntermediates.entrySet()) {
                newMappers.put(entry.getKey(), entry.getValue().build(context));
            }
            return newMappers;
        }

        void getAliasMappers(
            Map<String, Mapper> mappers,
            Map<String, Mapper> topLevelMappers,
            Map<String, Mapper> aliasMappers,
            Map<String, ObjectMapper.Builder> objectIntermediates,
            Map<String, ObjectMapper.Builder> objectIntermediatesFullName,
            MapperBuilderContext context,
            int level
        ) {
            if (level >= MAX_NESTING_LEVEL_FOR_PASS_THROUGH_OBJECTS) {
                logger.warn("Exceeded maximum nesting level for searching for pass-through object fields within object fields.");
                return;
            }
            for (Mapper mapper : mappers.values()) {
                // Create aliases for all fields in child passthrough mappers and place them under the root object.
                if (mapper instanceof PassThroughObjectMapper passthroughMapper) {
                    for (Mapper internalMapper : passthroughMapper.mappers.values()) {
                        if (internalMapper instanceof FieldMapper fieldMapper) {
                            // If there's a conflicting alias with the same name at the root level, we don't want to throw an error
                            // to avoid indexing disruption.
                            // TODO: record an error without affecting document indexing, so that it can be investigated later.
                            Mapper conflict = mappers.get(fieldMapper.simpleName());
                            if (conflict != null) {
                                if (conflict.typeName().equals(FieldAliasMapper.CONTENT_TYPE) == false
                                    || ((FieldAliasMapper) conflict).path().equals(fieldMapper.mappedFieldType.name()) == false) {
                                    logger.warn(
                                        "Root alias for field "
                                            + fieldMapper.name()
                                            + " conflicts with existing field or alias, skipping alias creation."
                                    );
                                }
                            } else {
                                // Check if the field name contains dots, as aliases require nesting within objects in this case.
                                String[] fieldNameParts = fieldMapper.simpleName().split("\\.");
                                if (fieldNameParts.length == 0) {
                                    throw new IllegalArgumentException("field name cannot contain only dots");
                                }
                                if (fieldNameParts.length == 1) {
                                    // No nesting required, add the alias directly to the root.
                                    FieldAliasMapper aliasMapper = new FieldAliasMapper.Builder(fieldMapper.simpleName()).path(
                                        fieldMapper.mappedFieldType.name()
                                    ).build(context);
                                    aliasMappers.put(aliasMapper.simpleName(), aliasMapper);
                                } else {
                                    conflict = topLevelMappers.get(fieldNameParts[0]);
                                    if (conflict != null) {
                                        if (isConflictingObject(conflict, fieldNameParts)) {
                                            throw new IllegalArgumentException(
                                                "Conflicting objects created during alias generation for pass-through field: ["
                                                    + conflict.name()
                                                    + "]"
                                            );
                                        }
                                    }

                                    // Nest the alias within object(s).
                                    String realFieldName = fieldNameParts[fieldNameParts.length - 1];
                                    Mapper.Builder fieldBuilder = new FieldAliasMapper.Builder(realFieldName).path(
                                        fieldMapper.mappedFieldType.name()
                                    );
                                    ObjectMapper.Builder intermediate = null;
                                    for (int i = fieldNameParts.length - 2; i >= 0; --i) {
                                        String intermediateObjectName = fieldNameParts[i];
                                        intermediate = objectIntermediatesFullName.computeIfAbsent(
                                            concatStrings(fieldNameParts, i),
                                            s -> new ObjectMapper.Builder(intermediateObjectName, ObjectMapper.Defaults.SUBOBJECTS)
                                        );
                                        intermediate.add(fieldBuilder);
                                        fieldBuilder = intermediate;
                                    }
                                    objectIntermediates.putIfAbsent(fieldNameParts[0], intermediate);
                                }
                            }
                        }
                    }
                } else if (mapper instanceof ObjectMapper objectMapper) {
                    // Call recursively to check child fields. The level guards against long recursive call sequences.
                    getAliasMappers(
                        objectMapper.mappers,
                        topLevelMappers,
                        aliasMappers,
                        objectIntermediates,
                        objectIntermediatesFullName,
                        context,
                        level + 1
                    );
                }
            }
        }
    }

    private static String concatStrings(String[] parts, int last) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i <= last; i++) {
            builder.append('.');
            builder.append(parts[i]);
        }
        return builder.toString();
    }

    private static boolean isConflictingObject(Mapper mapper, String[] parts) {
        for (int i = 0; i < parts.length - 1; i++) {
            if (mapper == null) {
                return true;
            }
            if (mapper instanceof ObjectMapper objectMapper) {
                mapper = objectMapper.getMapper(parts[i + 1]);
            } else {
                return true;
            }
        }
        return mapper == null;
    }

    private final Explicit<DateFormatter[]> dynamicDateTimeFormatters;
    private final Explicit<Boolean> dateDetection;
    private final Explicit<Boolean> numericDetection;
    private final Explicit<DynamicTemplate[]> dynamicTemplates;
    private final Map<String, RuntimeField> runtimeFields;

    RootObjectMapper(
        String name,
        Explicit<Boolean> enabled,
        Explicit<Boolean> subobjects,
        Dynamic dynamic,
        Map<String, Mapper> mappers,
        Map<String, RuntimeField> runtimeFields,
        Explicit<DateFormatter[]> dynamicDateTimeFormatters,
        Explicit<DynamicTemplate[]> dynamicTemplates,
        Explicit<Boolean> dateDetection,
        Explicit<Boolean> numericDetection
    ) {
        super(name, name, enabled, subobjects, dynamic, mappers);
        this.runtimeFields = runtimeFields;
        this.dynamicTemplates = dynamicTemplates;
        this.dynamicDateTimeFormatters = dynamicDateTimeFormatters;
        this.dateDetection = dateDetection;
        this.numericDetection = numericDetection;
    }

    @Override
    public RootObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder(name(), subobjects);
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        return builder;
    }

    @Override
    RootObjectMapper withoutMappers() {
        return new RootObjectMapper(
            simpleName(),
            enabled,
            subobjects,
            dynamic,
            Map.of(),
            Map.of(),
            dynamicDateTimeFormatters,
            dynamicTemplates,
            dateDetection,
            numericDetection
        );
    }

    /**
     * Public API
     */
    public boolean dateDetection() {
        return this.dateDetection.value();
    }

    /**
     * Public API
     */
    public boolean numericDetection() {
        return this.numericDetection.value();
    }

    /**
     * Public API
     */
    public DateFormatter[] dynamicDateTimeFormatters() {
        return dynamicDateTimeFormatters.value();
    }

    /**
     * Public API
     */
    public DynamicTemplate[] dynamicTemplates() {
        return dynamicTemplates.value();
    }

    Collection<RuntimeField> runtimeFields() {
        return runtimeFields.values();
    }

    RuntimeField getRuntimeField(String name) {
        return runtimeFields.get(name);
    }

    @Override
    protected MapperMergeContext createChildContext(MapperMergeContext mapperMergeContext, String name) {
        assert Objects.equals(mapperMergeContext.getMapperBuilderContext().buildFullName("foo"), "foo");
        return mapperMergeContext.createChildContext(null, dynamic);
    }

    @Override
    public RootObjectMapper merge(Mapper mergeWith, MergeReason reason, MapperMergeContext parentMergeContext) {
        if (mergeWith instanceof RootObjectMapper == false) {
            MapperErrors.throwObjectMappingConflictError(mergeWith.name());
        }
        return merge((RootObjectMapper) mergeWith, reason, parentMergeContext);
    }

    RootObjectMapper merge(RootObjectMapper mergeWithObject, MergeReason reason, MapperMergeContext parentMergeContext) {
        final var mergeResult = MergeResult.build(this, mergeWithObject, reason, parentMergeContext);
        final Explicit<Boolean> numericDetection;
        if (mergeWithObject.numericDetection.explicit()) {
            numericDetection = mergeWithObject.numericDetection;
        } else {
            numericDetection = this.numericDetection;
        }

        final Explicit<Boolean> dateDetection;
        if (mergeWithObject.dateDetection.explicit()) {
            dateDetection = mergeWithObject.dateDetection;
        } else {
            dateDetection = this.dateDetection;
        }

        final Explicit<DateFormatter[]> dynamicDateTimeFormatters;
        if (mergeWithObject.dynamicDateTimeFormatters.explicit()) {
            dynamicDateTimeFormatters = mergeWithObject.dynamicDateTimeFormatters;
        } else {
            dynamicDateTimeFormatters = this.dynamicDateTimeFormatters;
        }

        final Explicit<DynamicTemplate[]> dynamicTemplates;
        if (mergeWithObject.dynamicTemplates.explicit()) {
            if (reason == MergeReason.INDEX_TEMPLATE) {
                Map<String, DynamicTemplate> templatesByKey = new LinkedHashMap<>();
                for (DynamicTemplate template : this.dynamicTemplates.value()) {
                    templatesByKey.put(template.name(), template);
                }
                for (DynamicTemplate template : mergeWithObject.dynamicTemplates.value()) {
                    templatesByKey.put(template.name(), template);
                }

                DynamicTemplate[] mergedTemplates = templatesByKey.values().toArray(new DynamicTemplate[0]);
                dynamicTemplates = new Explicit<>(mergedTemplates, true);
            } else {
                dynamicTemplates = mergeWithObject.dynamicTemplates;
            }
        } else {
            dynamicTemplates = this.dynamicTemplates;
        }
        final Map<String, RuntimeField> runtimeFields = new HashMap<>(this.runtimeFields);
        for (Map.Entry<String, RuntimeField> runtimeField : mergeWithObject.runtimeFields.entrySet()) {
            if (runtimeField.getValue() == null) {
                runtimeFields.remove(runtimeField.getKey());
            } else if (runtimeFields.containsKey(runtimeField.getKey())) {
                runtimeFields.put(runtimeField.getKey(), runtimeField.getValue());
            } else if (parentMergeContext.decrementFieldBudgetIfPossible(1)) {
                runtimeFields.put(runtimeField.getValue().name(), runtimeField.getValue());
            }
        }

        return new RootObjectMapper(
            simpleName(),
            mergeResult.enabled(),
            mergeResult.subObjects(),
            mergeResult.dynamic(),
            mergeResult.mappers(),
            Map.copyOf(runtimeFields),
            dynamicDateTimeFormatters,
            dynamicTemplates,
            dateDetection,
            numericDetection
        );
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        final boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (dynamicDateTimeFormatters.explicit() || includeDefaults) {
            builder.startArray("dynamic_date_formats");
            for (DateFormatter dateTimeFormatter : dynamicDateTimeFormatters.value()) {
                builder.value(dateTimeFormatter.pattern());
            }
            builder.endArray();
        }

        if (dynamicTemplates.explicit() || includeDefaults) {
            builder.startArray("dynamic_templates");
            for (DynamicTemplate dynamicTemplate : dynamicTemplates.value()) {
                builder.startObject();
                builder.field(dynamicTemplate.name(), dynamicTemplate);
                builder.endObject();
            }
            builder.endArray();
        }

        if (dateDetection.explicit() || includeDefaults) {
            builder.field("date_detection", dateDetection.value());
        }
        if (numericDetection.explicit() || includeDefaults) {
            builder.field("numeric_detection", numericDetection.value());
        }

        if (runtimeFields.size() > 0 && params.paramAsBoolean(TOXCONTENT_SKIP_RUNTIME, false) == false) {
            builder.startObject("runtime");
            List<RuntimeField> sortedRuntimeFields = runtimeFields.values()
                .stream()
                .sorted(Comparator.comparing(RuntimeField::name))
                .toList();
            for (RuntimeField fieldType : sortedRuntimeFields) {
                fieldType.toXContent(builder, params);
            }
            builder.endObject();
        }
    }

    private static void validateDynamicTemplate(MappingParserContext parserContext, DynamicTemplate template) {

        if (containsSnippet(template.getMapping(), "{name}")) {
            // Can't validate template, because field names can't be guessed up front.
            return;
        }

        final XContentFieldType[] types = template.getXContentFieldTypes();

        Exception lastError = null;

        for (XContentFieldType fieldType : types) {
            String dynamicType = template.isRuntimeMapping() ? fieldType.defaultRuntimeMappingType() : fieldType.defaultMappingType();
            String mappingType = template.mappingType(dynamicType);
            try {
                if (template.isRuntimeMapping()) {
                    RuntimeField.Parser parser = parserContext.runtimeFieldParser(mappingType);
                    if (parser == null) {
                        throw new IllegalArgumentException("No runtime field found for type [" + mappingType + "]");
                    }
                    validate(template, dynamicType, (name, mapping) -> parser.parse(name, mapping, parserContext));
                } else {
                    Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
                    if (typeParser == null) {
                        throw new IllegalArgumentException("No mapper found for type [" + mappingType + "]");
                    }
                    validate(
                        template,
                        dynamicType,
                        (name, mapping) -> typeParser.parse(name, mapping, parserContext).build(MapperBuilderContext.root(false, false))
                    );
                }
                lastError = null; // ok, the template is valid for at least one type
                break;
            } catch (Exception e) {
                lastError = e;
            }
        }
        if (lastError != null) {
            String format = "dynamic template [%s] has invalid content [%s], "
                + "attempted to validate it with the following match_mapping_type: %s";
            String message = String.format(Locale.ROOT, format, template.getName(), Strings.toString(template), Arrays.toString(types));
            final boolean failInvalidDynamicTemplates = parserContext.indexVersionCreated().onOrAfter(IndexVersions.V_8_0_0);
            if (failInvalidDynamicTemplates) {
                throw new IllegalArgumentException(message, lastError);
            } else {
                DEPRECATION_LOGGER.warn(
                    DeprecationCategory.TEMPLATES,
                    "invalid_dynamic_template",
                    "{}, last error: [{}]",
                    message,
                    lastError.getMessage()
                );
            }
        }
    }

    private static void validate(DynamicTemplate template, String dynamicType, BiConsumer<String, Map<String, Object>> mappingConsumer) {
        String templateName = "__dynamic__" + template.name();
        Map<String, Object> fieldTypeConfig = template.mappingForName(templateName, dynamicType);
        mappingConsumer.accept(templateName, fieldTypeConfig);
        fieldTypeConfig.remove("type");
        if (fieldTypeConfig.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown mapping attributes [" + fieldTypeConfig + "]");
        }
    }

    private static boolean containsSnippet(Map<?, ?> map, String snippet) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            if (key.contains(snippet)) {
                return true;
            }
            Object value = entry.getValue();
            if (containsSnippet(value, snippet)) {
                return true;
            }
        }

        return false;
    }

    private static boolean containsSnippet(List<?> list, String snippet) {
        for (Object value : list) {
            if (containsSnippet(value, snippet)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsSnippet(Object value, String snippet) {
        if (value instanceof Map) {
            return containsSnippet((Map<?, ?>) value, snippet);
        } else if (value instanceof List) {
            return containsSnippet((List<?>) value, snippet);
        } else if (value instanceof String) {
            return ((String) value).contains(snippet);
        }
        return false;
    }

    @Override
    protected void startSyntheticField(XContentBuilder b) throws IOException {
        b.startObject();
    }

    public static RootObjectMapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
        throws MapperParsingException {
        Explicit<Boolean> subobjects = parseSubobjects(node);
        RootObjectMapper.Builder builder = new Builder(name, subobjects);
        Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();
            if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)
                || processField(builder, fieldName, fieldNode, parserContext)) {
                iterator.remove();
            }
        }
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static boolean processField(
        RootObjectMapper.Builder builder,
        String fieldName,
        Object fieldNode,
        MappingParserContext parserContext
    ) {
        if (fieldName.equals("date_formats") || fieldName.equals("dynamic_date_formats")) {
            if (fieldNode instanceof List) {
                List<DateFormatter> formatters = new ArrayList<>();
                for (Object formatter : (List<?>) fieldNode) {
                    if (formatter.toString().startsWith("epoch_")) {
                        throw new MapperParsingException("Epoch [" + formatter + "] is not supported as dynamic date format");
                    }
                    formatters.add(parseDateTimeFormatter(formatter));
                }
                builder.dynamicDateTimeFormatter(formatters);
            } else if ("none".equals(fieldNode.toString())) {
                builder.dynamicDateTimeFormatter(Collections.emptyList());
            } else {
                builder.dynamicDateTimeFormatter(Collections.singleton(parseDateTimeFormatter(fieldNode)));
            }
            return true;
        } else if (fieldName.equals("dynamic_templates")) {
            /*
              "dynamic_templates" : [
                  {
                      "template_1" : {
                          "match" : "*_test",
                          "match_mapping_type" : "string",
                          "mapping" : { "type" : "keyword", "store" : "yes" }
                      }
                  }
              ]
            */
            if ((fieldNode instanceof List) == false) {
                throw new MapperParsingException("Dynamic template syntax error. An array of named objects is expected.");
            }
            List<?> tmplNodes = (List<?>) fieldNode;
            List<DynamicTemplate> templates = new ArrayList<>();
            for (Object tmplNode : tmplNodes) {
                Map<String, Object> tmpl = (Map<String, Object>) tmplNode;
                if (tmpl.size() != 1) {
                    throw new MapperParsingException("A dynamic template must be defined with a name");
                }
                Map.Entry<String, Object> entry = tmpl.entrySet().iterator().next();
                String templateName = entry.getKey();
                Map<String, Object> templateParams = (Map<String, Object>) entry.getValue();
                DynamicTemplate template = DynamicTemplate.parse(templateName, templateParams);
                validateDynamicTemplate(parserContext, template);
                templates.add(template);
            }
            builder.dynamicTemplates(templates);
            return true;
        } else if (fieldName.equals("date_detection")) {
            builder.dateDetection = Explicit.explicitBoolean(nodeBooleanValue(fieldNode, "date_detection"));
            return true;
        } else if (fieldName.equals("numeric_detection")) {
            builder.numericDetection = Explicit.explicitBoolean(nodeBooleanValue(fieldNode, "numeric_detection"));
            return true;
        } else if (fieldName.equals("runtime")) {
            if (fieldNode instanceof Map) {
                Map<String, RuntimeField> fields = RuntimeField.parseRuntimeFields((Map<String, Object>) fieldNode, parserContext, true);
                builder.addRuntimeFields(fields);
                return true;
            } else {
                throw new ElasticsearchParseException("runtime must be a map type");
            }
        }
        return false;
    }

    @Override
    public int getTotalFieldsCount() {
        return mappers.values().stream().mapToInt(Mapper::getTotalFieldsCount).sum() + runtimeFields.size();
    }
}
