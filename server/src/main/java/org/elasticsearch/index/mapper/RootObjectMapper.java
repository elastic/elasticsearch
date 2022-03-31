/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
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
import java.util.function.BiConsumer;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseDateTimeFormatter;

public class RootObjectMapper extends ObjectMapper {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RootObjectMapper.class);

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

        public Builder(String name) {
            super(name);
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
            return new RootObjectMapper(
                name,
                enabled,
                dynamic,
                buildMappers(true, context),
                runtimeFields,
                dynamicDateTimeFormatters,
                dynamicTemplates,
                dateDetection,
                numericDetection
            );
        }
    }

    /**
     * Removes redundant root includes in {@link NestedObjectMapper} trees to avoid duplicate
     * fields on the root mapper when {@code isIncludeInRoot} is {@code true} for a node that is
     * itself included into a parent node, for which either {@code isIncludeInRoot} is
     * {@code true} or which is transitively included in root by a chain of nodes with
     * {@code isIncludeInParent} returning {@code true}.
     */
    // TODO it would be really nice to make this an implementation detail of NestedObjectMapper
    // and run it as part of the builder, but this does not yet work because of the way that
    // index templates are merged together. If merge() was run on Builder objects rather than
    // on Mappers then we could move this.
    public void fixRedundantIncludes() {
        fixRedundantIncludes(this, true);
    }

    private static void fixRedundantIncludes(ObjectMapper objectMapper, boolean parentIncluded) {
        for (Mapper mapper : objectMapper) {
            if (mapper instanceof NestedObjectMapper child) {
                boolean isNested = child.isNested();
                boolean includeInRootViaParent = parentIncluded && isNested && child.isIncludeInParent();
                boolean includedInRoot = isNested && child.isIncludeInRoot();
                if (includeInRootViaParent && includedInRoot) {
                    child.setIncludeInParent(true);
                    child.setIncludeInRoot(false);
                }
                fixRedundantIncludes(child, includeInRootViaParent || includedInRoot);
            }
        }
    }

    static final class TypeParser extends ObjectMapper.TypeParser {

        @Override
        public RootObjectMapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            RootObjectMapper.Builder builder = new Builder(name);
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
                    Map<String, RuntimeField> fields = RuntimeField.parseRuntimeFields(
                        (Map<String, Object>) fieldNode,
                        parserContext,
                        true
                    );
                    builder.addRuntimeFields(fields);
                    return true;
                } else {
                    throw new ElasticsearchParseException("runtime must be a map type");
                }
            }
            return false;
        }
    }

    private Explicit<DateFormatter[]> dynamicDateTimeFormatters;
    private Explicit<Boolean> dateDetection;
    private Explicit<Boolean> numericDetection;
    private Explicit<DynamicTemplate[]> dynamicTemplates;
    private Map<String, RuntimeField> runtimeFields;

    RootObjectMapper(
        String name,
        Explicit<Boolean> enabled,
        Dynamic dynamic,
        Map<String, Mapper> mappers,
        Map<String, RuntimeField> runtimeFields,
        Explicit<DateFormatter[]> dynamicDateTimeFormatters,
        Explicit<DynamicTemplate[]> dynamicTemplates,
        Explicit<Boolean> dateDetection,
        Explicit<Boolean> numericDetection
    ) {
        super(name, name, enabled, dynamic, mappers);
        this.runtimeFields = runtimeFields;
        this.dynamicTemplates = dynamicTemplates;
        this.dynamicDateTimeFormatters = dynamicDateTimeFormatters;
        this.dateDetection = dateDetection;
        this.numericDetection = numericDetection;
    }

    @Override
    protected ObjectMapper clone() {
        ObjectMapper clone = super.clone();
        ((RootObjectMapper) clone).runtimeFields = new HashMap<>(this.runtimeFields);
        return clone;
    }

    @Override
    public RootObjectMapper.Builder newBuilder(Version indexVersionCreated) {
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder(name());
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        return builder;
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
    public RootObjectMapper merge(Mapper mergeWith, MergeReason reason) {
        return (RootObjectMapper) super.merge(mergeWith, reason);
    }

    @Override
    protected void doMerge(ObjectMapper mergeWith, MergeReason reason) {
        super.doMerge(mergeWith, reason);
        RootObjectMapper mergeWithObject = (RootObjectMapper) mergeWith;
        if (mergeWithObject.numericDetection.explicit()) {
            this.numericDetection = mergeWithObject.numericDetection;
        }

        if (mergeWithObject.dateDetection.explicit()) {
            this.dateDetection = mergeWithObject.dateDetection;
        }

        if (mergeWithObject.dynamicDateTimeFormatters.explicit()) {
            this.dynamicDateTimeFormatters = mergeWithObject.dynamicDateTimeFormatters;
        }

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
                this.dynamicTemplates = new Explicit<>(mergedTemplates, true);
            } else {
                this.dynamicTemplates = mergeWithObject.dynamicTemplates;
            }
        }
        assert this.runtimeFields != mergeWithObject.runtimeFields;
        for (Map.Entry<String, RuntimeField> runtimeField : mergeWithObject.runtimeFields.entrySet()) {
            if (runtimeField.getValue() == null) {
                this.runtimeFields.remove(runtimeField.getKey());
            } else {
                this.runtimeFields.put(runtimeField.getKey(), runtimeField.getValue());
            }
        }
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
                        (name, mapping) -> typeParser.parse(name, mapping, parserContext).build(MapperBuilderContext.ROOT)
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
            final boolean failInvalidDynamicTemplates = parserContext.indexVersionCreated().onOrAfter(Version.V_8_0_0);
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
}
