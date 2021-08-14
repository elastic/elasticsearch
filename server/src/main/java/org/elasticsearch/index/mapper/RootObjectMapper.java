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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

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
import java.util.stream.Collectors;

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
        public static final DateFormatter[] DYNAMIC_DATE_TIME_FORMATTERS =
                new DateFormatter[]{
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                        DateFormatter.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis")
                };
        public static final boolean DATE_DETECTION = true;
        public static final boolean NUMERIC_DETECTION = false;
    }

    public static class Builder extends ObjectMapper.Builder {

        protected Explicit<DynamicTemplate[]> dynamicTemplates = new Explicit<>(new DynamicTemplate[0], false);
        protected Explicit<DateFormatter[]> dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        protected Explicit<Boolean> dateDetection = new Explicit<>(Defaults.DATE_DETECTION, false);
        protected Explicit<Boolean> numericDetection = new Explicit<>(Defaults.NUMERIC_DETECTION, false);
        protected Map<String, RuntimeField> runtimeFields;

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

        public RootObjectMapper.Builder setRuntime(Map<String, RuntimeField> runtimeFields) {
            this.runtimeFields = runtimeFields;
            return this;
        }

        @Override
        public RootObjectMapper build(ContentPath contentPath) {
            return new RootObjectMapper(name, enabled, dynamic, buildMappers(contentPath),
                runtimeFields == null ? Collections.emptyMap() : runtimeFields,
                dynamicDateTimeFormatters,
                dynamicTemplates,
                dateDetection, numericDetection);
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
            if (mapper instanceof NestedObjectMapper) {
                NestedObjectMapper child = (NestedObjectMapper) mapper;
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
        private boolean processField(RootObjectMapper.Builder builder,
                                     String fieldName, Object fieldNode,
                                     MappingParserContext parserContext) {
            if (fieldName.equals("date_formats") || fieldName.equals("dynamic_date_formats")) {
                if (fieldNode instanceof List) {
                    List<DateFormatter> formatters = new ArrayList<>();
                    for (Object formatter : (List<?>) fieldNode) {
                        if (formatter.toString().startsWith("epoch_")) {
                            throw new MapperParsingException("Epoch ["+ formatter +"] is not supported as dynamic date format");
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
                //  "dynamic_templates" : [
                //      {
                //          "template_1" : {
                //              "match" : "*_test",
                //              "match_mapping_type" : "string",
                //              "mapping" : { "type" : "keyword", "store" : "yes" }
                //          }
                //      }
                //  ]
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
                    DynamicTemplate template = DynamicTemplate.parse(templateName, templateParams, parserContext.indexVersionCreated());
                    if (template != null) {
                        validateDynamicTemplate(parserContext, template);
                        templates.add(template);
                    }
                }
                builder.dynamicTemplates(templates);
                return true;
            } else if (fieldName.equals("date_detection")) {
                builder.dateDetection = new Explicit<>(nodeBooleanValue(fieldNode, "date_detection"), true);
                return true;
            } else if (fieldName.equals("numeric_detection")) {
                builder.numericDetection = new Explicit<>(nodeBooleanValue(fieldNode, "numeric_detection"), true);
                return true;
            } else if (fieldName.equals("runtime")) {
                if (fieldNode instanceof Map) {
                    Map<String, RuntimeField> fields = RuntimeField.parseRuntimeFields(
                        (Map<String, Object>) fieldNode,
                        parserContext,
                        true
                    );
                    builder.setRuntime(fields);
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

    RootObjectMapper(String name, Explicit<Boolean> enabled, Dynamic dynamic, Map<String, Mapper> mappers,
                     Map<String, RuntimeField> runtimeFields,
                     Explicit<DateFormatter[]> dynamicDateTimeFormatters, Explicit<DynamicTemplate[]> dynamicTemplates,
                     Explicit<Boolean> dateDetection, Explicit<Boolean> numericDetection) {
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
    RootObjectMapper copyAndReset() {
        RootObjectMapper copy = (RootObjectMapper) super.copyAndReset();
        // for dynamic updates, no need to carry root-specific options, we just
        // set everything to their implicit default value so that they are not
        // applied at merge time
        copy.dynamicTemplates = new Explicit<>(new DynamicTemplate[0], false);
        copy.dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        copy.dateDetection = new Explicit<>(Defaults.DATE_DETECTION, false);
        copy.numericDetection = new Explicit<>(Defaults.NUMERIC_DETECTION, false);
        //also no need to carry the already defined runtime fields, only new ones need to be added
        copy.runtimeFields.clear();
        return copy;
    }

    boolean dateDetection() {
        return this.dateDetection.value();
    }

    boolean numericDetection() {
        return this.numericDetection.value();
    }

    DateFormatter[] dynamicDateTimeFormatters() {
        return dynamicDateTimeFormatters.value();
    }

    DynamicTemplate[] dynamicTemplates() {
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

    void addRuntimeFields(Collection<RuntimeField> runtimeFields) {
        for (RuntimeField runtimeField : runtimeFields) {
            this.runtimeFields.put(runtimeField.name(), runtimeField);
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
            List<RuntimeField> sortedRuntimeFields = runtimeFields.values().stream().sorted(
                Comparator.comparing(RuntimeField::name)).collect(Collectors.toList());
            for (RuntimeField fieldType : sortedRuntimeFields) {
                fieldType.toXContent(builder, params);
            }
            builder.endObject();
        }
    }

    private static void validateDynamicTemplate(MappingParserContext parserContext,
                                                DynamicTemplate template) {

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
                    validate(template, dynamicType,
                        (name, mapping) -> typeParser.parse(name, mapping, parserContext).build(new ContentPath(1)));
                }
                lastError = null; // ok, the template is valid for at least one type
                break;
            } catch (Exception e) {
                lastError = e;
            }
        }
        final boolean shouldEmitDeprecationWarning = parserContext.indexVersionCreated().onOrAfter(Version.V_7_7_0);
        if (lastError != null && shouldEmitDeprecationWarning) {
            String format = "dynamic template [%s] has invalid content [%s], " +
                "attempted to validate it with the following match_mapping_type: %s, caused by [%s]";
            String message = String.format(Locale.ROOT, format,
                template.getName(), Strings.toString(template), Arrays.toString(types), lastError.getMessage());
            DEPRECATION_LOGGER.deprecate(DeprecationCategory.TEMPLATES, "invalid_dynamic_template", message);
        }
    }

    private static void validate(DynamicTemplate template,
                                 String dynamicType,
                                 BiConsumer<String, Map<String, Object>> mappingConsumer) {
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
