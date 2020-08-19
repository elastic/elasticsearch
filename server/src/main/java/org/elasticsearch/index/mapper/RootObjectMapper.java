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

import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseDateTimeFormatter;

public class RootObjectMapper extends ObjectMapper {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RootObjectMapper.class);

    public static class Defaults {
        public static final DateFormatter[] DYNAMIC_DATE_TIME_FORMATTERS =
                new DateFormatter[]{
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                        DateFormatter.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis")
                };
        public static final boolean DATE_DETECTION = true;
        public static final boolean NUMERIC_DETECTION = false;
    }

    public static class Builder extends ObjectMapper.Builder<Builder> {

        protected Explicit<DynamicTemplate[]> dynamicTemplates = new Explicit<>(new DynamicTemplate[0], false);
        protected Explicit<DateFormatter[]> dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        protected Explicit<Boolean> dateDetection = new Explicit<>(Defaults.DATE_DETECTION, false);
        protected Explicit<Boolean> numericDetection = new Explicit<>(Defaults.NUMERIC_DETECTION, false);

        public Builder(String name) {
            super(name);
            this.builder = this;
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
        public RootObjectMapper build(BuilderContext context) {
            return (RootObjectMapper) super.build(context);
        }

        @Override
        protected ObjectMapper createMapper(String name, String fullPath, Explicit<Boolean> enabled, Nested nested, Dynamic dynamic,
                Map<String, Mapper> mappers, @Nullable Settings settings) {
            assert !nested.isNested();
            return new RootObjectMapper(name, enabled, dynamic, mappers,
                    dynamicDateTimeFormatters,
                    dynamicTemplates,
                    dateDetection, numericDetection, settings);
        }
    }

    /**
     * Removes redundant root includes in {@link ObjectMapper.Nested} trees to avoid duplicate
     * fields on the root mapper when {@code isIncludeInRoot} is {@code true} for a node that is
     * itself included into a parent node, for which either {@code isIncludeInRoot} is
     * {@code true} or which is transitively included in root by a chain of nodes with
     * {@code isIncludeInParent} returning {@code true}.
     */
    public void fixRedundantIncludes() {
       fixRedundantIncludes(this, true);
    }

    private static void fixRedundantIncludes(ObjectMapper objectMapper, boolean parentIncluded) {
        for (Mapper mapper : objectMapper) {
            if (mapper instanceof ObjectMapper) {
                ObjectMapper child = (ObjectMapper) mapper;
                Nested nested = child.nested();
                boolean isNested = nested.isNested();
                boolean includeInRootViaParent = parentIncluded && isNested && nested.isIncludeInParent();
                boolean includedInRoot = isNested && nested.isIncludeInRoot();
                if (includeInRootViaParent && includedInRoot) {
                    nested.setIncludeInParent(true);
                    nested.setIncludeInRoot(false);
                }
                fixRedundantIncludes(child, includeInRootViaParent || includedInRoot);
            }
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {

        @Override
        @SuppressWarnings("rawtypes")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {

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
        protected boolean processField(RootObjectMapper.Builder builder, String fieldName, Object fieldNode,
                                       ParserContext parserContext) {
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
            }
            return false;
        }
    }

    private Explicit<DateFormatter[]> dynamicDateTimeFormatters;
    private Explicit<Boolean> dateDetection;
    private Explicit<Boolean> numericDetection;
    private Explicit<DynamicTemplate[]> dynamicTemplates;

    RootObjectMapper(String name, Explicit<Boolean> enabled, Dynamic dynamic, Map<String, Mapper> mappers,
                     Explicit<DateFormatter[]> dynamicDateTimeFormatters, Explicit<DynamicTemplate[]> dynamicTemplates,
                     Explicit<Boolean> dateDetection, Explicit<Boolean> numericDetection, Settings settings) {
        super(name, name, enabled, Nested.NO, dynamic, mappers, settings);
        this.dynamicTemplates = dynamicTemplates;
        this.dynamicDateTimeFormatters = dynamicDateTimeFormatters;
        this.dateDetection = dateDetection;
        this.numericDetection = numericDetection;
    }

    @Override
    public ObjectMapper mappingUpdate(Mapper mapper) {
        RootObjectMapper update = (RootObjectMapper) super.mappingUpdate(mapper);
        // for dynamic updates, no need to carry root-specific options, we just
        // set everything to they implicit default value so that they are not
        // applied at merge time
        update.dynamicTemplates = new Explicit<>(new DynamicTemplate[0], false);
        update.dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        update.dateDetection = new Explicit<>(Defaults.DATE_DETECTION, false);
        update.numericDetection = new Explicit<>(Defaults.NUMERIC_DETECTION, false);
        return update;
    }

    public boolean dateDetection() {
        return this.dateDetection.value();
    }

    public boolean numericDetection() {
        return this.numericDetection.value();
    }

    public DateFormatter[] dynamicDateTimeFormatters() {
        return dynamicDateTimeFormatters.value();
    }

    public DynamicTemplate[] dynamicTemplates() {
        return dynamicTemplates.value();
    }

    @SuppressWarnings("rawtypes")
    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, XContentFieldType matchType) {
        return findTemplateBuilder(context, name, matchType, null);
    }

    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, DateFormatter dateFormatter) {
        return findTemplateBuilder(context, name, XContentFieldType.DATE, dateFormatter);
    }

    /**
     * Find a template. Returns {@code null} if no template could be found.
     * @param name        the field name
     * @param matchType   the type of the field in the json document or null if unknown
     * @param dateFormat  a dateformatter to use if the type is a date, null if not a date or is using the default format
     * @return a mapper builder, or null if there is no template for such a field
     */
    @SuppressWarnings("rawtypes")
    private Mapper.Builder findTemplateBuilder(ParseContext context, String name, XContentFieldType matchType, DateFormatter dateFormat) {
        DynamicTemplate dynamicTemplate = findTemplate(context.path(), name, matchType);
        if (dynamicTemplate == null) {
            return null;
        }
        String dynamicType = matchType.defaultMappingType();
        Mapper.TypeParser.ParserContext parserContext = context.docMapperParser().parserContext(dateFormat);
        String mappingType = dynamicTemplate.mappingType(dynamicType);
        Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
        if (typeParser == null) {
            throw new MapperParsingException("failed to find type parsed [" + mappingType + "] for [" + name + "]");
        }
        return typeParser.parse(name, dynamicTemplate.mappingForName(name, dynamicType), parserContext);
    }

    public DynamicTemplate findTemplate(ContentPath path, String name, XContentFieldType matchType) {
        final String pathAsString = path.pathAsText(name);
        for (DynamicTemplate dynamicTemplate : dynamicTemplates.value()) {
            if (dynamicTemplate.match(pathAsString, name, matchType)) {
                return dynamicTemplate;
            }
        }
        return null;
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
    }

    private static void validateDynamicTemplate(Mapper.TypeParser.ParserContext parserContext,
                                                DynamicTemplate dynamicTemplate) {

        if (containsSnippet(dynamicTemplate.getMapping(), "{name}")) {
            // Can't validate template, because field names can't be guessed up front.
            return;
        }

        final XContentFieldType[] types;
        if (dynamicTemplate.getXContentFieldType() != null) {
            types = new XContentFieldType[]{dynamicTemplate.getXContentFieldType()};
        } else {
            types = XContentFieldType.values();
        }

        Exception lastError = null;
        boolean dynamicTemplateInvalid = true;

        for (XContentFieldType contentFieldType : types) {
            String defaultDynamicType = contentFieldType.defaultMappingType();
            String mappingType = dynamicTemplate.mappingType(defaultDynamicType);
            Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
            if (typeParser == null) {
                lastError = new IllegalArgumentException("No mapper found for type [" + mappingType + "]");
                continue;
            }

            String templateName = "__dynamic__" + dynamicTemplate.name();
            Map<String, Object> fieldTypeConfig = dynamicTemplate.mappingForName(templateName, defaultDynamicType);
            try {
                Mapper.Builder<?> dummyBuilder = typeParser.parse(templateName, fieldTypeConfig, parserContext);
                fieldTypeConfig.remove("type");
                if (fieldTypeConfig.isEmpty()) {
                    Settings indexSettings = parserContext.mapperService().getIndexSettings().getSettings();
                    BuilderContext builderContext = new BuilderContext(indexSettings, new ContentPath(1));
                    dummyBuilder.build(builderContext);
                    dynamicTemplateInvalid = false;
                    break;
                } else {
                    lastError = new IllegalArgumentException("Unused mapping attributes [" + fieldTypeConfig + "]");
                }
            } catch (Exception e) {
                lastError = e;
            }
        }

        final boolean failInvalidDynamicTemplates = parserContext.indexVersionCreated().onOrAfter(Version.V_8_0_0);
        if (dynamicTemplateInvalid) {
            String message = String.format(Locale.ROOT, "dynamic template [%s] has invalid content [%s]",
                dynamicTemplate.getName(), Strings.toString(dynamicTemplate));
            if (failInvalidDynamicTemplates) {
                throw new IllegalArgumentException(message, lastError);
            } else {
                final String deprecationMessage;
                if (lastError != null) {
                     deprecationMessage = String.format(Locale.ROOT, "%s, caused by [%s]", message, lastError.getMessage());
                } else {
                    deprecationMessage = message;
                }
                DEPRECATION_LOGGER.deprecate("invalid_dynamic_template", deprecationMessage);
            }
        }
    }

    private static boolean containsSnippet(Map<?, ?> map, String snippet) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            if (key.contains(snippet)) {
                return true;
            }

            Object value = entry.getValue();
            if (value instanceof Map) {
                if (containsSnippet((Map<?, ?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof List) {
                if (containsSnippet((List<?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof String) {
                String valueString = (String) value;
                if (valueString.contains(snippet)) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean containsSnippet(List<?> list, String snippet) {
        for (Object value : list) {
            if (value instanceof Map) {
                if (containsSnippet((Map<?, ?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof List) {
                if (containsSnippet((List<?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof String) {
                String valueString = (String) value;
                if (valueString.contains(snippet)) {
                    return true;
                }
            }
        }
        return false;
    }
}
