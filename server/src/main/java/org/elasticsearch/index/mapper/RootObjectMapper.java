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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseDateTimeFormatter;

public class RootObjectMapper extends ObjectMapper {

    public static class Defaults {
        public static final DateFormatter[] DYNAMIC_DATE_TIME_FORMATTERS =
                new DateFormatter[]{
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                        DateFormatter.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis")
                };
        public static final boolean DATE_DETECTION = true;
        public static final boolean NUMERIC_DETECTION = false;
    }

    public static class Builder extends ObjectMapper.Builder<Builder, RootObjectMapper> {

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
            fixRedundantIncludes(this, true);
            return super.build(context);
        }

        /**
         * Removes redundant root includes in {@link ObjectMapper.Nested} trees to avoid duplicate
         * fields on the root mapper when {@code isIncludeInRoot} is {@code true} for a node that is
         * itself included into a parent node, for which either {@code isIncludeInRoot} is
         * {@code true} or which is transitively included in root by a chain of nodes with
         * {@code isIncludeInParent} returning {@code true}.
         * @param omb Builder whose children to check.
         * @param parentIncluded True iff node is a child of root or a node that is included in
         * root
         */
        @SuppressWarnings("rawtypes")
        private static void fixRedundantIncludes(ObjectMapper.Builder omb, boolean parentIncluded) {
            for (Object mapper : omb.mappersBuilders) {
                if (mapper instanceof ObjectMapper.Builder) {
                    ObjectMapper.Builder child = (ObjectMapper.Builder) mapper;
                    Nested nested = child.nested;
                    boolean isNested = nested.isNested();
                    boolean includeInRootViaParent = parentIncluded && isNested && nested.isIncludeInParent();
                    boolean includedInRoot = isNested && nested.isIncludeInRoot();
                    if (includeInRootViaParent && includedInRoot) {
                        child.nested = Nested.newNested(true, false);
                    }
                    fixRedundantIncludes(child, includeInRootViaParent || includedInRoot);
                }
            }
        }

        @Override
        protected ObjectMapper createMapper(String name, String fullPath, boolean enabled, Nested nested, Dynamic dynamic,
                Map<String, Mapper> mappers, @Nullable Settings settings) {
            assert !nested.isNested();
            return new RootObjectMapper(name, enabled, dynamic, mappers,
                    dynamicDateTimeFormatters,
                    dynamicTemplates,
                    dateDetection, numericDetection, settings);
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
                        || processField(builder, fieldName, fieldNode, parserContext.indexVersionCreated())) {
                    iterator.remove();
                }
            }
            return builder;
        }

        @SuppressWarnings("unchecked")
        protected boolean processField(RootObjectMapper.Builder builder, String fieldName, Object fieldNode,
                                       Version indexVersionCreated) {
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
                              "mapping" : { "type" : "string", "store" : "yes" }
                          }
                      }
                  ]
                */
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

    RootObjectMapper(String name, boolean enabled, Dynamic dynamic, Map<String, Mapper> mappers,
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

    @SuppressWarnings("rawtypes")
    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, XContentFieldType matchType) {
        return findTemplateBuilder(context, name, matchType.defaultMappingType(), matchType);
    }

    /**
     * Find a template. Returns {@code null} if no template could be found.
     * @param name        the field name
     * @param dynamicType the field type to give the field if the template does not define one
     * @param matchType   the type of the field in the json document or null if unknown
     * @return a mapper builder, or null if there is no template for such a field
     */
    @SuppressWarnings("rawtypes")
    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, String dynamicType, XContentFieldType matchType) {
        DynamicTemplate dynamicTemplate = findTemplate(context.path(), name, matchType);
        if (dynamicTemplate == null) {
            return null;
        }
        Mapper.TypeParser.ParserContext parserContext = context.docMapperParser().parserContext();
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
    public RootObjectMapper merge(Mapper mergeWith) {
        return (RootObjectMapper) super.merge(mergeWith);
    }

    @Override
    protected void doMerge(ObjectMapper mergeWith) {
        super.doMerge(mergeWith);
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
            this.dynamicTemplates = mergeWithObject.dynamicTemplates;
        }
    }

    @Override
    public RootObjectMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        return (RootObjectMapper) super.updateFieldType(fullNameToFieldType);
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
}
