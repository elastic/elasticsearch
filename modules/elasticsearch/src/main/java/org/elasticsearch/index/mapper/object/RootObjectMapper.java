/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.object;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DateFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;
import static org.elasticsearch.index.mapper.core.TypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class RootObjectMapper extends ObjectMapper {

    public static class Defaults {
        public static final FormatDateTimeFormatter[] DATE_TIME_FORMATTERS =
                new FormatDateTimeFormatter[]{
                        DateFieldMapper.Defaults.DATE_TIME_FORMATTER,
                        Joda.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd")
                };
        public static final boolean DATE_DETECTION = true;
    }

    public static class Builder extends ObjectMapper.Builder<Builder, RootObjectMapper> {

        protected final List<DynamicTemplate> dynamicTemplates = newArrayList();

        // we use this to filter out seen date formats, because we might get duplicates during merging
        protected Set<String> seenDateFormats = Sets.newHashSet();
        protected List<FormatDateTimeFormatter> dateTimeFormatters = newArrayList();

        protected boolean dateDetection = Defaults.DATE_DETECTION;

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder noDateTimeFormatter() {
            this.dateTimeFormatters = null;
            return builder;
        }

        public Builder dateTimeFormatter(Iterable<FormatDateTimeFormatter> dateTimeFormatters) {
            for (FormatDateTimeFormatter dateTimeFormatter : dateTimeFormatters) {
                if (!seenDateFormats.contains(dateTimeFormatter.format())) {
                    seenDateFormats.add(dateTimeFormatter.format());
                    this.dateTimeFormatters.add(dateTimeFormatter);
                }
            }
            return builder;
        }

        public Builder add(DynamicTemplate dynamicTemplate) {
            this.dynamicTemplates.add(dynamicTemplate);
            return this;
        }

        public Builder add(DynamicTemplate... dynamicTemplate) {
            for (DynamicTemplate template : dynamicTemplate) {
                this.dynamicTemplates.add(template);
            }
            return this;
        }


        @Override protected ObjectMapper createMapper(String name, String fullPath, boolean enabled, Nested nested, Dynamic dynamic, ContentPath.Type pathType, Map<String, Mapper> mappers) {
            assert !nested.isNested();
            FormatDateTimeFormatter[] dates = null;
            if (dateTimeFormatters == null) {
                dates = new FormatDateTimeFormatter[0];
            } else if (dateTimeFormatters.isEmpty()) {
                // add the default one
                dates = Defaults.DATE_TIME_FORMATTERS;
            } else {
                dates = dateTimeFormatters.toArray(new FormatDateTimeFormatter[dateTimeFormatters.size()]);
            }
            // root dynamic must not be null, since its the default
            if (dynamic == null) {
                dynamic = Dynamic.TRUE;
            }
            return new RootObjectMapper(name, enabled, dynamic, pathType, mappers,
                    dates,
                    dynamicTemplates.toArray(new DynamicTemplate[dynamicTemplates.size()]),
                    dateDetection);
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {

        @Override protected ObjectMapper.Builder createBuilder(String name) {
            return new Builder(name);
        }

        @Override protected void processField(ObjectMapper.Builder builder, String fieldName, Object fieldNode) {
            if (fieldName.equals("date_formats")) {
                List<FormatDateTimeFormatter> dateTimeFormatters = newArrayList();
                if (fieldNode instanceof List) {
                    for (Object node1 : (List) fieldNode) {
                        dateTimeFormatters.add(parseDateTimeFormatter(fieldName, node1));
                    }
                } else if ("none".equals(fieldNode.toString())) {
                    dateTimeFormatters = null;
                } else {
                    dateTimeFormatters.add(parseDateTimeFormatter(fieldName, fieldNode));
                }
                if (dateTimeFormatters == null) {
                    ((Builder) builder).noDateTimeFormatter();
                } else {
                    ((Builder) builder).dateTimeFormatter(dateTimeFormatters);
                }
            } else if (fieldName.equals("dynamic_templates")) {
                //  "dynamic_templates" : [
                //      {
                //          "template_1" : {
                //              "match" : "*_test",
                //              "match_mapping_type" : "string",
                //              "mapping" : { "type" : "string", "store" : "yes" }
                //          }
                //      }
                //  ]
                List tmplNodes = (List) fieldNode;
                for (Object tmplNode : tmplNodes) {
                    Map<String, Object> tmpl = (Map<String, Object>) tmplNode;
                    if (tmpl.size() != 1) {
                        throw new MapperParsingException("A dynamic template must be defined with a name");
                    }
                    Map.Entry<String, Object> entry = tmpl.entrySet().iterator().next();
                    ((Builder) builder).add(DynamicTemplate.parse(entry.getKey(), (Map<String, Object>) entry.getValue()));
                }
            } else if (fieldName.equals("date_detection")) {
                ((Builder) builder).dateDetection = nodeBooleanValue(fieldNode);
            }
        }
    }

    private final FormatDateTimeFormatter[] dateTimeFormatters;

    private final boolean dateDetection;

    private volatile DynamicTemplate dynamicTemplates[];

    RootObjectMapper(String name, boolean enabled, Dynamic dynamic, ContentPath.Type pathType, Map<String, Mapper> mappers,
                     FormatDateTimeFormatter[] dateTimeFormatters, DynamicTemplate dynamicTemplates[], boolean dateDetection) {
        super(name, name, enabled, Nested.NO, dynamic, pathType, mappers);
        this.dynamicTemplates = dynamicTemplates;
        this.dateTimeFormatters = dateTimeFormatters;
        this.dateDetection = dateDetection;
    }

    public boolean dateDetection() {
        return this.dateDetection;
    }

    public FormatDateTimeFormatter[] dateTimeFormatters() {
        return dateTimeFormatters;
    }

    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, String dynamicType) {
        DynamicTemplate dynamicTemplate = findTemplate(context.path(), name, dynamicType);
        if (dynamicTemplate == null) {
            return null;
        }
        Mapper.TypeParser.ParserContext parserContext = context.docMapperParser().parserContext();
        return parserContext.typeParser(dynamicTemplate.mappingType(dynamicType)).parse(name, dynamicTemplate.mappingForName(name, dynamicType), parserContext);
    }

    public DynamicTemplate findTemplate(ContentPath path, String name, String dynamicType) {
        for (DynamicTemplate dynamicTemplate : dynamicTemplates) {
            if (dynamicTemplate.match(path, name, dynamicType)) {
                return dynamicTemplate;
            }
        }
        return null;
    }

    @Override protected void doMerge(ObjectMapper mergeWith, MergeContext mergeContext) {
        RootObjectMapper mergeWithObject = (RootObjectMapper) mergeWith;
        if (!mergeContext.mergeFlags().simulate()) {
            // merge them
            List<DynamicTemplate> mergedTemplates = Lists.newArrayList(Arrays.asList(this.dynamicTemplates));
            for (DynamicTemplate template : mergeWithObject.dynamicTemplates) {
                boolean replaced = false;
                for (int i = 0; i < mergedTemplates.size(); i++) {
                    if (mergedTemplates.get(i).name().equals(template.name())) {
                        mergedTemplates.set(i, template);
                        replaced = true;
                    }
                }
                if (!replaced) {
                    mergedTemplates.add(template);
                }
            }
            this.dynamicTemplates = mergedTemplates.toArray(new DynamicTemplate[mergedTemplates.size()]);
        }
    }

    @Override protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (dateTimeFormatters != Defaults.DATE_TIME_FORMATTERS) {
            if (dateTimeFormatters.length > 0) {
                builder.startArray("date_formats");
                for (FormatDateTimeFormatter dateTimeFormatter : dateTimeFormatters) {
                    builder.value(dateTimeFormatter.format());
                }
                builder.endArray();
            }
        }

        if (dynamicTemplates != null && dynamicTemplates.length > 0) {
            builder.startArray("dynamic_templates");
            for (DynamicTemplate dynamicTemplate : dynamicTemplates) {
                builder.startObject();
                builder.field(dynamicTemplate.name());
                builder.map(dynamicTemplate.conf());
                builder.endObject();
            }
            builder.endArray();
        }

        if (dateDetection != Defaults.DATE_DETECTION) {
            builder.field("date_detection", dateDetection);
        }
    }
}
