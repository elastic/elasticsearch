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

package org.elasticsearch.index.mapper.xcontent;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class RootObjectMapper extends ObjectMapper {

    public static class Builder extends ObjectMapper.Builder<Builder, RootObjectMapper> {

        protected final List<DynamicTemplate> dynamicTemplates = newArrayList();

        public Builder(String name) {
            super(name);
            this.builder = this;
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


        @Override protected ObjectMapper createMapper(String name, boolean enabled, boolean dynamic, ContentPath.Type pathType, FormatDateTimeFormatter[] dateTimeFormatters, Map<String, XContentMapper> mappers) {
            return new RootObjectMapper(name, enabled, dynamic, pathType, dateTimeFormatters, mappers, dynamicTemplates.toArray(new DynamicTemplate[dynamicTemplates.size()]));
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {

        @Override protected ObjectMapper.Builder createBuilder(String name) {
            return new Builder(name);
        }

        @Override protected void processField(ObjectMapper.Builder builder, String fieldName, Object fieldNode) {
            if (fieldName.equals("dynamic_templates")) {
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
            }
        }
    }

    private volatile DynamicTemplate dynamicTemplates[];

    RootObjectMapper(String name, boolean enabled, boolean dynamic, ContentPath.Type pathType,
                     FormatDateTimeFormatter[] dateTimeFormatters, Map<String, XContentMapper> mappers, DynamicTemplate dynamicTemplates[]) {
        super(name, enabled, dynamic, pathType, dateTimeFormatters, mappers);
        this.dynamicTemplates = dynamicTemplates;
    }

    public XContentMapper.Builder findTemplateBuilder(ParseContext context, String name, String dynamicType) {
        DynamicTemplate dynamicTemplate = findTemplate(name, dynamicType);
        if (dynamicTemplate == null) {
            return null;
        }
        XContentMapper.TypeParser.ParserContext parserContext = context.docMapperParser().parserContext();
        return parserContext.typeParser(dynamicTemplate.mappingType(dynamicType)).parse(name, dynamicTemplate.mappingForName(name, dynamicType), parserContext);
    }

    public DynamicTemplate findTemplate(String name, String dynamicType) {
        for (DynamicTemplate dynamicTemplate : dynamicTemplates) {
            if (dynamicTemplate.match(name, dynamicType)) {
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

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
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
    }
}
