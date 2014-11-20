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

package org.elasticsearch.index.mapper.object;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;

import static org.elasticsearch.index.mapper.MapperBuilders.*;

public class DynamicValueMapperLookup {

    public static Mapper getMapper(final ParseContext context, String currentFieldName, XContentParser.Token token) throws IOException {
        Mapper mapper = null;
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings(), context.path());
        if (token == XContentParser.Token.VALUE_STRING) {
            boolean resolved = false;

            // do a quick test to see if its fits a dynamic template, if so, use it.
            // we need to do it here so we can handle things like attachment templates, where calling
            // text (to see if its a date) causes the binary value to be cleared
            if (!resolved) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "string", null);
                if (builder != null) {
                    mapper = builder.build(builderContext);
                    resolved = true;
                }
            }

            if (!resolved && context.root().dateDetection()) {
                String text = context.parser().text();
                // a safe check since "1" gets parsed as well
                if (Strings.countOccurrencesOf(text, ":") > 1 || Strings.countOccurrencesOf(text, "-") > 1 || Strings.countOccurrencesOf(text, "/") > 1) {
                    for (FormatDateTimeFormatter dateTimeFormatter : context.root().dynamicDateTimeFormatters()) {
                        try {
                            dateTimeFormatter.parser().parseMillis(text);
                            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "date");
                            if (builder == null) {
                                builder = dateField(currentFieldName).dateTimeFormatter(dateTimeFormatter);
                            }
                            mapper = builder.build(builderContext);
                            resolved = true;
                            break;
                        } catch (Exception e) {
                            // failure to parse this, continue
                        }
                    }
                }
            }
            if (!resolved && context.root().numericDetection()) {
                String text = context.parser().text();
                try {
                    Long.parseLong(text);
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                    if (builder == null) {
                        builder = longField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                    resolved = true;
                } catch (Exception e) {
                    // not a long number
                }
                if (!resolved) {
                    try {
                        Double.parseDouble(text);
                        Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                        if (builder == null) {
                            builder = doubleField(currentFieldName);
                        }
                        mapper = builder.build(builderContext);
                        resolved = true;
                    } catch (Exception e) {
                        // not a long number
                    }
                }
            }
            if (!resolved) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "string");
                if (builder == null) {
                    builder = stringField(currentFieldName);
                }
                mapper = builder.build(builderContext);
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = context.parser().numberType();
            if (numberType == XContentParser.NumberType.INT) {
                if (context.parser().estimatedNumberType()) {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                    if (builder == null) {
                        builder = longField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                } else {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "integer");
                    if (builder == null) {
                        builder = integerField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                }
            } else if (numberType == XContentParser.NumberType.LONG) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                if (builder == null) {
                    builder = longField(currentFieldName);
                }
                mapper = builder.build(builderContext);
            } else if (numberType == XContentParser.NumberType.FLOAT) {
                if (context.parser().estimatedNumberType()) {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                    if (builder == null) {
                        builder = doubleField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                } else {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "float");
                    if (builder == null) {
                        builder = floatField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                }
            } else if (numberType == XContentParser.NumberType.DOUBLE) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                if (builder == null) {
                    builder = doubleField(currentFieldName);
                }
                mapper = builder.build(builderContext);
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "boolean");
            if (builder == null) {
                builder = booleanField(currentFieldName);
            }
            mapper = builder.build(builderContext);
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "binary");
            if (builder == null) {
                builder = binaryField(currentFieldName);
            }
            mapper = builder.build(builderContext);
        } else if (token == XContentParser.Token.START_OBJECT){
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "object");
            if(builder == null){
                builder = MapperBuilders.object(currentFieldName).enabled(true).pathType(context.path().pathType());
            }
            mapper = builder.build(builderContext);
        } else {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, null);
            if (builder != null) {
                mapper = builder.build(builderContext);
            } else {
                // TODO how do we identify dynamically that its a binary value?
                throw new ElasticsearchIllegalStateException("Can't handle serializing a dynamic type with content token [" + token + "] and field name [" + currentFieldName + "]");
            }
        }
        return mapper;
    }
}
