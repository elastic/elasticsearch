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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.util.concurrent.ThreadSafe;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.common.collect.ImmutableMap.*;
import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.common.collect.MapBuilder.*;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;
import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public class ObjectMapper implements XContentMapper, IncludeInAllMapper {

    public static final String CONTENT_TYPE = "object";

    public static class Defaults {
        public static final boolean ENABLED = true;
        public static final Dynamic DYNAMIC = null; // not set, inherited from father
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static enum Dynamic {
        TRUE,
        FALSE,
        STRICT
    }

    public static class Builder<T extends Builder, Y extends ObjectMapper> extends XContentMapper.Builder<T, Y> {

        protected boolean enabled = Defaults.ENABLED;

        protected Dynamic dynamic = Defaults.DYNAMIC;

        protected ContentPath.Type pathType = Defaults.PATH_TYPE;

        protected Boolean includeInAll;

        protected final List<XContentMapper.Builder> mappersBuilders = newArrayList();

        public Builder(String name) {
            super(name);
            this.builder = (T) this;
        }

        public T enabled(boolean enabled) {
            this.enabled = enabled;
            return builder;
        }

        public T dynamic(Dynamic dynamic) {
            this.dynamic = dynamic;
            return builder;
        }

        public T pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return builder;
        }

        public T includeInAll(boolean includeInAll) {
            this.includeInAll = includeInAll;
            return builder;
        }

        public T add(XContentMapper.Builder builder) {
            mappersBuilders.add(builder);
            return this.builder;
        }

        @Override public Y build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);
            context.path().add(name);

            Map<String, XContentMapper> mappers = new HashMap<String, XContentMapper>();
            for (XContentMapper.Builder builder : mappersBuilders) {
                XContentMapper mapper = builder.build(context);
                mappers.put(mapper.name(), mapper);
            }
            ObjectMapper objectMapper = createMapper(name, enabled, dynamic, pathType, mappers);

            context.path().pathType(origPathType);
            context.path().remove();

            objectMapper.includeInAllIfNotSet(includeInAll);

            return (Y) objectMapper;
        }

        protected ObjectMapper createMapper(String name, boolean enabled, Dynamic dynamic, ContentPath.Type pathType, Map<String, XContentMapper> mappers) {
            return new ObjectMapper(name, enabled, dynamic, pathType, mappers);
        }
    }

    public static class TypeParser implements XContentMapper.TypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Map<String, Object> objectNode = node;
            ObjectMapper.Builder builder = createBuilder(name);

            for (Map.Entry<String, Object> entry : objectNode.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();

                if (fieldName.equals("dynamic")) {
                    String value = fieldNode.toString();
                    if (value.equalsIgnoreCase("strict")) {
                        builder.dynamic(Dynamic.STRICT);
                    } else {
                        builder.dynamic(nodeBooleanValue(fieldNode) ? Dynamic.TRUE : Dynamic.FALSE);
                    }
                } else if (fieldName.equals("type")) {
                    String type = fieldNode.toString();
                    if (!type.equals("object")) {
                        throw new MapperParsingException("Trying to parse an object but has a different type [" + type + "] for [" + name + "]");
                    }
                } else if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                } else if (fieldName.equals("properties")) {
                    parseProperties(builder, (Map<String, Object>) fieldNode, parserContext);
                } else if (fieldName.equals("include_in_all")) {
                    builder.includeInAll(nodeBooleanValue(fieldNode));
                } else {
                    processField(builder, fieldName, fieldNode);
                }
            }
            return builder;
        }

        private void parseProperties(ObjectMapper.Builder objBuilder, Map<String, Object> propsNode, ParserContext parserContext) {
            for (Map.Entry<String, Object> entry : propsNode.entrySet()) {
                String propName = entry.getKey();
                Map<String, Object> propNode = (Map<String, Object>) entry.getValue();

                String type;
                Object typeNode = propNode.get("type");
                if (typeNode != null) {
                    type = typeNode.toString();
                } else {
                    // lets see if we can derive this...
                    if (propNode.get("properties") != null) {
                        type = ObjectMapper.CONTENT_TYPE;
                    } else if (propNode.get("fields") != null) {
                        type = MultiFieldMapper.CONTENT_TYPE;
                    } else {
                        throw new MapperParsingException("No type specified for property [" + propName + "]");
                    }
                }

                XContentMapper.TypeParser typeParser = parserContext.typeParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + propName + "]");
                }
                objBuilder.add(typeParser.parse(propName, propNode, parserContext));
            }
        }

        protected Builder createBuilder(String name) {
            return object(name);
        }

        protected void processField(Builder builder, String fieldName, Object fieldNode) {

        }
    }

    private final String name;

    private final boolean enabled;

    private final Dynamic dynamic;

    private final ContentPath.Type pathType;

    private Boolean includeInAll;

    private volatile ImmutableMap<String, XContentMapper> mappers = ImmutableMap.of();

    private final Object mutex = new Object();

    protected ObjectMapper(String name) {
        this(name, Defaults.ENABLED, Defaults.DYNAMIC, Defaults.PATH_TYPE);
    }


    protected ObjectMapper(String name, boolean enabled, Dynamic dynamic, ContentPath.Type pathType) {
        this(name, enabled, dynamic, pathType, null);
    }

    ObjectMapper(String name, boolean enabled, Dynamic dynamic, ContentPath.Type pathType, Map<String, XContentMapper> mappers) {
        this.name = name;
        this.enabled = enabled;
        this.dynamic = dynamic;
        this.pathType = pathType;
        if (mappers != null) {
            this.mappers = copyOf(mappers);
        }
    }

    @Override public String name() {
        return this.name;
    }

    @Override public void includeInAll(Boolean includeInAll) {
        if (includeInAll == null) {
            return;
        }
        this.includeInAll = includeInAll;
        // when called from outside, apply this on all the inner mappers
        for (XContentMapper mapper : mappers.values()) {
            if (mapper instanceof IncludeInAllMapper) {
                ((IncludeInAllMapper) mapper).includeInAll(includeInAll);
            }
        }
    }

    @Override public void includeInAllIfNotSet(Boolean includeInAll) {
        if (this.includeInAll == null) {
            this.includeInAll = includeInAll;
        }
        // when called from outside, apply this on all the inner mappers
        for (XContentMapper mapper : mappers.values()) {
            if (mapper instanceof IncludeInAllMapper) {
                ((IncludeInAllMapper) mapper).includeInAllIfNotSet(includeInAll);
            }
        }
    }

    public ObjectMapper putMapper(XContentMapper mapper) {
        if (mapper instanceof IncludeInAllMapper) {
            ((IncludeInAllMapper) mapper).includeInAllIfNotSet(includeInAll);
        }
        synchronized (mutex) {
            mappers = newMapBuilder(mappers).put(mapper.name(), mapper).immutableMap();
        }
        return this;
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        for (XContentMapper mapper : mappers.values()) {
            mapper.traverse(fieldMapperListener);
        }
    }

    public final Dynamic dynamic() {
        return this.dynamic;
    }

    public void parse(ParseContext context) throws IOException {
        if (!enabled) {
            context.parser().skipChildren();
            return;
        }
        XContentParser parser = context.parser();

        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);

        String currentFieldName = parser.currentName();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            // the object is null ("obj1" : null), simply bail
            return;
        }
        // if we are at the end of the previous object, advance
        if (token == XContentParser.Token.END_OBJECT) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            // if we are just starting an OBJECT, advance, this is the object we are parsing, we need the name first
            token = parser.nextToken();
        }
        while (token != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                serializeObject(context, currentFieldName);
            } else if (token == XContentParser.Token.START_ARRAY) {
                serializeArray(context, currentFieldName);
            } else if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NULL) {
                serializeNullValue(context, currentFieldName);
            } else if (token == null) {
                throw new MapperParsingException("object_mapper [" + name + "] tried to parse as object, but got EOF, has a concrete value been provided to it?");
            } else if (token.isValue()) {
                serializeValue(context, currentFieldName, token);
            }
            token = parser.nextToken();
        }
        // restore the enable path flag
        context.path().pathType(origPathType);
    }

    private void serializeNullValue(ParseContext context, String lastFieldName) throws IOException {
        // we can only handle null values if we have mappings for them
        XContentMapper mapper = mappers.get(lastFieldName);
        if (mapper != null) {
            mapper.parse(context);
        }
    }

    private void serializeObject(ParseContext context, String currentFieldName) throws IOException {
        context.path().add(currentFieldName);

        XContentMapper objectMapper = mappers.get(currentFieldName);
        if (objectMapper != null) {
            objectMapper.parse(context);
        } else {
            Dynamic dynamic = this.dynamic;
            if (dynamic == null) {
                dynamic = context.root().dynamic();
            }
            if (dynamic == Dynamic.STRICT) {
                throw new StrictDynamicMappingException(currentFieldName);
            } else if (dynamic == Dynamic.TRUE) {
                // we sync here just so we won't add it twice. Its not the end of the world
                // to sync here since next operations will get it before
                synchronized (mutex) {
                    objectMapper = mappers.get(currentFieldName);
                    if (objectMapper != null) {
                        objectMapper.parse(context);
                    } else {
                        XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "object");
                        if (builder == null) {
                            builder = XContentMapperBuilders.object(currentFieldName).enabled(true).dynamic(dynamic).pathType(pathType);
                        }
                        // remove the current field name from path, since the object builder adds it as well...
                        context.path().remove();
                        BuilderContext builderContext = new BuilderContext(context.path());
                        objectMapper = builder.build(builderContext);
                        putMapper(objectMapper);

                        // now re add it and parse...
                        context.path().add(currentFieldName);
                        objectMapper.parse(context);
                        context.addedMapper();
                    }
                }
            } else {
                // not dynamic, read everything up to end object
                context.parser().skipChildren();
            }
        }

        context.path().remove();
    }

    private void serializeArray(ParseContext context, String lastFieldName) throws IOException {
        XContentMapper mapper = mappers.get(lastFieldName);
        if (mapper != null && mapper instanceof ArrayValueMapperParser) {
            mapper.parse(context);
        } else {
            XContentParser parser = context.parser();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    serializeObject(context, lastFieldName);
                } else if (token == XContentParser.Token.START_ARRAY) {
                    serializeArray(context, lastFieldName);
                } else if (token == XContentParser.Token.FIELD_NAME) {
                    lastFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    serializeNullValue(context, lastFieldName);
                } else {
                    serializeValue(context, lastFieldName, token);
                }
            }
        }
    }

    private void serializeValue(final ParseContext context, String currentFieldName, XContentParser.Token token) throws IOException {
        XContentMapper mapper = mappers.get(currentFieldName);
        if (mapper != null) {
            mapper.parse(context);
            return;
        }
        Dynamic dynamic = this.dynamic;
        if (dynamic == null) {
            dynamic = context.root().dynamic();
        }
        if (dynamic == Dynamic.STRICT) {
            throw new StrictDynamicMappingException(currentFieldName);
        }
        if (dynamic == Dynamic.FALSE) {
            return;
        }
        // we sync here since we don't want to add this field twice to the document mapper
        // its not the end of the world, since we add it to the mappers once we create it
        // so next time we won't even get here for this field
        synchronized (mutex) {
            mapper = mappers.get(currentFieldName);
            if (mapper != null) {
                mapper.parse(context);
                return;
            }

            BuilderContext builderContext = new BuilderContext(context.path());
            if (token == XContentParser.Token.VALUE_STRING) {
                String text = context.parser().text();
                // check if it fits one of the date formats
                boolean resolved = false;
                // a safe check since "1" gets parsed as well
                if (text.contains(":") || text.contains("-") || text.contains("/")) {
                    for (FormatDateTimeFormatter dateTimeFormatter : context.root().dateTimeFormatters()) {
                        try {
                            dateTimeFormatter.parser().parseMillis(text);
                            XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "date");
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
                // DON'T do automatic ip detection logic, since it messes up with docs that have hosts and ips
                // check if its an ip
//                if (!resolved && text.indexOf('.') != -1) {
//                    try {
//                        IpFieldMapper.ipToLong(text);
//                        XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "ip");
//                        if (builder == null) {
//                            builder = ipField(currentFieldName);
//                        }
//                        mapper = builder.build(builderContext);
//                        resolved = true;
//                    } catch (Exception e) {
//                        // failure to parse, not ip...
//                    }
//                }
                if (!resolved) {
                    XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "string");
                    if (builder == null) {
                        builder = stringField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                XContentParser.NumberType numberType = context.parser().numberType();
                if (numberType == XContentParser.NumberType.INT) {
                    if (context.parser().estimatedNumberType()) {
                        XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                        if (builder == null) {
                            builder = longField(currentFieldName);
                        }
                        mapper = builder.build(builderContext);
                    } else {
                        XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "integer");
                        if (builder == null) {
                            builder = integerField(currentFieldName);
                        }
                        mapper = builder.build(builderContext);
                    }
                } else if (numberType == XContentParser.NumberType.LONG) {
                    XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                    if (builder == null) {
                        builder = longField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                } else if (numberType == XContentParser.NumberType.FLOAT) {
                    if (context.parser().estimatedNumberType()) {
                        XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                        if (builder == null) {
                            builder = doubleField(currentFieldName);
                        }
                        mapper = builder.build(builderContext);
                    } else {
                        XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "float");
                        if (builder == null) {
                            builder = floatField(currentFieldName);
                        }
                        mapper = builder.build(builderContext);
                    }
                } else if (numberType == XContentParser.NumberType.DOUBLE) {
                    XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                    if (builder == null) {
                        builder = doubleField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "boolean");
                if (builder == null) {
                    builder = booleanField(currentFieldName);
                }
                mapper = builder.build(builderContext);
            } else {
                XContentMapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, null);
                if (builder != null) {
                    mapper = builder.build(builderContext);
                } else {
                    // TODO how do we identify dynamically that its a binary value?
                    throw new ElasticSearchIllegalStateException("Can't handle serializing a dynamic type with content token [" + token + "] and field name [" + currentFieldName + "]");
                }
            }
            putMapper(mapper);
            mapper.traverse(new FieldMapperListener() {
                @Override public void fieldMapper(FieldMapper fieldMapper) {
                    context.docMapper().addFieldMapper(fieldMapper);
                }
            });

            mapper.parse(context);
            context.addedMapper();
        }
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        if (!(mergeWith instanceof ObjectMapper)) {
            mergeContext.addConflict("Can't merge a non object mapping [" + mergeWith.name() + "] with an object mapping [" + name() + "]");
            return;
        }
        ObjectMapper mergeWithObject = (ObjectMapper) mergeWith;

        doMerge(mergeWithObject, mergeContext);

        synchronized (mutex) {
            for (XContentMapper mergeWithMapper : mergeWithObject.mappers.values()) {
                XContentMapper mergeIntoMapper = mappers.get(mergeWithMapper.name());
                if (mergeIntoMapper == null) {
                    // no mapping, simply add it if not simulating
                    if (!mergeContext.mergeFlags().simulate()) {
                        putMapper(mergeWithMapper);
                        if (mergeWithMapper instanceof AbstractFieldMapper) {
                            mergeContext.docMapper().addFieldMapper((FieldMapper) mergeWithMapper);
                        }
                    }
                } else {
                    if ((mergeWithMapper instanceof MultiFieldMapper) && !(mergeIntoMapper instanceof MultiFieldMapper)) {
                        MultiFieldMapper mergeWithMultiField = (MultiFieldMapper) mergeWithMapper;
                        mergeWithMultiField.merge(mergeIntoMapper, mergeContext);
                        if (!mergeContext.mergeFlags().simulate()) {
                            putMapper(mergeWithMultiField);
                            // now, raise events for all mappers
                            for (XContentMapper mapper : mergeWithMultiField.mappers().values()) {
                                if (mapper instanceof AbstractFieldMapper) {
                                    mergeContext.docMapper().addFieldMapper((FieldMapper) mapper);
                                }
                            }
                        }
                    } else {
                        mergeIntoMapper.merge(mergeWithMapper, mergeContext);
                    }
                }
            }
        }
    }

    protected void doMerge(ObjectMapper mergeWith, MergeContext mergeContext) {

    }

    @Override public void close() {
        for (XContentMapper mapper : mappers.values()) {
            mapper.close();
        }
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        toXContent(builder, params, null, XContentMapper.EMPTY_ARRAY);
        return builder;
    }

    public void toXContent(XContentBuilder builder, Params params, ToXContent custom, XContentMapper... additionalMappers) throws IOException {
        builder.startObject(name);
        if (mappers.isEmpty()) { // only write the object content type if there are no properties, otherwise, it is automatically detected
            builder.field("type", CONTENT_TYPE);
        }
        // grr, ugly! on root, dynamic defaults to TRUE, on childs, it defaults to null to
        // inherit the root behavior
        if (this instanceof RootObjectMapper) {
            if (dynamic != Dynamic.TRUE) {
                builder.field("dynamic", dynamic.name().toLowerCase());
            }
        } else {
            if (dynamic != Defaults.DYNAMIC) {
                builder.field("dynamic", dynamic.name().toLowerCase());
            }
        }
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase());
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }

        if (custom != null) {
            custom.toXContent(builder, params);
        }

        doXContent(builder, params);

        // sort the mappers so we get consistent serialization format
        TreeMap<String, XContentMapper> sortedMappers = new TreeMap<String, XContentMapper>(mappers);

        // check internal mappers first (this is only relevant for root object)
        for (XContentMapper mapper : sortedMappers.values()) {
            if (mapper instanceof InternalMapper) {
                mapper.toXContent(builder, params);
            }
        }
        if (additionalMappers != null) {
            for (XContentMapper mapper : additionalMappers) {
                mapper.toXContent(builder, params);
            }
        }

        if (!mappers.isEmpty()) {
            builder.startObject("properties");
            for (XContentMapper mapper : sortedMappers.values()) {
                if (!(mapper instanceof InternalMapper)) {
                    mapper.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        builder.endObject();
    }

    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }
}
