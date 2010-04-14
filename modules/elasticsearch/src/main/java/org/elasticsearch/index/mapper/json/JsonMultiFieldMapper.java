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

package org.elasticsearch.index.mapper.json;

import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;
import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.index.mapper.json.JsonTypeParsers.*;
import static org.elasticsearch.util.MapBuilder.*;

/**
 * @author kimchy (shay.banon)
 */
public class JsonMultiFieldMapper implements JsonMapper, JsonIncludeInAllMapper {

    public static final String JSON_TYPE = "multi_field";

    public static class Defaults {
        public static final JsonPath.Type PATH_TYPE = JsonPath.Type.FULL;
    }

    public static class Builder extends JsonMapper.Builder<Builder, JsonMultiFieldMapper> {

        private JsonPath.Type pathType = Defaults.PATH_TYPE;

        private final List<JsonMapper.Builder> mappersBuilders = newArrayList();

        private JsonMapper.Builder defaultMapperBuilder;

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder pathType(JsonPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder add(JsonMapper.Builder builder) {
            if (builder.name.equals(name)) {
                defaultMapperBuilder = builder;
            } else {
                mappersBuilders.add(builder);
            }
            return this;
        }

        @Override public JsonMultiFieldMapper build(BuilderContext context) {
            JsonPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            JsonMapper defaultMapper = null;
            if (defaultMapperBuilder != null) {
                defaultMapper = defaultMapperBuilder.build(context);
            }

            context.path().add(name);
            Map<String, JsonMapper> mappers = new HashMap<String, JsonMapper>();
            for (JsonMapper.Builder builder : mappersBuilders) {
                JsonMapper mapper = builder.build(context);
                mappers.put(mapper.name(), mapper);
            }
            context.path().remove();

            context.path().pathType(origPathType);

            return new JsonMultiFieldMapper(name, pathType, mappers, defaultMapper);
        }
    }

    public static class TypeParser implements JsonTypeParser {
        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode multiFieldNode = (ObjectNode) node;
            JsonMultiFieldMapper.Builder builder = multiField(name);
            for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = multiFieldNode.getFields(); fieldsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = fieldsIt.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                JsonNode fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.getValueAsText()));
                } else if (fieldName.equals("fields")) {
                    ObjectNode fieldsNode = (ObjectNode) fieldNode;
                    for (Iterator<Map.Entry<String, JsonNode>> propsIt = fieldsNode.getFields(); propsIt.hasNext();) {
                        Map.Entry<String, JsonNode> entry1 = propsIt.next();
                        String propName = entry1.getKey();
                        JsonNode propNode = entry1.getValue();

                        String type;
                        JsonNode typeNode = propNode.get("type");
                        if (typeNode != null) {
                            type = typeNode.getTextValue();
                        } else {
                            throw new MapperParsingException("No type specified for property [" + propName + "]");
                        }

                        JsonTypeParser typeParser = parserContext.typeParser(type);
                        if (typeParser == null) {
                            throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + fieldName + "]");
                        }
                        builder.add(typeParser.parse(propName, propNode, parserContext));
                    }
                }
            }
            return builder;
        }
    }

    private final String name;

    private final JsonPath.Type pathType;

    private final Object mutex = new Object();

    private volatile ImmutableMap<String, JsonMapper> mappers = ImmutableMap.of();

    private volatile JsonMapper defaultMapper;

    public JsonMultiFieldMapper(String name, JsonPath.Type pathType, JsonMapper defaultMapper) {
        this(name, pathType, new HashMap<String, JsonMapper>(), defaultMapper);
    }

    public JsonMultiFieldMapper(String name, JsonPath.Type pathType, Map<String, JsonMapper> mappers, JsonMapper defaultMapper) {
        this.name = name;
        this.pathType = pathType;
        this.mappers = ImmutableMap.copyOf(mappers);
        this.defaultMapper = defaultMapper;

        // we disable the all in mappers, only the default one can be added
        for (JsonMapper mapper : mappers.values()) {
            if (mapper instanceof JsonIncludeInAllMapper) {
                ((JsonIncludeInAllMapper) mapper).includeInAll(false);
            }
        }
    }

    @Override public String name() {
        return this.name;
    }

    @Override public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null && defaultMapper != null && (defaultMapper instanceof JsonIncludeInAllMapper)) {
            ((JsonIncludeInAllMapper) defaultMapper).includeInAll(includeInAll);
        }
    }

    public JsonPath.Type pathType() {
        return pathType;
    }

    public JsonMapper defaultMapper() {
        return this.defaultMapper;
    }

    public ImmutableMap<String, JsonMapper> mappers() {
        return this.mappers;
    }

    @Override public void parse(JsonParseContext jsonContext) throws IOException {
        JsonPath.Type origPathType = jsonContext.path().pathType();
        jsonContext.path().pathType(pathType);

        // do the default mapper without adding the path
        if (defaultMapper != null) {
            defaultMapper.parse(jsonContext);
        }

        jsonContext.path().add(name);
        for (JsonMapper mapper : mappers.values()) {
            mapper.parse(jsonContext);
        }
        jsonContext.path().remove();

        jsonContext.path().pathType(origPathType);
    }

    @Override public void merge(JsonMapper mergeWith, JsonMergeContext mergeContext) throws MergeMappingException {
        if (!(mergeWith instanceof JsonMultiFieldMapper)) {
            mergeContext.addConflict("Can't merge a non multi_field mapping [" + mergeWith.name() + "] with a multi_field mapping [" + name() + "]");
            return;
        }
        JsonMultiFieldMapper mergeWithMultiField = (JsonMultiFieldMapper) mergeWith;
        synchronized (mutex) {
            // merge the default mapper
            if (defaultMapper == null) {
                if (mergeWithMultiField.defaultMapper != null) {
                    if (!mergeContext.mergeFlags().simulate()) {
                        defaultMapper = mergeWithMultiField.defaultMapper;
                        mergeContext.docMapper().addFieldMapper((FieldMapper) defaultMapper);
                    }
                }
            } else {
                if (mergeWithMultiField.defaultMapper != null) {
                    defaultMapper.merge(mergeWithMultiField.defaultMapper, mergeContext);
                }
            }

            // merge all the other mappers
            for (JsonMapper mergeWithMapper : mergeWithMultiField.mappers.values()) {
                JsonMapper mergeIntoMapper = mappers.get(mergeWithMapper.name());
                if (mergeIntoMapper == null) {
                    // no mapping, simply add it if not simulating
                    if (!mergeContext.mergeFlags().simulate()) {
                        // disable the mapper from being in all, only the default mapper is in all
                        if (mergeWithMapper instanceof JsonIncludeInAllMapper) {
                            ((JsonIncludeInAllMapper) mergeWithMapper).includeInAll(false);
                        }
                        mappers = newMapBuilder(mappers).put(mergeWithMapper.name(), mergeWithMapper).immutableMap();
                        if (mergeWithMapper instanceof JsonFieldMapper) {
                            mergeContext.docMapper().addFieldMapper((FieldMapper) mergeWithMapper);
                        }
                    }
                } else {
                    mergeIntoMapper.merge(mergeWithMapper, mergeContext);
                }
            }
        }
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        if (defaultMapper != null) {
            defaultMapper.traverse(fieldMapperListener);
        }
        for (JsonMapper mapper : mappers.values()) {
            mapper.traverse(fieldMapperListener);
        }
    }

    @Override public void toJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", JSON_TYPE);
        builder.field("path", pathType.name().toLowerCase());

        builder.startObject("fields");
        if (defaultMapper != null) {
            defaultMapper.toJson(builder, params);
        }
        for (JsonMapper mapper : mappers.values()) {
            mapper.toJson(builder, params);
        }
        builder.endObject();

        builder.endObject();
    }
}
