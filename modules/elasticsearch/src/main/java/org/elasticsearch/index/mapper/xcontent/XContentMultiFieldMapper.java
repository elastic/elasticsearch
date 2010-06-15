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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.common.collect.MapBuilder.*;
import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentMultiFieldMapper implements XContentMapper, XContentIncludeInAllMapper {

    public static final String CONTENT_TYPE = "multi_field";

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static class Builder extends XContentMapper.Builder<Builder, XContentMultiFieldMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private final List<XContentMapper.Builder> mappersBuilders = newArrayList();

        private XContentMapper.Builder defaultMapperBuilder;

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder add(XContentMapper.Builder builder) {
            if (builder.name.equals(name)) {
                defaultMapperBuilder = builder;
            } else {
                mappersBuilders.add(builder);
            }
            return this;
        }

        @Override public XContentMultiFieldMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            XContentMapper defaultMapper = null;
            if (defaultMapperBuilder != null) {
                defaultMapper = defaultMapperBuilder.build(context);
            }

            context.path().add(name);
            Map<String, XContentMapper> mappers = new HashMap<String, XContentMapper>();
            for (XContentMapper.Builder builder : mappersBuilders) {
                XContentMapper mapper = builder.build(context);
                mappers.put(mapper.name(), mapper);
            }
            context.path().remove();

            context.path().pathType(origPathType);

            return new XContentMultiFieldMapper(name, pathType, mappers, defaultMapper);
        }
    }

    public static class TypeParser implements XContentTypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            XContentMultiFieldMapper.Builder builder = multiField(name);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                } else if (fieldName.equals("fields")) {
                    Map<String, Object> fieldsNode = (Map<String, Object>) fieldNode;
                    for (Map.Entry<String, Object> entry1 : fieldsNode.entrySet()) {
                        String propName = entry1.getKey();
                        Map<String, Object> propNode = (Map<String, Object>) entry1.getValue();

                        String type;
                        Object typeNode = propNode.get("type");
                        if (typeNode != null) {
                            type = typeNode.toString();
                        } else {
                            throw new MapperParsingException("No type specified for property [" + propName + "]");
                        }

                        XContentTypeParser typeParser = parserContext.typeParser(type);
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

    private final ContentPath.Type pathType;

    private final Object mutex = new Object();

    private volatile ImmutableMap<String, XContentMapper> mappers = ImmutableMap.of();

    private volatile XContentMapper defaultMapper;

    public XContentMultiFieldMapper(String name, ContentPath.Type pathType, XContentMapper defaultMapper) {
        this(name, pathType, new HashMap<String, XContentMapper>(), defaultMapper);
    }

    public XContentMultiFieldMapper(String name, ContentPath.Type pathType, Map<String, XContentMapper> mappers, XContentMapper defaultMapper) {
        this.name = name;
        this.pathType = pathType;
        this.mappers = ImmutableMap.copyOf(mappers);
        this.defaultMapper = defaultMapper;

        // we disable the all in mappers, only the default one can be added
        for (XContentMapper mapper : mappers.values()) {
            if (mapper instanceof XContentIncludeInAllMapper) {
                ((XContentIncludeInAllMapper) mapper).includeInAll(false);
            }
        }
    }

    @Override public String name() {
        return this.name;
    }

    @Override public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null && defaultMapper != null && (defaultMapper instanceof XContentIncludeInAllMapper)) {
            ((XContentIncludeInAllMapper) defaultMapper).includeInAll(includeInAll);
        }
    }

    public ContentPath.Type pathType() {
        return pathType;
    }

    public XContentMapper defaultMapper() {
        return this.defaultMapper;
    }

    public ImmutableMap<String, XContentMapper> mappers() {
        return this.mappers;
    }

    @Override public void parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);

        // do the default mapper without adding the path
        if (defaultMapper != null) {
            defaultMapper.parse(context);
        }

        context.path().add(name);
        for (XContentMapper mapper : mappers.values()) {
            mapper.parse(context);
        }
        context.path().remove();

        context.path().pathType(origPathType);
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        if (!(mergeWith instanceof XContentMultiFieldMapper)) {
            mergeContext.addConflict("Can't merge a non multi_field mapping [" + mergeWith.name() + "] with a multi_field mapping [" + name() + "]");
            return;
        }
        XContentMultiFieldMapper mergeWithMultiField = (XContentMultiFieldMapper) mergeWith;
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
            for (XContentMapper mergeWithMapper : mergeWithMultiField.mappers.values()) {
                XContentMapper mergeIntoMapper = mappers.get(mergeWithMapper.name());
                if (mergeIntoMapper == null) {
                    // no mapping, simply add it if not simulating
                    if (!mergeContext.mergeFlags().simulate()) {
                        // disable the mapper from being in all, only the default mapper is in all
                        if (mergeWithMapper instanceof XContentIncludeInAllMapper) {
                            ((XContentIncludeInAllMapper) mergeWithMapper).includeInAll(false);
                        }
                        mappers = newMapBuilder(mappers).put(mergeWithMapper.name(), mergeWithMapper).immutableMap();
                        if (mergeWithMapper instanceof XContentFieldMapper) {
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
        for (XContentMapper mapper : mappers.values()) {
            mapper.traverse(fieldMapperListener);
        }
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);
        builder.field("path", pathType.name().toLowerCase());

        builder.startObject("fields");
        if (defaultMapper != null) {
            defaultMapper.toXContent(builder, params);
        }
        for (XContentMapper mapper : mappers.values()) {
            mapper.toXContent(builder, params);
        }
        builder.endObject();

        builder.endObject();
    }
}
