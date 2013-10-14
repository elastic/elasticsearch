/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.mapper.multifield;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.elasticsearch.index.mapper.MapperBuilders.multiField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;

/**
 *
 */
public class MultiFieldMapper implements Mapper, AllFieldMapper.IncludeInAll {

    public static final String CONTENT_TYPE = "multi_field";

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static class Builder extends Mapper.Builder<Builder, MultiFieldMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private final List<Mapper.Builder> mappersBuilders = newArrayList();

        private Mapper.Builder defaultMapperBuilder;

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder add(Mapper.Builder builder) {
            if (builder.name().equals(name)) {
                defaultMapperBuilder = builder;
            } else {
                mappersBuilders.add(builder);
            }
            return this;
        }

        @Override
        public MultiFieldMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            Mapper defaultMapper = null;
            if (defaultMapperBuilder != null) {
                defaultMapper = defaultMapperBuilder.build(context);
            }

            String origSourcePath = context.path().sourcePath(context.path().fullPathAsText(name));
            context.path().add(name);
            Map<String, Mapper> mappers = new HashMap<String, Mapper>();
            for (Mapper.Builder builder : mappersBuilders) {
                Mapper mapper = builder.build(context);
                mappers.put(mapper.name(), mapper);
            }
            context.path().remove();
            context.path().sourcePath(origSourcePath);

            context.path().pathType(origPathType);

            return new MultiFieldMapper(name, pathType, mappers, defaultMapper);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            MultiFieldMapper.Builder builder = multiField(name);

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

                        Mapper.TypeParser typeParser = parserContext.typeParser(type);
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

    private volatile ImmutableMap<String, Mapper> mappers = ImmutableMap.of();

    private volatile Mapper defaultMapper;

    public MultiFieldMapper(String name, ContentPath.Type pathType, Mapper defaultMapper) {
        this(name, pathType, new HashMap<String, Mapper>(), defaultMapper);
    }

    public MultiFieldMapper(String name, ContentPath.Type pathType, Map<String, Mapper> mappers, Mapper defaultMapper) {
        this.name = name;
        this.pathType = pathType;
        this.mappers = ImmutableMap.copyOf(mappers);
        this.defaultMapper = defaultMapper;

        // we disable the all in mappers, only the default one can be added
        for (Mapper mapper : mappers.values()) {
            if (mapper instanceof AllFieldMapper.IncludeInAll) {
                ((AllFieldMapper.IncludeInAll) mapper).includeInAll(false);
            }
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null && defaultMapper != null && (defaultMapper instanceof AllFieldMapper.IncludeInAll)) {
            ((AllFieldMapper.IncludeInAll) defaultMapper).includeInAll(includeInAll);
        }
    }

    @Override
    public void includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && defaultMapper != null && (defaultMapper instanceof AllFieldMapper.IncludeInAll)) {
            ((AllFieldMapper.IncludeInAll) defaultMapper).includeInAllIfNotSet(includeInAll);
        }
    }

    public ContentPath.Type pathType() {
        return pathType;
    }

    public Mapper defaultMapper() {
        return this.defaultMapper;
    }

    public ImmutableMap<String, Mapper> mappers() {
        return this.mappers;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);

        // do the default mapper without adding the path
        if (defaultMapper != null) {
            defaultMapper.parse(context);
        }

        context.path().add(name);
        for (Mapper mapper : mappers.values()) {
            mapper.parse(context);
        }
        context.path().remove();

        context.path().pathType(origPathType);
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        if (!(mergeWith instanceof MultiFieldMapper) && !(mergeWith instanceof AbstractFieldMapper)) {
            mergeContext.addConflict("Can't merge a non multi_field / non simple mapping [" + mergeWith.name() + "] with a multi_field mapping [" + name() + "]");
            return;
        }
        synchronized (mutex) {
            if (mergeWith instanceof AbstractFieldMapper) {
                // its a single field mapper, upgraded into a multi field mapper, just update the default mapper
                if (defaultMapper == null) {
                    if (!mergeContext.mergeFlags().simulate()) {
                        mergeContext.docMapper().addFieldMappers((FieldMapper) defaultMapper);
                        defaultMapper = mergeWith; // only set & expose it after adding fieldmapper
                    }
                }
            } else {
                MultiFieldMapper mergeWithMultiField = (MultiFieldMapper) mergeWith;

                List<FieldMapper> newFieldMappers = null;
                MapBuilder<String, Mapper> newMappersBuilder = null;
                Mapper newDefaultMapper = null;
                // merge the default mapper
                if (defaultMapper == null) {
                    if (mergeWithMultiField.defaultMapper != null) {
                        if (!mergeContext.mergeFlags().simulate()) {
                            if (newFieldMappers == null) {
                                newFieldMappers = new ArrayList<FieldMapper>();
                            }
                            newFieldMappers.add((FieldMapper) defaultMapper);
                            newDefaultMapper = mergeWithMultiField.defaultMapper;
                        }
                    }
                } else {
                    if (mergeWithMultiField.defaultMapper != null) {
                        defaultMapper.merge(mergeWithMultiField.defaultMapper, mergeContext);
                    }
                }

                // merge all the other mappers
                for (Mapper mergeWithMapper : mergeWithMultiField.mappers.values()) {
                    Mapper mergeIntoMapper = mappers.get(mergeWithMapper.name());
                    if (mergeIntoMapper == null) {
                        // no mapping, simply add it if not simulating
                        if (!mergeContext.mergeFlags().simulate()) {
                            // disable the mapper from being in all, only the default mapper is in all
                            if (mergeWithMapper instanceof AllFieldMapper.IncludeInAll) {
                                ((AllFieldMapper.IncludeInAll) mergeWithMapper).includeInAll(false);
                            }
                            if (newMappersBuilder == null) {
                                newMappersBuilder = newMapBuilder(mappers);
                            }
                            newMappersBuilder.put(mergeWithMapper.name(), mergeWithMapper);
                            if (mergeWithMapper instanceof AbstractFieldMapper) {
                                if (newFieldMappers == null) {
                                    newFieldMappers = new ArrayList<FieldMapper>();
                                }
                                newFieldMappers.add((FieldMapper) mergeWithMapper);
                            }
                        }
                    } else {
                        mergeIntoMapper.merge(mergeWithMapper, mergeContext);
                    }
                }

                // first add all field mappers
                if (newFieldMappers != null && !newFieldMappers.isEmpty()) {
                    mergeContext.docMapper().addFieldMappers(newFieldMappers);
                }
                // now publish mappers
                if (newDefaultMapper != null) {
                    defaultMapper = newDefaultMapper;
                }
                if (newMappersBuilder != null) {
                    mappers = newMappersBuilder.immutableMap();
                }
            }
        }
    }

    @Override
    public void close() {
        if (defaultMapper != null) {
            defaultMapper.close();
        }
        for (Mapper mapper : mappers.values()) {
            mapper.close();
        }
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        if (defaultMapper != null) {
            defaultMapper.traverse(fieldMapperListener);
        }
        for (Mapper mapper : mappers.values()) {
            mapper.traverse(fieldMapperListener);
        }
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);
        if (pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
        }


        builder.startObject("fields");
        if (defaultMapper != null) {
            defaultMapper.toXContent(builder, params);
        }
        if (mappers.size() <= 1) {
            for (Mapper mapper : mappers.values()) {
                mapper.toXContent(builder, params);
            }
        } else {
            // sort the mappers (by name) if there is more than one mapping
            TreeMap<String, Mapper> sortedMappers = new TreeMap<String, Mapper>(mappers);
            for (Mapper mapper : sortedMappers.values()) {
                mapper.toXContent(builder, params);
            }
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }
}
