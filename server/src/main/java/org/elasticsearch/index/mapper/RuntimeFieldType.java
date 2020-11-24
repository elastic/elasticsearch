/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Base implementation for a runtime field that can be defined as part of the runtime section of the index mappings
 */
public abstract class RuntimeFieldType extends MappedFieldType implements ToXContentFragment {

    protected RuntimeFieldType(String name, Map<String, String> meta) {
        super(name, false, false, false, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, meta);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.field("type", typeName());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        doXContentBody(builder, includeDefaults);
        builder.endObject();
        return builder;
    }

    /**
     * Prints out the parameters that subclasses expose
     */
    protected abstract void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException;

    /**
     * Parser for a runtime field. Creates the appropriate {@link RuntimeFieldType} for a runtime field,
     * as defined in the runtime section of the index mappings.
     */
    public interface Parser {
        RuntimeFieldType parse(String name, Map<String, Object> node, Mapper.TypeParser.ParserContext parserContext)
            throws MapperParsingException;
    }

    public static void parseRuntimeFields(Map<String, Object> node,
                                          Mapper.TypeParser.ParserContext parserContext,
                                          Consumer<RuntimeFieldType> runtimeFieldTypeConsumer) {
        Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> propNode = new HashMap<>(((Map<String, Object>) entry.getValue()));
                Object typeNode = propNode.get("type");
                String type;
                if (typeNode == null) {
                    throw new MapperParsingException("No type specified for runtime field [" + fieldName + "]");
                } else {
                    type = typeNode.toString();
                }
                Parser typeParser = parserContext.runtimeFieldTypeParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("No handler for type [" + type +
                        "] declared on runtime field [" + fieldName + "]");
                }
                runtimeFieldTypeConsumer.accept(typeParser.parse(fieldName, propNode, parserContext));
                propNode.remove("type");
                DocumentMapperParser.checkNoRemainingFields(fieldName, propNode, parserContext.indexVersionCreated());
                iterator.remove();
            } else {
                throw new MapperParsingException("Expected map for runtime field [" + fieldName + "] definition but got a "
                    + fieldName.getClass().getName());
            }
        }
    }
}
