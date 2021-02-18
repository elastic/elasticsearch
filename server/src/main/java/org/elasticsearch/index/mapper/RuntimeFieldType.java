/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

    /**
     * Parse runtime fields from the provided map, using the provided parser context.
     * @param node the map that holds the runtime fields configuration
     * @param parserContext the parser context that holds info needed when parsing mappings
     * @param supportsRemoval whether a null value for a runtime field should be properly parsed and
     *                        translated to the removal of such runtime field
     * @return the parsed runtime fields
     */
    public static Map<String, RuntimeFieldType> parseRuntimeFields(Map<String, Object> node,
                                                                   Mapper.TypeParser.ParserContext parserContext,
                                                                   boolean supportsRemoval) {
        Map<String, RuntimeFieldType> runtimeFields = new HashMap<>();
        Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            if (entry.getValue() == null) {
                if (supportsRemoval) {
                    runtimeFields.put(fieldName, null);
                } else {
                    throw new MapperParsingException("Runtime field [" + fieldName + "] was set to null but its removal is not supported " +
                        "in this context");
                }
            } else if (entry.getValue() instanceof Map) {
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
                runtimeFields.put(fieldName, typeParser.parse(fieldName, propNode, parserContext));
                propNode.remove("type");
                MappingParser.checkNoRemainingFields(fieldName, propNode);
                iterator.remove();
            } else {
                throw new MapperParsingException("Expected map for runtime field [" + fieldName + "] definition but got a "
                    + entry.getValue().getClass().getName());
            }
        }
        return Collections.unmodifiableMap(runtimeFields);
    }
}
