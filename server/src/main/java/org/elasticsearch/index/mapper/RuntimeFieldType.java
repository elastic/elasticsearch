/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Collections;
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
     * Each runtime field will be provided to the given consumer.
     * @param node the map that holds the runtime fields configuration
     * @param parserContext the parser context that holds info needed when parsing mappings
     * @param runtimeFieldTypeConsumer the consumer that will receive each parsed runtime field
     * @param supportsRemoval whether a null value for a runtime field should be properly parsed and
     *                        translated to the removal of such runtime field
     */
    public static void parseRuntimeFields(Map<String, Object> node,
                                          Mapper.TypeParser.ParserContext parserContext,
                                          Consumer<RuntimeFieldType> runtimeFieldTypeConsumer,
                                          boolean supportsRemoval) {
        Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            if (entry.getValue() == null) {
                if (supportsRemoval) {
                    runtimeFieldTypeConsumer.accept(new PlaceholderForRemoval(fieldName));
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
                runtimeFieldTypeConsumer.accept(typeParser.parse(fieldName, propNode, parserContext));
                propNode.remove("type");
                MappingParser.checkNoRemainingFields(fieldName, propNode);
                iterator.remove();
            } else {
                throw new MapperParsingException("Expected map for runtime field [" + fieldName + "] definition but got a "
                    + entry.getValue().getClass().getName());
            }
        }
    }

    final boolean isPlaceholderForRemoval() {
        return this instanceof PlaceholderForRemoval;
    }

    /**
     * Placeholder runtime field used to remove an existing runtime field.
     * It is expected to disappear after merging, hence it does not need to implement
     * any of the usual runtime field methods. It only needs to be able to print itself out
     * as a field name with a null value, which is the syntax to indicate the intention to
     * remove a specific runtime field.
     */
    private static class PlaceholderForRemoval extends RuntimeFieldType {
        PlaceholderForRemoval(String name) {
            super(name, Collections.emptyMap());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.nullField(name());
            return builder;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String typeName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
