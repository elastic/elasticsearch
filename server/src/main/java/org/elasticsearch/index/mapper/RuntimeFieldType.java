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
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Definition of a runtime field that can be defined as part of the runtime section of the index mappings
 */
public interface RuntimeFieldType extends ToXContentFragment {

    @Override
    default XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.field("type", typeName());
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Prints out the parameters that subclasses expose
     */
    void doXContentBody(XContentBuilder builder, Params params) throws IOException;

    /**
     * Exposes the name of the runtime field
     * @return name of the field
     */
    String name();

    /**
     * Exposes the type of the runtime field
     * @return type of the field
     */
    String typeName();

    /**
     * Exposes the {@link MappedFieldType} backing this runtime field, used to execute queries, run aggs etc.
     * @return the {@link MappedFieldType} backing this runtime field
     */
    MappedFieldType asMappedFieldType();

    /**
     *  For runtime fields the {@link RuntimeFieldType.Parser} returns directly the {@link MappedFieldType}.
     *  Internally we still create a {@link RuntimeFieldType.Builder} so we reuse the {@link FieldMapper.Parameter} infrastructure,
     *  but {@link RuntimeFieldType.Builder#init(FieldMapper)} and {@link RuntimeFieldType.Builder#build(ContentPath)} are never called as
     *  {@link RuntimeFieldType.Parser#parse(String, Map, Mapper.TypeParser.ParserContext)} calls
     *  {@link RuntimeFieldType.Builder#parse(String, Mapper.TypeParser.ParserContext, Map)} and returns the corresponding
     *  {@link MappedFieldType}.
     */
    abstract class Builder extends FieldMapper.Builder {
        final FieldMapper.Parameter<Map<String, String>> meta = FieldMapper.Parameter.metaParam();

        protected Builder(String name) {
            super(name);
        }

        public Map<String, String> meta() {
            return meta.getValue();
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            return Collections.singletonList(meta);
        }

        @Override
        public FieldMapper.Builder init(FieldMapper initializer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final FieldMapper build(ContentPath context) {
            throw new UnsupportedOperationException();
        }

        protected abstract RuntimeFieldType buildFieldType();

        private void validate() {
            ContentPath contentPath = parentPath(name());
            FieldMapper.MultiFields multiFields = multiFieldsBuilder.build(this, contentPath);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException("runtime field [" + name + "] does not support [fields]");
            }
            FieldMapper.CopyTo copyTo = this.copyTo.build();
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException("runtime field [" + name + "] does not support [copy_to]");
            }
        }
    }

    /**
     * Parser for a runtime field. Creates the appropriate {@link RuntimeFieldType} for a runtime field,
     * as defined in the runtime section of the index mappings.
     */
    final class Parser {
        private final BiFunction<String, Mapper.TypeParser.ParserContext, RuntimeFieldType.Builder> builderFunction;

        public Parser(BiFunction<String, Mapper.TypeParser.ParserContext, RuntimeFieldType.Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        RuntimeFieldType parse(String name, Map<String, Object> node, Mapper.TypeParser.ParserContext parserContext)
            throws MapperParsingException {

            RuntimeFieldType.Builder builder = builderFunction.apply(name, parserContext);
            builder.parse(name, parserContext, node);
            builder.validate();
            return builder.buildFieldType();
        }
    }

    /**
     * Parse runtime fields from the provided map, using the provided parser context.
     * @param node the map that holds the runtime fields configuration
     * @param parserContext the parser context that holds info needed when parsing mappings
     * @param supportsRemoval whether a null value for a runtime field should be properly parsed and
     *                        translated to the removal of such runtime field
     * @return the parsed runtime fields
     */
    static Map<String, RuntimeFieldType> parseRuntimeFields(Map<String, Object> node,
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
