/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;

/**
 * Parser for {@link Mapping} provided in {@link CompressedXContent} format
 */

public final class MappingParser {
    private final Supplier<MappingParserContext> parserContextSupplier;
    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();
    private final Function<String, Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>> metadataMappersFunction;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;
    private final Function<String, String> documentTypeResolver;
    private final NamedXContentRegistry xContentRegistry;

    MappingParser(
        Supplier<MappingParserContext> parserContextSupplier,
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers,
        Function<String, Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>> metadataMappersFunction,
        Function<String, String> documentTypeResolver,
        NamedXContentRegistry xContentRegistry
    ) {
        this.parserContextSupplier = parserContextSupplier;
        this.metadataMappersFunction = metadataMappersFunction;
        this.metadataMapperParsers = metadataMapperParsers;
        this.documentTypeResolver = documentTypeResolver;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Verify that there are no remaining fields in the provided map that contained mapped fields
     *
     * @param fieldName    the name of the field that is being parsed
     * @param fieldNodeMap the map of fields
     */
    public static void checkNoRemainingFields(String fieldName, Map<?, ?> fieldNodeMap) {
        checkNoRemainingFields(fieldNodeMap, "Mapping definition for [" + fieldName + "] has unsupported parameters: ");
    }

    /**
     * Verify that there are no remaining fields in the provided map that contained mapped fields
     *
     * @param fieldNodeMap the map of fields
     * @param message      the error message to be returned in case the provided map contains one or more fields
     */
    public static void checkNoRemainingFields(Map<?, ?> fieldNodeMap, String message) {
        if (fieldNodeMap.isEmpty() == false) {
            throw new MapperParsingException(message + getRemainingFields(fieldNodeMap));
        }
    }

    private static String getRemainingFields(Map<?, ?> map) {
        StringBuilder remainingFields = new StringBuilder();
        for (Object key : map.keySet()) {
            remainingFields.append(" [").append(key).append(" : ").append(map.get(key)).append("]");
        }
        return remainingFields.toString();
    }

    public Mapping parse(@Nullable String type, CompressedXContent source) throws MapperParsingException {
        return parse(type, source, null);
    }

    public Mapping parse(@Nullable String type, CompressedXContent source, String defaultSource) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            Map<String, Object> root = XContentHelper.convertToMap(source.compressedReference(), true, XContentType.JSON).v2();
            Tuple<String, Map<String, Object>> t = extractMapping(type, root);
            type = t.v1();
            mapping = t.v2();
        }
        if (mapping == null) {
            mapping = new HashMap<>();
        }
        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }

        if (defaultSource != null) {
            Tuple<String, Map<String, Object>> t = extractMapping(MapperService.DEFAULT_MAPPING, defaultSource);
            if (t.v2() != null) {
                XContentHelper.mergeDefaults(mapping, t.v2());
            }
        }

        MappingParserContext parserContext = parserContextSupplier.get();
        RootObjectMapper rootObjectMapper = rootObjectTypeParser.parse(type, mapping, parserContext).build(MapperBuilderContext.ROOT);

        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = metadataMappersFunction.apply(type);
        Map<String, Object> meta = null;

        Iterator<Map.Entry<String, Object>> iterator = mapping.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();

            MetadataFieldMapper.TypeParser typeParser = metadataMapperParsers.get(fieldName);
            if (typeParser != null) {
                iterator.remove();
                if (false == fieldNode instanceof Map) {
                    throw new IllegalArgumentException("[_parent] must be an object containing [type]");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> fieldNodeMap = (Map<String, Object>) fieldNode;
                MetadataFieldMapper metadataFieldMapper = typeParser.parse(fieldName, fieldNodeMap, parserContext)
                    .build(MapperBuilderContext.ROOT);
                metadataMappers.put(metadataFieldMapper.getClass(), metadataFieldMapper);
                fieldNodeMap.remove("type");
                checkNoRemainingFields(fieldName, fieldNodeMap);
            }
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> removed = (Map<String, Object>) mapping.remove("_meta");
        if (removed != null) {
            // It may not be required to copy meta here to maintain immutability
            // but the cost is pretty low here.
            meta = unmodifiableMap(new HashMap<>(removed));
        }
        checkNoRemainingFields(mapping, "Root mapping definition has unsupported parameters: ");

        return new Mapping(rootObjectMapper, metadataMappers.values().toArray(new MetadataFieldMapper[0]), meta);
    }

    private Tuple<String, Map<String, Object>> extractMapping(String type, String source) throws MapperParsingException {
        Map<String, Object> root;
        try (
            XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, source)
        ) {
            root = parser.mapOrdered();
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse mapping definition", e);
        }
        return extractMapping(type, root);
    }

    /**
     * Given an optional type name and mapping definition, returns the type and a normalized form of the mappings.
     *
     * The provided mapping definition may or may not contain the type name as the root key in the map. This method
     * attempts to unwrap the mappings, so that they no longer contain a type name at the root. If no type name can
     * be found, through either the 'type' parameter or by examining the provided mappings, then an exception will be
     * thrown.
     *
     * @param type An optional type name.
     * @param root The mapping definition.
     * @return A tuple of the form (type, normalized mappings).
     */
    @SuppressWarnings("unchecked")
    private Tuple<String, Map<String, Object>> extractMapping(String type, Map<String, Object> root) throws MapperParsingException {
        if (root.size() == 0) {
            if (type != null) {
                return new Tuple<>(type, root);
            } else {
                throw new MapperParsingException("malformed mapping, no type name found");
            }
        }

        String rootName = root.keySet().iterator().next();
        Tuple<String, Map<String, Object>> mapping;
        if (type == null || type.equals(rootName) || documentTypeResolver.apply(type).equals(rootName)) {
            mapping = new Tuple<>(rootName, (Map<String, Object>) root.get(rootName));
        } else {
            mapping = new Tuple<>(type, root);
        }
        return mapping;
    }
}
