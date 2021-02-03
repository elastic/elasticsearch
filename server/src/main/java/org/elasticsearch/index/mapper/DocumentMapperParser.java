/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;

public class DocumentMapperParser {
    private final IndexSettings indexSettings;
    private final IndexAnalyzers indexAnalyzers;
    private final Function<String, String> documentTypeResolver;
    private final DocumentParser documentParser;
    private final Function<String, Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>> metadataMappersFunction;
    private final Supplier<Mapper.TypeParser.ParserContext> parserContextSupplier;
    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();
    private final Map<String, MetadataFieldMapper.TypeParser> rootTypeParsers;
    private final NamedXContentRegistry xContentRegistry;

    DocumentMapperParser(IndexSettings indexSettings,
                         IndexAnalyzers indexAnalyzers,
                         Function<String, String> documentTypeResolver,
                         DocumentParser documentParser,
                         Function<String, Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>> metadataMappersFunction,
                         Supplier<Mapper.TypeParser.ParserContext> parserContextSupplier,
                         Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers,
                         NamedXContentRegistry xContentRegistry) {
        this.indexSettings = indexSettings;
        this.indexAnalyzers = indexAnalyzers;
        this.documentTypeResolver = documentTypeResolver;
        this.documentParser = documentParser;
        this.metadataMappersFunction = metadataMappersFunction;
        this.parserContextSupplier = parserContextSupplier;
        this.rootTypeParsers = metadataMapperParsers;
        this.xContentRegistry = xContentRegistry;
    }

    public DocumentMapper parse(@Nullable String type, CompressedXContent source) throws MapperParsingException {
        return parse(type, source, null);
    }

    public DocumentMapper parse(@Nullable String type, CompressedXContent source, String defaultSource) throws MapperParsingException {
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
        return parse(type, mapping, defaultSource);
    }

    @SuppressWarnings({"unchecked"})
    private DocumentMapper parse(String type, Map<String, Object> mapping, String defaultSource) throws MapperParsingException {
        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }

        if (defaultSource != null) {
            Tuple<String, Map<String, Object>> t = extractMapping(MapperService.DEFAULT_MAPPING, defaultSource);
            if (t.v2() != null) {
                XContentHelper.mergeDefaults(mapping, t.v2());
            }
        }

        Mapper.TypeParser.ParserContext parserContext = parserContextSupplier.get();
        // parse RootObjectMapper
        RootObjectMapper.Builder root = (RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext);
        DocumentMapper.Builder docBuilder = new DocumentMapper.Builder(root, indexSettings, indexAnalyzers, documentParser,
            metadataMappersFunction);
        Iterator<Map.Entry<String, Object>> iterator = mapping.entrySet().iterator();
        // parse DocumentMapper
        while(iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();

            MetadataFieldMapper.TypeParser typeParser = rootTypeParsers.get(fieldName);
            if (typeParser != null) {
                iterator.remove();
                if (false == fieldNode instanceof Map) {
                    throw new IllegalArgumentException("[_parent] must be an object containing [type]");
                }
                Map<String, Object> fieldNodeMap = (Map<String, Object>) fieldNode;
                docBuilder.put(typeParser.parse(fieldName, fieldNodeMap, parserContext));
                fieldNodeMap.remove("type");
                checkNoRemainingFields(fieldName, fieldNodeMap);
            }
        }

        Map<String, Object> meta = (Map<String, Object>) mapping.remove("_meta");
        if (meta != null) {
            // It may not be required to copy meta here to maintain immutability
            // but the cost is pretty low here.
            docBuilder.meta(unmodifiableMap(new HashMap<>(meta)));
        }

        checkNoRemainingFields(mapping, "Root mapping definition has unsupported parameters: ");

        return docBuilder.build();
    }

    public static void checkNoRemainingFields(String fieldName, Map<?, ?> fieldNodeMap) {
        checkNoRemainingFields(fieldNodeMap, "Mapping definition for [" + fieldName + "] has unsupported parameters: ");
    }

    public static void checkNoRemainingFields(Map<?, ?> fieldNodeMap, String message) {
        if (!fieldNodeMap.isEmpty()) {
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

    private Tuple<String, Map<String, Object>> extractMapping(String type, String source) throws MapperParsingException {
        Map<String, Object> root;
        try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, source)) {
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
     *
     * @return A tuple of the form (type, normalized mappings).
     */
    @SuppressWarnings({"unchecked"})
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
