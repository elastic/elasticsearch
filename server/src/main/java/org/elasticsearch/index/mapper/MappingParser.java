/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Parser for {@link Mapping} provided in {@link CompressedXContent} format
 */
public final class MappingParser {
    private final Supplier<MappingParserContext> mappingParserContextSupplier;
    private final Supplier<Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>> metadataMappersSupplier;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;
    private final Function<String, String> documentTypeResolver;

    MappingParser(
        Supplier<MappingParserContext> mappingParserContextSupplier,
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers,
        Supplier<Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>> metadataMappersSupplier,
        Function<String, String> documentTypeResolver
    ) {
        this.mappingParserContextSupplier = mappingParserContextSupplier;
        this.metadataMapperParsers = metadataMapperParsers;
        this.metadataMappersSupplier = metadataMappersSupplier;
        this.documentTypeResolver = documentTypeResolver;
    }

    /**
     * Verify that there are no remaining fields in the provided map that contained mapped fields
     *
     * @param fieldName the name of the field that is being parsed
     * @param fieldNodeMap the map of fields
     */
    public static void checkNoRemainingFields(String fieldName, Map<?, ?> fieldNodeMap) {
        checkNoRemainingFields(fieldNodeMap, "Mapping definition for [" + fieldName + "] has unsupported parameters: ");
    }

    /**
     * Verify that there are no remaining fields in the provided map that contained mapped fields
     *
     * @param fieldNodeMap the map of fields
     * @param message the error message to be returned in case the provided map contains one or more fields
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

    static Map<String, Object> convertToMap(CompressedXContent source) {
        Objects.requireNonNull(source, "source cannot be null");
        return XContentHelper.convertToMap(source.compressedReference(), true, XContentType.JSON).v2();
    }

    Mapping parse(@Nullable String type, CompressedXContent source) throws MapperParsingException {
        Map<String, Object> mapping = convertToMap(source);
        return parse(type, mapping);
    }

    /**
     * A method to parse mapping from a source in a map form.
     *
     * @param type          the mapping type
     * @param mappingSource mapping source already converted to a map form, but not yet processed otherwise
     * @return a parsed mapping
     * @throws MapperParsingException in case of parsing error
     */
    @SuppressWarnings("unchecked")
    Mapping parse(@Nullable String type, Map<String, Object> mappingSource) throws MapperParsingException {
        if (mappingSource.isEmpty()) {
            if (type == null) {
                throw new MapperParsingException("malformed mapping, no type name found");
            }
        } else {
            String rootName = mappingSource.keySet().iterator().next();
            if (type == null || type.equals(rootName) || documentTypeResolver.apply(type).equals(rootName)) {
                type = rootName;
                mappingSource = (Map<String, Object>) mappingSource.get(rootName);
            }
        }
        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }
        if (type.isEmpty()) {
            throw new MapperParsingException("type cannot be an empty string");
        }

        final MappingParserContext mappingParserContext = mappingParserContextSupplier.get();

        RootObjectMapper.Builder rootObjectMapper = RootObjectMapper.parse(type, mappingSource, mappingParserContext);

        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = metadataMappersSupplier.get();
        Map<String, Object> meta = null;

        boolean isSourceSynthetic = mappingParserContext.getIndexSettings().getMode().isSyntheticSourceEnabled();
        boolean isDataStream = false;

        Iterator<Map.Entry<String, Object>> iterator = mappingSource.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();

            MetadataFieldMapper.TypeParser typeParser = metadataMapperParsers.get(fieldName);
            if (typeParser != null) {
                iterator.remove();
                if (false == fieldNode instanceof Map) {
                    throw new IllegalArgumentException("[" + fieldName + "] config must be an object");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> fieldNodeMap = (Map<String, Object>) fieldNode;
                MetadataFieldMapper metadataFieldMapper = typeParser.parse(fieldName, fieldNodeMap, mappingParserContext).build();
                metadataMappers.put(metadataFieldMapper.getClass(), metadataFieldMapper);
                assert fieldNodeMap.isEmpty();

                if (metadataFieldMapper instanceof SourceFieldMapper sfm) {
                    // Validation in other places should have failed first
                    assert sfm.isSynthetic()
                        || (sfm.isSynthetic() == false && mappingParserContext.getIndexSettings().getMode() != IndexMode.TIME_SERIES)
                        : "synthetic source can't be disabled in a time series index";
                    isSourceSynthetic = sfm.isSynthetic();
                }

                if (metadataFieldMapper instanceof DataStreamTimestampFieldMapper dsfm) {
                    isDataStream = dsfm.isEnabled();
                }
            }
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> removed = (Map<String, Object>) mappingSource.remove("_meta");
        if (removed != null) {
            /*
             * It may not be required to copy meta here to maintain immutability but the cost is pretty low here.
             *
             * Note: this copy can not be replaced by Map#copyOf because we rely on consistent serialization order since we do
             * byte-level checks on the mapping between what we receive from the master and what we have locally. As Map#copyOf
             * is not necessarily the same underlying map implementation, we could end up with a different iteration order.
             * For reference, see MapperService#assertSerializtion and GitHub issues #10302 and #10318.
             *
             * Do not change this to Map#copyOf or any other method of copying meta that could change the iteration order.
             *
             * TODO:
             *  - this should almost surely be a copy as a LinkedHashMap to have the ordering guarantees that we are relying on
             *  - investigate the above note about whether or not we need to be copying here, the ideal outcome would be to not
             */
            meta = Collections.unmodifiableMap(new HashMap<>(removed));
        }
        if (mappingParserContext.indexVersionCreated().isLegacyIndexVersion() == false) {
            // legacy indices are allowed to have extra definitions that we ignore (we will drop them on import)
            checkNoRemainingFields(mappingSource, "Root mapping definition has unsupported parameters: ");
        }

        return new Mapping(
            rootObjectMapper.build(MapperBuilderContext.root(isSourceSynthetic, isDataStream)),
            metadataMappers.values().toArray(new MetadataFieldMapper[0]),
            meta
        );
    }
}
