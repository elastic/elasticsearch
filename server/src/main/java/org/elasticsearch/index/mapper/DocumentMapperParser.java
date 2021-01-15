/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class DocumentMapperParser {
    private final IndexSettings indexSettings;
    private final IndexAnalyzers indexAnalyzers;
    private final Function<String, String> documentTypeResolver;
    private final DocumentParser documentParser;
    private final Supplier<Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>> metadataMappersSupplier;
    private final Supplier<Mapper.TypeParser.ParserContext> parserContextSupplier;
    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();
    private final Map<String, MetadataFieldMapper.TypeParser> rootTypeParsers;

    DocumentMapperParser(IndexSettings indexSettings,
                         IndexAnalyzers indexAnalyzers,
                         Function<String, String> documentTypeResolver,
                         DocumentParser documentParser,
                         Supplier<Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper>> metadataMappersSupplier,
                         Supplier<Mapper.TypeParser.ParserContext> parserContextSupplier,
                         Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers) {
        this.indexSettings = indexSettings;
        this.indexAnalyzers = indexAnalyzers;
        this.documentTypeResolver = documentTypeResolver;
        this.documentParser = documentParser;
        this.metadataMappersSupplier = metadataMappersSupplier;
        this.parserContextSupplier = parserContextSupplier;
        this.rootTypeParsers = metadataMapperParsers;
    }

    @SuppressWarnings("unchecked")
    DocumentMapper parse(@Nullable String type, CompressedXContent source) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            mapping = XContentHelper.convertToMap(source.compressedReference(), true, XContentType.JSON).v2();
            if (mapping.isEmpty()) {
                if (type == null) {
                    throw new MapperParsingException("malformed mapping, no type name found");
                }
            } else {
                String rootName = mapping.keySet().iterator().next();
                if (type == null || type.equals(rootName) || documentTypeResolver.apply(type).equals(rootName)) {
                    type = rootName;
                    mapping = (Map<String, Object>) mapping.get(rootName);
                }
            }
        }
        if (mapping == null) {
            mapping = new HashMap<>();
        }
        return parse(type, mapping);
    }

    @SuppressWarnings({"unchecked"})
    private DocumentMapper parse(String type, Map<String, Object> mapping) throws MapperParsingException {
        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }

        Mapper.TypeParser.ParserContext parserContext = parserContextSupplier.get();
        // parse RootObjectMapper
        RootObjectMapper.Builder root = (RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext);
        DocumentMapper.Builder docBuilder = new DocumentMapper.Builder(root, indexSettings, indexAnalyzers, documentParser,
            metadataMappersSupplier.get());
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
            /*
             * It may not be required to copy meta here to maintain immutability but the cost is pretty low here.
             *
             * Note: this copy can not be replaced by Map#copyOf because we rely on consistent serialization order since we do byte-level
             * checks on the mapping between what we receive from the master and what we have locally. As Map#copyOf is not necessarily
             * the same underlying map implementation, we could end up with a different iteration order. For reference, see
             * MapperService#assertSerializtion and GitHub issues #10302 and #10318.
             *
             * Do not change this to Map#copyOf or any other method of copying meta that could change the iteration order.
             *
             * TODO:
             *  - this should almost surely be a copy as a LinkedHashMap to have the ordering guarantees that we are relying on
             *  - investigate the above note about whether or not we really need to be copying here, the ideal outcome would be to not
             */
            docBuilder.meta(Collections.unmodifiableMap(new HashMap<>(meta)));
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
}
