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

package org.elasticsearch.common.xcontent;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
@SuppressWarnings("unchecked")
public class XContentHelper {

    public static XContentParser createParser(BytesReference bytes) throws IOException {
        if (bytes.hasArray()) {
            return createParser(bytes.array(), bytes.arrayOffset(), bytes.length());
        }
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor != null) {
            CompressedStreamInput compressedInput = compressor.streamInput(bytes.streamInput());
            XContentType contentType = XContentFactory.xContentType(compressedInput);
            compressedInput.resetToBufferStart();
            return XContentFactory.xContent(contentType).createParser(compressedInput);
        } else {
            return XContentFactory.xContent(bytes).createParser(bytes.streamInput());
        }
    }


    public static XContentParser createParser(byte[] data, int offset, int length) throws IOException {
        Compressor compressor = CompressorFactory.compressor(data, offset, length);
        if (compressor != null) {
            CompressedStreamInput compressedInput = compressor.streamInput(new BytesStreamInput(data, offset, length, false));
            XContentType contentType = XContentFactory.xContentType(compressedInput);
            compressedInput.resetToBufferStart();
            return XContentFactory.xContent(contentType).createParser(compressedInput);
        } else {
            return XContentFactory.xContent(data, offset, length).createParser(data, offset, length);
        }
    }

    public static Tuple<XContentType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered) throws ElasticSearchParseException {
        if (bytes.hasArray()) {
            return convertToMap(bytes.array(), bytes.arrayOffset(), bytes.length(), ordered);
        }
        try {
            XContentParser parser;
            XContentType contentType;
            Compressor compressor = CompressorFactory.compressor(bytes);
            if (compressor != null) {
                CompressedStreamInput compressedStreamInput = compressor.streamInput(bytes.streamInput());
                contentType = XContentFactory.xContentType(compressedStreamInput);
                compressedStreamInput.resetToBufferStart();
                parser = XContentFactory.xContent(contentType).createParser(compressedStreamInput);
            } else {
                contentType = XContentFactory.xContentType(bytes);
                parser = XContentFactory.xContent(contentType).createParser(bytes.streamInput());
            }
            if (ordered) {
                return Tuple.tuple(contentType, parser.mapOrderedAndClose());
            } else {
                return Tuple.tuple(contentType, parser.mapAndClose());
            }
        } catch (IOException e) {
            throw new ElasticSearchParseException("Failed to parse content to map", e);
        }
    }

    public static Tuple<XContentType, Map<String, Object>> convertToMap(byte[] data, boolean ordered) throws ElasticSearchParseException {
        return convertToMap(data, 0, data.length, ordered);
    }

    public static Tuple<XContentType, Map<String, Object>> convertToMap(byte[] data, int offset, int length, boolean ordered) throws ElasticSearchParseException {
        try {
            XContentParser parser;
            XContentType contentType;
            Compressor compressor = CompressorFactory.compressor(data, offset, length);
            if (compressor != null) {
                CompressedStreamInput compressedStreamInput = compressor.streamInput(new BytesStreamInput(data, offset, length, false));
                contentType = XContentFactory.xContentType(compressedStreamInput);
                compressedStreamInput.resetToBufferStart();
                parser = XContentFactory.xContent(contentType).createParser(compressedStreamInput);
            } else {
                contentType = XContentFactory.xContentType(data, offset, length);
                parser = XContentFactory.xContent(contentType).createParser(data, offset, length);
            }
            if (ordered) {
                return Tuple.tuple(contentType, parser.mapOrderedAndClose());
            } else {
                return Tuple.tuple(contentType, parser.mapAndClose());
            }
        } catch (IOException e) {
            throw new ElasticSearchParseException("Failed to parse content to map", e);
        }
    }

    public static String convertToJson(BytesReference bytes, boolean reformatJson) throws IOException {
        return convertToJson(bytes, reformatJson, false);
    }

    public static String convertToJson(BytesReference bytes, boolean reformatJson, boolean prettyPrint) throws IOException {
        if (bytes.hasArray()) {
            return convertToJson(bytes.array(), bytes.arrayOffset(), bytes.length(), reformatJson, prettyPrint);
        }
        XContentType xContentType = XContentFactory.xContentType(bytes);
        if (xContentType == XContentType.JSON && !reformatJson) {
            BytesArray bytesArray = bytes.toBytesArray();
            return new String(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length(), Charsets.UTF_8);
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(xContentType).createParser(bytes.streamInput());
            parser.nextToken();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            if (prettyPrint) {
                builder.prettyPrint();
            }
            builder.copyCurrentStructure(parser);
            return builder.string();
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public static String convertToJson(byte[] data, int offset, int length, boolean reformatJson) throws IOException {
        return convertToJson(data, offset, length, reformatJson, false);
    }

    public static String convertToJson(byte[] data, int offset, int length, boolean reformatJson, boolean prettyPrint) throws IOException {
        XContentType xContentType = XContentFactory.xContentType(data, offset, length);
        if (xContentType == XContentType.JSON && !reformatJson) {
            return new String(data, offset, length, Charsets.UTF_8);
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(xContentType).createParser(data, offset, length);
            parser.nextToken();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            if (prettyPrint) {
                builder.prettyPrint();
            }
            builder.copyCurrentStructure(parser);
            return builder.string();
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    /**
     * Updates the provided changes into the source. If the key exists in the changes, it overrides the one in source
     * unless both are Maps, in which case it recuersively updated it.
     */
    public static void update(Map<String, Object> source, Map<String, Object> changes) {
        for (Map.Entry<String, Object> changesEntry : changes.entrySet()) {
            if (!source.containsKey(changesEntry.getKey())) {
                // safe to copy, change does not exist in source
                source.put(changesEntry.getKey(), changesEntry.getValue());
            } else {
                if (source.get(changesEntry.getKey()) instanceof Map && changesEntry.getValue() instanceof Map) {
                    // recursive merge maps
                    update((Map<String, Object>) source.get(changesEntry.getKey()), (Map<String, Object>) changesEntry.getValue());
                } else {
                    // update the field
                    source.put(changesEntry.getKey(), changesEntry.getValue());
                }
            }
        }
    }

    /**
     * Merges the defaults provided as the second parameter into the content of the first. Only does recursive merge
     * for inner maps.
     */
    @SuppressWarnings({"unchecked"})
    public static void mergeDefaults(Map<String, Object> content, Map<String, Object> defaults) {
        for (Map.Entry<String, Object> defaultEntry : defaults.entrySet()) {
            if (!content.containsKey(defaultEntry.getKey())) {
                // copy it over, it does not exists in the content
                content.put(defaultEntry.getKey(), defaultEntry.getValue());
            } else {
                // in the content and in the default, only merge compound ones (maps)
                if (content.get(defaultEntry.getKey()) instanceof Map && defaultEntry.getValue() instanceof Map) {
                    mergeDefaults((Map<String, Object>) content.get(defaultEntry.getKey()), (Map<String, Object>) defaultEntry.getValue());
                } else if (content.get(defaultEntry.getKey()) instanceof List && defaultEntry.getValue() instanceof List) {
                    List defaultList = (List) defaultEntry.getValue();
                    List contentList = (List) content.get(defaultEntry.getKey());

                    List mergedList = new ArrayList();
                    if (allListValuesAreMapsOfOne(defaultList) && allListValuesAreMapsOfOne(contentList)) {
                        // all are in the form of [ {"key1" : {}}, {"key2" : {}} ], merge based on keys
                        Map<String, Map<String, Object>> processed = Maps.newLinkedHashMap();
                        for (Object o : contentList) {
                            Map<String, Object> map = (Map<String, Object>) o;
                            Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                            processed.put(entry.getKey(), map);
                        }
                        for (Object o : defaultList) {
                            Map<String, Object> map = (Map<String, Object>) o;
                            Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                            if (processed.containsKey(entry.getKey())) {
                                mergeDefaults(processed.get(entry.getKey()), map);
                            }
                        }
                        for (Map<String, Object> map : processed.values()) {
                            mergedList.add(map);
                        }
                    } else {
                        // if both are lists, simply combine them, first the defaults, then the content
                        mergedList.addAll((Collection) defaultEntry.getValue());
                        mergedList.addAll((Collection) content.get(defaultEntry.getKey()));
                    }
                    content.put(defaultEntry.getKey(), mergedList);
                }
            }
        }
    }

    private static boolean allListValuesAreMapsOfOne(List list) {
        for (Object o : list) {
            if (!(o instanceof Map)) {
                return false;
            }
            if (((Map) o).size() != 1) {
                return false;
            }
        }
        return true;
    }

    public static void copyCurrentStructure(XContentGenerator generator, XContentParser parser) throws IOException {
        XContentParser.Token t = parser.currentToken();

        // Let's handle field-name separately first
        if (t == XContentParser.Token.FIELD_NAME) {
            generator.writeFieldName(parser.currentName());
            t = parser.nextToken();
            // fall-through to copy the associated value
        }

        switch (t) {
            case START_ARRAY:
                generator.writeStartArray();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    copyCurrentStructure(generator, parser);
                }
                generator.writeEndArray();
                break;
            case START_OBJECT:
                generator.writeStartObject();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    copyCurrentStructure(generator, parser);
                }
                generator.writeEndObject();
                break;
            default: // others are simple:
                copyCurrentEvent(generator, parser);
        }
    }

    public static void copyCurrentEvent(XContentGenerator generator, XContentParser parser) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                generator.writeStartObject();
                break;
            case END_OBJECT:
                generator.writeEndObject();
                break;
            case START_ARRAY:
                generator.writeStartArray();
                break;
            case END_ARRAY:
                generator.writeEndArray();
                break;
            case FIELD_NAME:
                generator.writeFieldName(parser.currentName());
                break;
            case VALUE_STRING:
                if (parser.hasTextCharacters()) {
                    generator.writeString(parser.textCharacters(), parser.textOffset(), parser.textLength());
                } else {
                    generator.writeString(parser.text());
                }
                break;
            case VALUE_NUMBER:
                switch (parser.numberType()) {
                    case INT:
                        generator.writeNumber(parser.intValue());
                        break;
                    case LONG:
                        generator.writeNumber(parser.longValue());
                        break;
                    case FLOAT:
                        generator.writeNumber(parser.floatValue());
                        break;
                    case DOUBLE:
                        generator.writeNumber(parser.doubleValue());
                        break;
                }
                break;
            case VALUE_BOOLEAN:
                generator.writeBoolean(parser.booleanValue());
                break;
            case VALUE_NULL:
                generator.writeNull();
                break;
            case VALUE_EMBEDDED_OBJECT:
                generator.writeBinary(parser.binaryValue());
        }
    }

}
