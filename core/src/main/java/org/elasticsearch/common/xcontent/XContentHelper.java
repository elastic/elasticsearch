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

package org.elasticsearch.common.xcontent;

import java.nio.charset.StandardCharsets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.ToXContent.Params;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

/**
 *
 */
@SuppressWarnings("unchecked")
public class XContentHelper {

    public static XContentParser createParser(BytesReference bytes) throws IOException {
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor != null) {
            InputStream compressedInput = compressor.streamInput(bytes.streamInput());
            if (compressedInput.markSupported() == false) {
                compressedInput = new BufferedInputStream(compressedInput);
            }
            XContentType contentType = XContentFactory.xContentType(compressedInput);
            return XContentFactory.xContent(contentType).createParser(compressedInput);
        } else {
            return XContentFactory.xContent(bytes).createParser(bytes.streamInput());
        }
    }

    public static Tuple<XContentType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered) throws ElasticsearchParseException {
        try {
            XContentType contentType;
            InputStream input;
            Compressor compressor = CompressorFactory.compressor(bytes);
            if (compressor != null) {
                InputStream compressedStreamInput = compressor.streamInput(bytes.streamInput());
                if (compressedStreamInput.markSupported() == false) {
                    compressedStreamInput = new BufferedInputStream(compressedStreamInput);
                }
                contentType = XContentFactory.xContentType(compressedStreamInput);
                input = compressedStreamInput;
            } else {
                contentType = XContentFactory.xContentType(bytes);
                input = bytes.streamInput();
            }
            try (XContentParser parser = XContentFactory.xContent(contentType).createParser(input)) {
                if (ordered) {
                    return Tuple.tuple(contentType, parser.mapOrdered());
                } else {
                    return Tuple.tuple(contentType, parser.map());
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
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
            return new String(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length(), StandardCharsets.UTF_8);
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
            return new String(data, offset, length, StandardCharsets.UTF_8);
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
     * Writes serialized toXContent to pretty-printed JSON string.
     *
     * @param toXContent object to be pretty printed
     * @return pretty-printed JSON serialization
     */
    public static String toString(ToXContent toXContent) {
        return toString(toXContent, EMPTY_PARAMS);
    }

    /**
     * Writes serialized toXContent to pretty-printed JSON string.
     *
     * @param toXContent object to be pretty printed
     * @param params     serialization parameters
     * @return pretty-printed JSON serialization
     */
    public static String toString(ToXContent toXContent, Params params) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            if (params.paramAsBoolean("pretty", true)) {
                builder.prettyPrint();
            }
            if (params.paramAsBoolean("human", true)) {
                builder.humanReadable(true);
            }
            builder.startObject();
            toXContent.toXContent(builder, params);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
                builder.startObject();
                builder.field("error", e.getMessage());
                builder.endObject();
                return builder.string();
            } catch (IOException e2) {
                throw new ElasticsearchException("cannot generate error message for deserialization", e);
            }
        }

    }

    /**
     * Updates the provided changes into the source. If the key exists in the changes, it overrides the one in source
     * unless both are Maps, in which case it recuersively updated it.
     *
     * @param source                 the original map to be updated
     * @param changes                the changes to update into updated
     * @param checkUpdatesAreUnequal should this method check if updates to the same key (that are not both maps) are
     *                               unequal?  This is just a .equals check on the objects, but that can take some time on long strings.
     * @return true if the source map was modified
     */
    public static boolean update(Map<String, Object> source, Map<String, Object> changes, boolean checkUpdatesAreUnequal) {
        boolean modified = false;
        for (Map.Entry<String, Object> changesEntry : changes.entrySet()) {
            if (!source.containsKey(changesEntry.getKey())) {
                // safe to copy, change does not exist in source
                source.put(changesEntry.getKey(), changesEntry.getValue());
                modified = true;
                continue;
            }
            Object old = source.get(changesEntry.getKey());
            if (old instanceof Map && changesEntry.getValue() instanceof Map) {
                // recursive merge maps
                modified |= update((Map<String, Object>) source.get(changesEntry.getKey()),
                        (Map<String, Object>) changesEntry.getValue(), checkUpdatesAreUnequal && !modified);
                continue;
            }
            // update the field
            source.put(changesEntry.getKey(), changesEntry.getValue());
            if (modified) {
                continue;
            }
            if (!checkUpdatesAreUnequal) {
                modified = true;
                continue;
            }
            modified = !Objects.equals(old, changesEntry.getValue());
        }
        return modified;
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
                        Map<String, Map<String, Object>> processed = new LinkedHashMap<>();
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
                            } else {
                                // put the default entries after the content ones.
                                processed.put(entry.getKey(), map);
                            }
                        }
                        for (Map<String, Object> map : processed.values()) {
                            mergedList.add(map);
                        }
                    } else {
                        // if both are lists, simply combine them, first the defaults, then the content
                        // just make sure not to add the same value twice
                        mergedList.addAll(defaultList);
                        for (Object o : contentList) {
                            if (!mergedList.contains(o)) {
                                mergedList.add(o);
                            }
                        }
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
        XContentParser.Token token = parser.currentToken();

        // Let's handle field-name separately first
        if (token == XContentParser.Token.FIELD_NAME) {
            generator.writeFieldName(parser.currentName());
            token = parser.nextToken();
            // fall-through to copy the associated value
        }

        switch (token) {
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

    /**
     * Directly writes the source to the output builder
     */
    public static void writeDirect(BytesReference source, XContentBuilder rawBuilder, ToXContent.Params params) throws IOException {
        Compressor compressor = CompressorFactory.compressor(source);
        if (compressor != null) {
            InputStream compressedStreamInput = compressor.streamInput(source.streamInput());
            if (compressedStreamInput.markSupported() == false) {
                compressedStreamInput = new BufferedInputStream(compressedStreamInput);
            }
            XContentType contentType = XContentFactory.xContentType(compressedStreamInput);
            if (contentType == rawBuilder.contentType()) {
                Streams.copy(compressedStreamInput, rawBuilder.stream());
            } else {
                try (XContentParser parser = XContentFactory.xContent(contentType).createParser(compressedStreamInput)) {
                    parser.nextToken();
                    rawBuilder.copyCurrentStructure(parser);
                }
            }
        } else {
            XContentType contentType = XContentFactory.xContentType(source);
            if (contentType == rawBuilder.contentType()) {
                source.writeTo(rawBuilder.stream());
            } else {
                try (XContentParser parser = XContentFactory.xContent(contentType).createParser(source)) {
                    parser.nextToken();
                    rawBuilder.copyCurrentStructure(parser);
                }
            }
        }
    }

    /**
     * Writes a "raw" (bytes) field, handling cases where the bytes are compressed, and tries to optimize writing using
     * {@link XContentBuilder#rawField(String, org.elasticsearch.common.bytes.BytesReference)}.
     */
    public static void writeRawField(String field, BytesReference source, XContentBuilder builder, ToXContent.Params params) throws IOException {
        Compressor compressor = CompressorFactory.compressor(source);
        if (compressor != null) {
            InputStream compressedStreamInput = compressor.streamInput(source.streamInput());
            if (compressedStreamInput.markSupported() == false) {
                compressedStreamInput = new BufferedInputStream(compressedStreamInput);
            }
            XContentType contentType = XContentFactory.xContentType(compressedStreamInput);
            if (contentType == builder.contentType()) {
                builder.rawField(field, compressedStreamInput);
            } else {
                try (XContentParser parser = XContentFactory.xContent(contentType).createParser(compressedStreamInput)) {
                    parser.nextToken();
                    builder.field(field);
                    builder.copyCurrentStructure(parser);
                }
            }
        } else {
            XContentType contentType = XContentFactory.xContentType(source);
            if (contentType == builder.contentType()) {
                builder.rawField(field, source);
            } else {
                try (XContentParser parser = XContentFactory.xContent(contentType).createParser(source)) {
                    parser.nextToken();
                    builder.field(field);
                    builder.copyCurrentStructure(parser);
                }
            }
        }
    }
}
