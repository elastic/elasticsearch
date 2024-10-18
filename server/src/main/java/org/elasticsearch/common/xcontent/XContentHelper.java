/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@SuppressWarnings("unchecked")
public class XContentHelper {

    /**
     * Creates a parser based on the bytes provided
     * @deprecated use {@link #createParser(XContentParserConfiguration, BytesReference, XContentType)}
     * to avoid content type auto-detection
     */
    @Deprecated
    public static XContentParser createParser(NamedXContentRegistry registry, DeprecationHandler deprecation, BytesReference bytes)
        throws IOException {
        return createParser(XContentParserConfiguration.EMPTY.withRegistry(registry).withDeprecationHandler(deprecation), bytes);
    }

    /**
     * Creates a parser based on the bytes provided
     * @deprecated use {@link #createParser(XContentParserConfiguration, BytesReference, XContentType)}
     * to avoid content type auto-detection
     */
    @Deprecated
    public static XContentParser createParser(XContentParserConfiguration config, BytesReference bytes) throws IOException {
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor != null) {
            InputStream compressedInput = compressor.threadLocalInputStream(bytes.streamInput());
            if (compressedInput.markSupported() == false) {
                compressedInput = new BufferedInputStream(compressedInput);
            }
            final XContentType contentType = XContentFactory.xContentType(compressedInput);
            return XContentFactory.xContent(contentType).createParser(config, compressedInput);
        } else {
            return createParserNotCompressed(config, bytes, xContentType(bytes));
        }
    }

    /**
     * Same as {@link #createParser(XContentParserConfiguration, BytesReference, XContentType)} but only supports uncompressed
     * {@code bytes}.
     */
    public static XContentParser createParserNotCompressed(
        XContentParserConfiguration config,
        BytesReference bytes,
        XContentType xContentType
    ) throws IOException {
        XContent xContent = xContentType.xContent();
        if (bytes.hasArray()) {
            return xContent.createParser(config, bytes.array(), bytes.arrayOffset(), bytes.length());
        }
        return xContent.createParser(config, bytes.streamInput());
    }

    /**
     * Creates a parser for the bytes provided
     * @deprecated use {@link #createParser(XContentParserConfiguration, BytesReference, XContentType)}
     */
    @Deprecated
    public static XContentParser createParser(
        NamedXContentRegistry registry,
        DeprecationHandler deprecation,
        BytesReference bytes,
        XContentType xContentType
    ) throws IOException {
        return createParser(
            XContentParserConfiguration.EMPTY.withRegistry(registry).withDeprecationHandler(deprecation),
            bytes,
            xContentType
        );
    }

    /**
     * Creates a parser for the bytes using the supplied content-type
     */
    public static XContentParser createParser(XContentParserConfiguration config, BytesReference bytes, XContentType xContentType)
        throws IOException {
        Objects.requireNonNull(xContentType);
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor != null) {
            return XContentFactory.xContent(xContentType).createParser(config, compressor.threadLocalInputStream(bytes.streamInput()));
        } else {
            // TODO now that we have config we make a method on bytes to do this building without needing this check everywhere
            return createParserNotCompressed(config, bytes, xContentType);
        }
    }

    /**
     * Converts the given bytes into a map that is optionally ordered.
     * <p>
     * Important: This can lose precision on numbers with a decimal point. It
     * converts numbers like {@code "n": 1234.567} to a {@code double} which
     * only has 52 bits of precision in the mantissa. This will come up most
     * frequently when folks write nanosecond precision dates as a decimal
     * number.
     * @deprecated this method relies on auto-detection of content type. Use {@link #convertToMap(BytesReference, boolean, XContentType)}
     *             instead with the proper {@link XContentType}
     */
    @Deprecated
    public static Tuple<XContentType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered)
        throws ElasticsearchParseException {
        return parseToType(ordered ? XContentParser::mapOrdered : XContentParser::map, bytes, null, XContentParserConfiguration.EMPTY);
    }

    /**
     * Exactly the same as {@link XContentHelper#convertToMap(BytesReference, boolean, XContentType, Set, Set)} but
     * none of the fields are filtered
     */
    public static Tuple<XContentType, Map<String, Object>> convertToMap(
        BytesReference bytes,
        boolean ordered,
        XContentType xContentType,
        XContentParserDecorator parserDecorator
    ) {
        return parseToType(
            ordered ? XContentParser::mapOrdered : XContentParser::map,
            bytes,
            xContentType,
            XContentParserConfiguration.EMPTY,
            parserDecorator
        );
    }

    public static Tuple<XContentType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered, XContentType xContentType) {
        return parseToType(
            ordered ? XContentParser::mapOrdered : XContentParser::map,
            bytes,
            xContentType,
            XContentParserConfiguration.EMPTY
        );
    }

    /**
     * Converts the given bytes into a map that is optionally ordered. The provided {@link XContentType} must be non-null.
     * <p>
     * Important: This can lose precision on numbers with a decimal point. It
     * converts numbers like {@code "n": 1234.567} to a {@code double} which
     * only has 52 bits of precision in the mantissa. This will come up most
     * frequently when folks write nanosecond precision dates as a decimal
     * number.
     */
    public static Tuple<XContentType, Map<String, Object>> convertToMap(
        BytesReference bytes,
        boolean ordered,
        XContentType xContentType,
        @Nullable Set<String> include,
        @Nullable Set<String> exclude
    ) throws ElasticsearchParseException {
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY;
        if (include != null || exclude != null) {
            config = config.withFiltering(include, exclude, false);
        }
        return parseToType(ordered ? XContentParser::mapOrdered : XContentParser::map, bytes, xContentType, config);
    }

    /**
     * Creates a parser based on the given {@link BytesReference} and uses {@param extractor} to get a parsed object.
     * @deprecated if {@param xContentType} is null, this method relies on auto-detection of content type.  Provide a non-null XContentType
     *             instead.
     */
    @Deprecated
    public static <T> Tuple<XContentType, T> parseToType(
        CheckedFunction<XContentParser, T, IOException> extractor,
        BytesReference bytes,
        @Nullable XContentType xContentType,
        @Nullable XContentParserConfiguration config
    ) throws ElasticsearchParseException {
        return parseToType(extractor, bytes, xContentType, config, XContentParserDecorator.NOOP);
    }

    public static <T> Tuple<XContentType, T> parseToType(
        CheckedFunction<XContentParser, T, IOException> extractor,
        BytesReference bytes,
        @Nullable XContentType xContentType,
        @Nullable XContentParserConfiguration config,
        XContentParserDecorator parserDecorator
    ) throws ElasticsearchParseException {
        config = config != null ? config : XContentParserConfiguration.EMPTY;
        try (
            XContentParser parser = parserDecorator.decorate(
                xContentType != null ? createParser(config, bytes, xContentType) : createParser(config, bytes)
            )
        ) {
            Tuple<XContentType, T> xContentTypeTTuple = new Tuple<>(parser.contentType(), extractor.apply(parser));
            return xContentTypeTTuple;
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to type", e);
        }
    }

    /**
     * Convert a string in some {@link XContent} format to a {@link Map}. Throws an {@link ElasticsearchParseException} if there is any
     * error.
     */
    public static Map<String, Object> convertToMap(XContent xContent, String string, boolean ordered) throws ElasticsearchParseException {
        try (XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, string)) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * The same as {@link XContentHelper#convertToMap(XContent, byte[], int, int, boolean, Set, Set)} but none of the
     * fields are filtered.
     */
    public static Map<String, Object> convertToMap(XContent xContent, InputStream input, boolean ordered)
        throws ElasticsearchParseException {
        return convertToMap(xContent, input, ordered, null, null);
    }

    /**
     * Convert a string in some {@link XContent} format to a {@link Map}. Throws an {@link ElasticsearchParseException} if there is any
     * error. Note that unlike {@link #convertToMap(BytesReference, boolean)}, this doesn't automatically uncompress the input.
     *
     * Additionally, fields may be included or excluded from the parsing.
     */
    public static Map<String, Object> convertToMap(
        XContent xContent,
        InputStream input,
        boolean ordered,
        @Nullable Set<String> include,
        @Nullable Set<String> exclude
    ) throws ElasticsearchParseException {
        try (
            XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY.withFiltering(include, exclude, false), input)
        ) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Convert a byte array in some {@link XContent} format to a {@link Map}. Throws an {@link ElasticsearchParseException} if there is any
     * error. Note that unlike {@link #convertToMap(BytesReference, boolean)}, this doesn't automatically uncompress the input.
     */
    public static Map<String, Object> convertToMap(XContent xContent, byte[] bytes, int offset, int length, boolean ordered)
        throws ElasticsearchParseException {
        return convertToMap(xContent, bytes, offset, length, ordered, null, null);
    }

    /**
     * Convert a byte array in some {@link XContent} format to a {@link Map}. Throws an {@link ElasticsearchParseException} if there is any
     * error. Note that unlike {@link #convertToMap(BytesReference, boolean)}, this doesn't automatically uncompress the input.
     *
     * Unlike {@link XContentHelper#convertToMap(XContent, byte[], int, int, boolean)} this optionally accepts fields to include or exclude
     * during XContent parsing.
     */
    public static Map<String, Object> convertToMap(
        XContent xContent,
        byte[] bytes,
        int offset,
        int length,
        boolean ordered,
        @Nullable Set<String> include,
        @Nullable Set<String> exclude
    ) throws ElasticsearchParseException {
        try (
            XContentParser parser = xContent.createParser(
                XContentParserConfiguration.EMPTY.withFiltering(include, exclude, false),
                bytes,
                offset,
                length
            )
        ) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    @Deprecated
    public static String convertToJson(BytesReference bytes, boolean reformatJson) throws IOException {
        return convertToJson(bytes, reformatJson, false);
    }

    @Deprecated
    public static String convertToJson(BytesReference bytes, boolean reformatJson, boolean prettyPrint) throws IOException {
        return convertToJson(bytes, reformatJson, prettyPrint, xContentType(bytes));
    }

    public static String convertToJson(BytesReference bytes, boolean reformatJson, XContentType xContentType) throws IOException {
        return convertToJson(bytes, reformatJson, false, xContentType);
    }

    /**
     * Accepts a JSON string, parses it and prints it without pretty-printing it. This is useful
     * where a piece of JSON is formatted for legibility, but needs to be stripped of unnecessary
     * whitespace e.g. for comparison in a test.
     *
     * @param json the JSON to format
     * @return reformatted JSON
     * @throws IOException if the reformatting fails, e.g. because the JSON is not well-formed
     */
    public static String stripWhitespace(String json) throws IOException {
        return convertToJson(new BytesArray(json), true, XContentType.JSON);
    }

    public static String convertToJson(BytesReference bytes, boolean reformatJson, boolean prettyPrint, XContentType xContentType)
        throws IOException {
        Objects.requireNonNull(xContentType);
        if (xContentType.canonical() == XContentType.JSON && reformatJson == false) {
            return bytes.utf8ToString();
        }

        try (var parser = createParserNotCompressed(XContentParserConfiguration.EMPTY, bytes, xContentType)) {
            return toJsonString(prettyPrint, parser);
        }
    }

    private static String toJsonString(boolean prettyPrint, XContentParser parser) throws IOException {
        parser.nextToken();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        if (prettyPrint) {
            builder.prettyPrint();
        }
        builder.copyCurrentStructure(parser);
        return Strings.toString(builder);
    }

    /**
     * Updates the provided changes into the source. If the key exists in the changes, it overrides the one in source
     * unless both are Maps, in which case it recursively updated it.
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
            if (source.containsKey(changesEntry.getKey()) == false) {
                // safe to copy, change does not exist in source
                source.put(changesEntry.getKey(), changesEntry.getValue());
                modified = true;
                continue;
            }
            Object old = source.get(changesEntry.getKey());
            if (old instanceof Map && changesEntry.getValue() instanceof Map) {
                // recursive merge maps
                modified |= update(
                    (Map<String, Object>) source.get(changesEntry.getKey()),
                    (Map<String, Object>) changesEntry.getValue(),
                    checkUpdatesAreUnequal && modified == false
                );
                continue;
            }
            // update the field
            source.put(changesEntry.getKey(), changesEntry.getValue());
            if (modified) {
                continue;
            }
            if (checkUpdatesAreUnequal == false) {
                modified = true;
                continue;
            }
            modified = Objects.equals(old, changesEntry.getValue()) == false;
        }
        return modified;
    }

    /**
     * Merges the defaults provided as the second parameter into the content of the first. Only does recursive merge for inner maps.
     */
    public static void mergeDefaults(Map<String, Object> content, Map<String, Object> defaults) {
        merge(content, defaults, null);
    }

    /**
     * Merges the map provided as the second parameter into the content of the first. Only does recursive merge for inner maps.
     * If a non-null {@link CustomMerge} is provided, it is applied whenever a merge is required, meaning - whenever both the first and
     * the second map has values for the same key. Otherwise, values from the first map will always have precedence, meaning - if the
     * first map contains a key, its value will not be overridden.
     * @param first the map which serves as the merge base
     * @param second the map of which contents are merged into the base map
     * @param customMerge a custom merge rule to apply whenever a key has concrete values (i.e. not a map or a collection) in both maps
     */
    public static void merge(Map<String, Object> first, Map<String, Object> second, @Nullable CustomMerge customMerge) {
        merge(null, first, second, customMerge);
    }

    /**
     * Merges the map provided as the second parameter into the content of the first. Only does recursive merge for inner maps.
     * If a non-null {@link CustomMerge} is provided, it is applied whenever a merge is required, meaning - whenever both the first and
     * the second map has values for the same key. Otherwise, values from the first map will always have precedence, meaning - if the
     * first map contains a key, its value will not be overridden.
     *
     * @param parent      used for recursion to maintain knowledge about the common parent of the currently merged sub-maps, if such exists
     * @param first       the map which serves as the merge base
     * @param second      the map of which contents are merged into the base map
     * @param customMerge a custom merge rule to apply whenever a key has concrete values (i.e. not a map or a collection) in both maps
     */
    public static void merge(
        @Nullable String parent,
        Map<String, Object> first,
        Map<String, Object> second,
        @Nullable CustomMerge customMerge
    ) {
        for (Map.Entry<String, Object> toMergeEntry : second.entrySet()) {
            if (first.containsKey(toMergeEntry.getKey()) == false) {
                // copy it over, it does not exist in the content
                first.put(toMergeEntry.getKey(), toMergeEntry.getValue());
            } else {
                // has values in both maps, merge compound ones (maps)
                Object baseValue = first.get(toMergeEntry.getKey());
                if (baseValue instanceof Map && toMergeEntry.getValue() instanceof Map) {
                    Map<String, Object> mergedValue = null;
                    if (customMerge != null) {
                        Object tmp = customMerge.merge(parent, toMergeEntry.getKey(), baseValue, toMergeEntry.getValue());
                        if (tmp != null && tmp instanceof Map == false) {
                            throw new IllegalStateException("merging of values for [" + toMergeEntry.getKey() + "] must yield a map");
                        }
                        mergedValue = (Map<String, Object>) tmp;
                    }
                    if (mergedValue != null) {
                        first.put(toMergeEntry.getKey(), mergedValue);
                    } else {
                        // if custom merge does not yield a value to be used, continue recursive merge
                        merge(
                            toMergeEntry.getKey(),
                            (Map<String, Object>) baseValue,
                            (Map<String, Object>) toMergeEntry.getValue(),
                            customMerge
                        );
                    }
                } else if (baseValue instanceof List && toMergeEntry.getValue() instanceof List) {
                    List<Object> listToMerge = (List<Object>) toMergeEntry.getValue();
                    List<Object> baseList = (List<Object>) baseValue;

                    if (allListValuesAreMapsOfOne(listToMerge) && allListValuesAreMapsOfOne(baseList)) {
                        // all are in the form of [ {"key1" : {}}, {"key2" : {}} ], merge based on keys
                        Map<String, Map<String, Object>> processed = new LinkedHashMap<>();
                        for (Object o : baseList) {
                            Map<String, Object> map = (Map<String, Object>) o;
                            Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                            processed.put(entry.getKey(), map);
                        }
                        for (Object o : listToMerge) {
                            Map<String, Object> map = (Map<String, Object>) o;
                            Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                            if (processed.containsKey(entry.getKey())) {
                                merge(toMergeEntry.getKey(), processed.get(entry.getKey()), map, customMerge);
                            } else {
                                // append the second list's entries after the first list's entries.
                                processed.put(entry.getKey(), map);
                            }
                        }

                        first.put(toMergeEntry.getKey(), new ArrayList<>(processed.values()));
                    } else {
                        // if both are lists, simply combine them, first the second list's values, then the first's
                        // just make sure not to add the same value twice
                        // custom merge is not applicable here
                        List<Object> mergedList = new ArrayList<>(listToMerge);

                        for (Object o : baseList) {
                            if (mergedList.contains(o) == false) {
                                mergedList.add(o);
                            }
                        }
                        first.put(toMergeEntry.getKey(), mergedList);
                    }
                } else if (customMerge != null) {
                    Object mergedValue = customMerge.merge(parent, toMergeEntry.getKey(), baseValue, toMergeEntry.getValue());
                    if (mergedValue != null) {
                        first.put(toMergeEntry.getKey(), mergedValue);
                    }
                }
            }
        }
    }

    private static boolean allListValuesAreMapsOfOne(List<Object> list) {
        for (Object o : list) {
            if ((o instanceof Map) == false) {
                return false;
            }
            if (((Map) o).size() != 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * A {@code FunctionalInterface} that can be used in order to customize map merges.
     */
    @FunctionalInterface
    public interface CustomMerge {
        /**
         * Based on the provided arguments, compute a value to use for the given key as a merge result.
         * If this method returns a non-{@code null} value, then the merge result will replace the original value of the provided key in
         * the base map.
         * If this method returns {@code null}, then:
         * <ul>
         *     <li> if the values are of map type, the old and new values will be merged recursively
         *     <li> otherwise, the original value will be maintained
         * </ul>
         * This method doesn't throw a checked exception, but it is expected that illegal merges will result in a {@link RuntimeException}.
         * @param parent merged field's parent
         * @param key merged field's name
         * @param oldValue original value of the provided key
         * @param newValue the new value of the provided key which is to be merged with the original
         * @return the merged value to use for the given key, or {@code null} if there is no custom merge result for it. If {@code null}
         * is returned, the algorithm will live the original value as is, unless it is a map, in which case the new map will be merged
         * into the old map recursively.
         */
        @Nullable
        Object merge(String parent, String key, Object oldValue, Object newValue);
    }

    /**
     * Writes a "raw" (bytes) field, handling cases where the bytes are compressed, and tries to optimize writing using
     * {@link XContentBuilder#rawField(String, InputStream)}.
     * @deprecated use {@link #writeRawField(String, BytesReference, XContentType, XContentBuilder, Params)} to avoid content type
     * auto-detection
     */
    @Deprecated
    public static void writeRawField(String field, BytesReference source, XContentBuilder builder, ToXContent.Params params)
        throws IOException {
        Compressor compressor = CompressorFactory.compressor(source);
        if (compressor != null) {
            try (InputStream compressedStreamInput = compressor.threadLocalInputStream(source.streamInput())) {
                builder.rawField(field, compressedStreamInput);
            }
        } else {
            try (InputStream stream = source.streamInput()) {
                builder.rawField(field, stream);
            }
        }
    }

    /**
     * Writes a "raw" (bytes) field, handling cases where the bytes are compressed, and tries to optimize writing using
     * {@link XContentBuilder#rawField(String, InputStream, XContentType)}.
     */
    public static void writeRawField(
        String field,
        BytesReference source,
        XContentType xContentType,
        XContentBuilder builder,
        ToXContent.Params params
    ) throws IOException {
        Objects.requireNonNull(xContentType);
        Compressor compressor = CompressorFactory.compressor(source);
        if (compressor != null) {
            try (InputStream compressedStreamInput = compressor.threadLocalInputStream(source.streamInput())) {
                builder.rawField(field, compressedStreamInput, xContentType);
            }
        } else {
            try (InputStream stream = source.streamInput()) {
                builder.rawField(field, stream, xContentType);
            }
        }
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     */
    public static BytesReference toXContent(ToXContent toXContent, XContentType xContentType, boolean humanReadable) throws IOException {
        return toXContent(toXContent, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ChunkedToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     */
    public static BytesReference toXContent(ChunkedToXContent toXContent, XContentType xContentType, boolean humanReadable)
        throws IOException {
        return toXContent(ChunkedToXContent.wrapAsToXContent(toXContent), xContentType, humanReadable);
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     */
    public static BytesReference toXContent(ToXContent toXContent, XContentType xContentType, Params params, boolean humanReadable)
        throws IOException {
        return toXContent(toXContent, xContentType, RestApiVersion.current(), params, humanReadable);
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     */
    public static BytesReference toXContent(
        ToXContent toXContent,
        XContentType xContentType,
        RestApiVersion restApiVersion,
        Params params,
        boolean humanReadable
    ) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent(), restApiVersion)) {
            builder.humanReadable(humanReadable);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ChunkedToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     */
    public static BytesReference toXContent(ChunkedToXContent toXContent, XContentType xContentType, Params params, boolean humanReadable)
        throws IOException {
        return toXContent(ChunkedToXContent.wrapAsToXContent(toXContent), xContentType, params, humanReadable);
    }

    /**
     * Guesses the content type based on the provided bytes which may be compressed.
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static XContentType xContentTypeMayCompressed(BytesReference bytes) {
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor != null) {
            try {
                InputStream compressedStreamInput = compressor.threadLocalInputStream(bytes.streamInput());
                if (compressedStreamInput.markSupported() == false) {
                    compressedStreamInput = new BufferedInputStream(compressedStreamInput);
                }
                return XContentFactory.xContentType(compressedStreamInput);
            } catch (IOException e) {
                assert false : "Should not happen, we're just reading bytes from memory";
                throw new UncheckedIOException(e);
            }
        } else {
            return XContentHelper.xContentType(bytes);
        }
    }

    /**
     * Guesses the content type based on the provided bytes.
     *
     * @deprecated the content type should not be guessed except for few cases where we effectively don't know the content type.
     * The REST layer should move to reading the Content-Type header instead. There are other places where auto-detection may be needed.
     * This method is deprecated to prevent usages of it from spreading further without specific reasons.
     */
    @Deprecated
    public static XContentType xContentType(BytesReference bytes) {
        if (bytes.hasArray()) {
            return XContentFactory.xContentType(bytes.array(), bytes.arrayOffset(), bytes.length());
        }
        try {
            final InputStream inputStream = bytes.streamInput();
            assert inputStream.markSupported();
            return XContentFactory.xContentType(inputStream);
        } catch (IOException e) {
            assert false : "Should not happen, we're just reading bytes from memory";
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the contents of an object as an unparsed BytesReference
     *
     * This is useful for things like mappings where we're copying bytes around but don't
     * actually need to parse their contents, and so avoids building large maps of maps
     * unnecessarily
     */
    public static BytesReference childBytes(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "Expected [START_OBJECT] but got [" + parser.currentToken() + "]"
                );
            }
        }
        XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent());
        builder.copyCurrentStructure(parser);
        return BytesReference.bytes(builder);
    }

    /**
     * Serialises new XContentType VND_ values in a bwc manner
     * TODO remove in ES v9
     * @param out stream output of the destination node
     * @param xContentType an instance to serialize
     */
    public static void writeTo(StreamOutput out, XContentType xContentType) throws IOException {
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            // when sending an enumeration to <v8 node it does not have new VND_ XContentType instances
            out.writeVInt(xContentType.canonical().ordinal());
        } else {
            out.writeVInt(xContentType.ordinal());
        }
    }

    /**
     * Convenience method that creates a {@link XContentParser} from a content map so that it can be passed to
     * existing REST based code for input parsing.
     *
     * @param config XContentParserConfiguration for this mapper
     * @param source the operator content as a map
     * @return
     */
    public static XContentParser mapToXContentParser(XContentParserConfiguration config, Map<String, ?> source) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.map(source);
            return createParserNotCompressed(config, BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }
}
