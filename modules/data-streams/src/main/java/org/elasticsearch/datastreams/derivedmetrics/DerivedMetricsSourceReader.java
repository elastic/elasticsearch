/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Reads the handful of source paths that derived metrics need out of a document that is being written.
 *
 * <p>The document's source is parsed with a filter restricted to the required paths, the same technique
 * {@link org.elasticsearch.cluster.routing.IndexRouting} uses to extract routing fields. Parsing a filtered slice rather than the whole
 * document is what keeps the write path affordable on a stream that only cares about a few dimensions.
 */
public final class DerivedMetricsSourceReader {

    private static final Logger logger = LogManager.getLogger(DerivedMetricsSourceReader.class);

    private DerivedMetricsSourceReader() {}

    /**
     * Returns the requested paths of the document's source, or null when the source could not be read. Callers treat a null source as
     * "no values available" rather than as an error, because a malformed document must never fail the write it is derived from.
     *
     * @param sourceFilter the filter restricted to the required paths, compiled once per configuration by {@link CompiledDerivedMetrics}.
     *                     Building it here instead would run {@code FilterPath.compile} for every document.
     */
    public static Map<String, Object> read(ParsedDocument parsedDocument, XContentParserConfiguration sourceFilter) {
        try (XContentParser parser = parsedDocument.source().parser(sourceFilter)) {
            return parser.map();
        } catch (IOException | RuntimeException e) {
            logger.debug(() -> "unable to read source for derived metrics", e);
            return null;
        }
    }

    /**
     * The value at the given path rendered as a dimension value, or null when the path is absent or holds something that cannot be a
     * single dimension value such as an object or a multi-valued field.
     */
    public static String stringValue(Map<String, Object> source, String[] path) {
        Object value = XContentMapValues.extractValue(source, path);
        if (value == null || value instanceof Map<?, ?> || value instanceof Collection<?>) {
            return null;
        }
        return String.valueOf(value);
    }

    /**
     * The numeric value at the given path, or null when the path is absent or does not hold a number.
     */
    public static Double numericValue(Map<String, Object> source, String[] path) {
        Object value = XContentMapValues.extractValue(source, path);
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String string) {
            try {
                return Double.valueOf(string);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
