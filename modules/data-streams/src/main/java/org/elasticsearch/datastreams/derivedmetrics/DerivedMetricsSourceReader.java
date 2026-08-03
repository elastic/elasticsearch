/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsSourcePaths.Node;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Pulls the handful of {@code _source} values derived metrics need out of a document that is being written.
 *
 * <p>The document is walked once and the values are written straight into a caller-owned array indexed by slot. Nothing intermediate is
 * built: no filtered copy of the source, no map of the values, no path strings. Any field that no configured path leads to has its whole
 * subtree skipped, which is almost every field in a real document.
 *
 * <p>That matters because this runs once per document on the indexing thread. Building a filtered map first, which is the obvious way and
 * is how this used to work, cost several kilobytes per document — more, in fact, than parsing the document with no filter at all, because
 * the filtering machinery is not free either.
 */
public final class DerivedMetricsSourceReader {

    private static final Logger logger = LogManager.getLogger(DerivedMetricsSourceReader.class);

    private DerivedMetricsSourceReader() {}

    /**
     * Fills {@code values} with what the document has at each configured path, leaving null where it has nothing.
     *
     * <p>A malformed document must never fail the write it is derived from, so a parse failure leaves the values as they are and is
     * logged at debug rather than propagating.
     *
     * @return whether the document was read. False means no values are available, not that the document had none.
     */
    public static boolean read(ParsedDocument parsedDocument, DerivedMetricsSourcePaths paths, Object[] values) {
        try (XContentParser parser = parsedDocument.source().parser(XContentParserConfiguration.EMPTY)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                return false;
            }
            readObject(parser, paths.root(), values);
            return true;
        } catch (IOException | RuntimeException e) {
            logger.debug(() -> "unable to read source for derived metrics", e);
            return false;
        }
    }

    /**
     * Reads the fields of one object, descending only where the trie says something below is wanted.
     */
    private static void readObject(XContentParser parser, Node node, Object[] values) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT && token != null) {
            if (token != XContentParser.Token.FIELD_NAME) {
                continue;
            }
            String name = parser.currentName();
            parser.nextToken();
            // A source may write a nested path either as nested objects or as one dotted field name, and both mean the same path, so the
            // field name is walked segment by segment rather than looked up whole.
            Node next = descend(node, name);
            if (next == null) {
                parser.skipChildren();
                continue;
            }
            readValue(parser, next, values);
        }
    }

    private static Node descend(Node node, String name) {
        Node next = node.child(name);
        if (next != null || name.indexOf('.') < 0) {
            return next;
        }
        for (String segment : name.split("\\.")) {
            node = node.child(segment);
            if (node == null) {
                return null;
            }
        }
        return node;
    }

    private static void readValue(XContentParser parser, Node node, Object[] values) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT -> {
                if (node.hasChildren()) {
                    readObject(parser, node, values);
                } else {
                    // the path names an object rather than a value, which is not something a dimension or a metric value can use
                    parser.skipChildren();
                }
            }
            case START_ARRAY -> {
                if (node.slot() >= 0) {
                    values[node.slot()] = readArray(parser);
                } else {
                    readArrayForChildren(parser, node, values);
                }
            }
            case VALUE_NULL -> {
            }
            default -> {
                if (node.slot() >= 0) {
                    values[node.slot()] = parser.objectText();
                }
            }
        }
    }

    /**
     * Collects a multi-valued field into a list, which predicates match against element by element. Allocating here is fine: a dimension
     * or predicate field is almost never an array, and when it is there is nothing cheaper to represent it with.
     */
    private static List<Object> readArray(XContentParser parser) throws IOException {
        List<Object> collected = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY && token != null) {
            if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else if (token != XContentParser.Token.VALUE_NULL) {
                collected.add(parser.objectText());
            }
        }
        return collected;
    }

    /**
     * An array below a path that only leads to deeper values, such as {@code host.name} where the document holds an array of hosts. Each
     * element is read as an object so the deeper values are still found.
     */
    private static void readArrayForChildren(XContentParser parser, Node node, Object[] values) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY && token != null) {
            if (token == XContentParser.Token.START_OBJECT) {
                readObject(parser, node, values);
            } else {
                parser.skipChildren();
            }
        }
    }

    /**
     * The value at the given slot rendered as a dimension value, or null when the document had nothing there or had something that cannot
     * be a single dimension value, such as an object or a multi-valued field.
     */
    public static String stringValue(Object[] values, int slot) {
        Object value = values[slot];
        if (value == null || value instanceof Collection<?>) {
            return null;
        }
        return value instanceof String string ? string : String.valueOf(value);
    }

    /**
     * The numeric value at the given slot, or {@code NaN} when the document had nothing there or had something that is not a number.
     */
    public static double numericValue(Object[] values, int slot) {
        Object value = values[slot];
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String string) {
            try {
                return Double.parseDouble(string);
            } catch (NumberFormatException e) {
                return Double.NaN;
            }
        }
        return Double.NaN;
    }
}
