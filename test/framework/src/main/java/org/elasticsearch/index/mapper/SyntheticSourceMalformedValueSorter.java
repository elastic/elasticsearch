/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * Sorts malformed values in the same order as the synthetic source loader, which stores them in binary doc values and sorts by encoded
 * BytesRef (type byte + value bytes). Uses {@link XContentDataHelper#encodeToken(XContentParser)} so the sort order always matches the
 * index.
 */
public final class SyntheticSourceMalformedValueSorter {

    private SyntheticSourceMalformedValueSorter() {}

    /**
     * Returns a comparator that orders malformed values in the same order as the loader.
     */
    public static Comparator<Object> comparator() {
        return (a, b) -> {
            try {
                return encoded(a).compareTo(encoded(b));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };
    }

    /**
     * Sorts the given list of malformed values in place in loader order.
     */
    public static void sort(List<Object> malformedValues) {
        malformedValues.sort(comparator());
    }

    /**
     * Encode a value the same way the index does, by round-tripping through JSON and using {@link XContentDataHelper#encodeToken}.
     */
    private static BytesRef encoded(Object v) throws IOException {
        BytesReference ref = BytesReference.bytes(JsonXContent.contentBuilder().startObject().field("v", v).endObject());
        try (XContentParser p = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, ref.streamInput())) {
            p.nextToken();
            p.nextToken();
            p.nextToken();
            return XContentDataHelper.encodeToken(p);
        }
    }
}
