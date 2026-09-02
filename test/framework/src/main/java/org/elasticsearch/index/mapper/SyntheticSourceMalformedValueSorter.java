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
 * Sorts malformed values in the order emitted by the synthetic source loader when the index is <em>not</em> in a strict-columnar mode
 * (i.e. {@code IndexMode.isStrictColumnar() == false}).
 *
 * <p>In non-strict-columnar indices, {@code ignore_malformed} values are written to the {@code ._ignore_malformed} sidecar column with
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ValueOrdering#SORTED}, so the loader emits them sorted by
 * their encoded {@link BytesRef} (type byte + value bytes). This class reproduces that sort order in test expectations using
 * {@link XContentDataHelper#encodeToken(XContentParser)} so the encoding is identical to the index.
 *
 * <p>In strict-columnar indices ({@code IndexMode.COLUMNAR}, {@code IndexMode.LOGSDB_COLUMNAR}), malformed values are instead written to
 * the {@code ._on_failure} sidecar column with
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ValueOrdering#UNSORTED}, so the loader emits them in document
 * encounter order. Test expectations for that path must therefore preserve encounter order and must <em>not</em> use this sorter.
 */
public final class SyntheticSourceMalformedValueSorter {

    private SyntheticSourceMalformedValueSorter() {}

    /**
     * Returns a comparator that orders malformed values in the order emitted by the loader in non-strict-columnar indices
     * (sorted by encoded {@link BytesRef}). Do not use this for strict-columnar indices, where encounter order is preserved.
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
     * Sorts the given list of malformed values in place in the order emitted by the loader in non-strict-columnar indices.
     * Do not use this for strict-columnar indices, where encounter order is preserved.
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
