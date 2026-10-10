/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A field's whole contribution to the {@code columnar_stored} {@code _source} blob on the columnar batch direct path: it holds the
 * field's {@link SourceColumn} layers in the same order {@link CompositeSyntheticFieldLoader} holds its layers (the doc-values layer
 * first, then the {@code ignore_above}, {@code ignore_malformed} and {@code on_failure} fallbacks) and applies the loader's
 * scalar-or-array rule exactly once, here, so the two cannot drift.
 *
 * <p>The rule, copied from {@link CompositeSyntheticFieldLoader#write}: sum the value counts of every layer; write nothing when the
 * total is zero, a single scalar when it is one, and an array otherwise. The layers are written in order either way, which is what
 * keeps leaf-array order and places a value dropped by {@code ignore_above} after the indexed values rather than at its original
 * position — the documented {@code columnar_stored} behaviour.</p>
 */
final class CompositeSourceColumn {

    private final String leafName;
    private final String fullPath;
    private final SourceColumn[] layers;

    /**
     * @param leafName the key written into the blob for this field; matches the {@code leafFieldName} the field's
     *                 {@link CompositeSyntheticFieldLoader} writes (the dotted leaf name in the flat columnar modes)
     * @param fullPath the field's full path; the direct path sorts fields by it, matching the {@code TreeMap} keyed by
     *                 {@code fullFieldName} that the root {@link ObjectMapper} loader sorts by
     * @param layers   the field's layers in loader order; must be non-empty
     */
    CompositeSourceColumn(String leafName, String fullPath, SourceColumn... layers) {
        assert layers.length > 0 : "a composite source column must have at least one layer";
        this.leafName = leafName;
        this.fullPath = fullPath;
        this.layers = layers;
    }

    /** The field's full path, the key the direct path sorts fields by. */
    String fullPath() {
        return fullPath;
    }

    /**
     * Writes this field's entry for {@code doc} into {@code builder}, applying the loader's scalar-or-array rule. Writes nothing when
     * the field contributes no value for this document, mirroring {@link CompositeSyntheticFieldLoader#write}.
     */
    void write(int doc, XContentBuilder builder) throws IOException {
        long totalCount = 0;
        for (SourceColumn layer : layers) {
            totalCount += layer.valueCount(doc);
        }

        if (totalCount == 0) {
            return;
        }

        if (totalCount == 1) {
            builder.field(leafName);
            for (SourceColumn layer : layers) {
                layer.writeValues(doc, builder);
            }
            return;
        }

        builder.startArray(leafName);
        for (SourceColumn layer : layers) {
            layer.writeValues(doc, builder);
        }
        builder.endArray();
    }
}
