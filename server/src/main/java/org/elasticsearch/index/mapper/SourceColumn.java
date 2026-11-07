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
import java.util.Set;

/**
 * One field's contribution to the {@code columnar_stored} {@code _source} blob on the columnar batch path, read straight from the
 * columns the field's mapper has just produced rather than from a synthetic source loader over a per-document Lucene reader.
 *
 * <p>A mapper that registers one of these from {@code doMapColumnBatch} ({@link BatchMappingContext#addSourceColumn}) promises that
 * for every document the layers write exactly the values, in exactly the order, that the field's {@link CompositeSyntheticFieldLoader}
 * would write for the same Lucene fields. To keep that promise cheap to make, a layer reads the mapper's own output data — the keyword
 * payload column, the number column in source order — which is what the loader decodes on the read side as well, and the one rule
 * that shapes a field's output (one value is a scalar, anything else an array) is {@link CompositeSyntheticFieldLoader#writeField
 * shared} with the loader rather than copied.
 *
 * <p>Layers are listed in loader order: the doc-values layer first, then the fallback layers ({@code ignore_above} originals and so
 * on), so that a value the row path diverted to a fallback column lands after the indexed values, as it does when the blob is read
 * back from a real segment.
 */
public final class SourceColumn {

    /**
     * One layer of a field's values for the current document, mirroring {@link CompositeSyntheticFieldLoader.Layer}: it reports how
     * many values it holds, then writes them. {@link #advance} is called for documents in increasing order, so a layer may keep a
     * forward cursor over its column.
     */
    public interface Layer extends CompositeSyntheticFieldLoader.ValuesWriter {
        /** Positions the layer on {@code doc}. Documents are visited in increasing order, each at most once. */
        void advance(int doc) throws IOException;

        /**
         * The number of values this layer contributes to the current document, with the same meaning as
         * {@link CompositeSyntheticFieldLoader.Layer#valueCount()}: the total over all layers decides whether the field is written as
         * a scalar ({@code 1}) or an array (anything larger), so a layer that must force an array reports more than it writes.
         */
        long valueCount();
    }

    /**
     * The {@link CompositeSyntheticFieldLoader.Layer} implementations whose output a {@link SourceColumn} knows how to reproduce. A
     * field whose loader uses any other layer stays on the loader path, so a layer added to a mapper later is correct before anyone
     * teaches the batch path about it. The match is on the exact class: a subclass may decode its values differently.
     */
    private static final Set<Class<?>> KNOWN_LOADER_LAYERS = Set.of(
        ColumnarPayloadBinaryDocValuesSyntheticFieldLoaderLayer.class,
        BinaryDocValuesSyntheticFieldLoaderLayer.class,
        ArrayOrderBinaryDocValuesSyntheticFieldLoaderLayer.class,
        SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer.class,
        SortedNumericDocValuesSyntheticFieldLoaderLayer.class
    );

    /**
     * Whether {@code loader} is a {@link CompositeSyntheticFieldLoader} made only of {@link #KNOWN_LOADER_LAYERS}, or of the failure
     * layers the batch path leaves empty ({@link CompositeSyntheticFieldLoader#isBinaryFailureValuesLayer}). Called with the loader of
     * a field whose mapper claims direct source support, as the check that the claim still describes the loader.
     */
    static boolean hasOnlyKnownLayers(SourceLoader.SyntheticFieldLoader loader) {
        if (loader instanceof CompositeSyntheticFieldLoader == false) {
            return false;
        }
        for (CompositeSyntheticFieldLoader.Layer layer : ((CompositeSyntheticFieldLoader) loader).layers()) {
            if (KNOWN_LOADER_LAYERS.contains(layer.getClass()) == false
                && CompositeSyntheticFieldLoader.isBinaryFailureValuesLayer(layer) == false) {
                return false;
            }
        }
        return true;
    }

    private final String fullPath;
    private final String leafName;
    private final Layer[] layers;

    /**
     * @param fullPath the field's full path; the blob lists fields in this order, as {@link ObjectMapper}'s loader does
     * @param leafName the key the field is written under, which is the loader's {@code leafFieldName}
     * @param layers   the field's layers in loader order
     */
    public SourceColumn(String fullPath, String leafName, Layer... layers) {
        this.fullPath = fullPath;
        this.leafName = leafName;
        this.layers = layers;
    }

    public String fullPath() {
        return fullPath;
    }

    /**
     * Writes the field for {@code doc} into {@code b}, or nothing when no layer holds a value for it. Documents must be passed in
     * increasing order.
     */
    public void write(int doc, XContentBuilder b) throws IOException {
        long totalCount = 0;
        for (Layer layer : layers) {
            layer.advance(doc);
            totalCount += layer.valueCount();
        }
        CompositeSyntheticFieldLoader.writeField(b, leafName, totalCount, layers);
    }
}
