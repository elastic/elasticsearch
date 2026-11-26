/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldTypeSelector;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;

/**
 * Decides whether an index is eligible for the ColumNAR doc values codec, based on its index mode and index
 * version. The per-field decision of which fields use the codec is made by the caller.
 */
public final class ColumnarDocValuesFormatSelector {

    private ColumnarDocValuesFormatSelector() {}

    /**
     * The codec for an eligible index, or {@code null} for one that is not.
     *
     * <p>How a column is written is a per-field choice, so the format is built with bridges to the mapper
     * rather than with one set of options for every field: what suits a field of a handful of repeated
     * terms is not what suits one whose values are long and all different, and a counter compresses
     * differently from a gauge. The format is therefore built per index, since the bridges close over that
     * index's mapping.
     *
     * @param indexSettings the index settings to base the decision on
     * @param fieldTypes    what kind of column each field routed here is written as
     * @param pipelines     which pipeline to encode a numeric column with, asked once per field
     * @param stringOptions what to write a string column with, asked once per field
     */
    @Nullable
    public static DocValuesFormat select(
        final IndexSettings indexSettings,
        final ColumnarFieldTypeSelector fieldTypes,
        final NumericPipelineSelector pipelines,
        final StringColumnOptionsSelector stringOptions
    ) {
        if (useColumnarCodec(indexSettings) == false) {
            return null;
        }
        return new ColumNARDocValuesFormat(pipelines, fieldTypes, ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE, stringOptions);
    }

    /**
     * @param indexSettings the index settings to base the decision on
     * @return {@code true} if the ColumNAR codec is eligible for the given index
     */
    public static boolean useColumnarCodec(final IndexSettings indexSettings) {
        return indexSettings.getMode().isStrictColumnar()
            && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG);
    }
}
