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
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;

/**
 * Decides whether an index is eligible for the ColumNAR doc values codec, based on its index mode and index
 * version. The per-field decision of which fields use the codec is made by the caller.
 *
 * <p>String fields of an eligible index always use the codec. Numeric fields additionally need the
 * {@code columnar_codec_numeric} feature flag and {@link IndexSettings#COLUMNAR_CODEC_NUMERIC_ENABLED_SETTING}
 * while that format is under development.
 */
public final class ColumnarDocValuesFormatSelector {

    public static final FeatureFlag COLUMNAR_CODEC_NUMERIC_FEATURE_FLAG = new FeatureFlag("columnar_codec_numeric");

    private ColumnarDocValuesFormatSelector() {}

    /**
     * The codec for an eligible index, or {@code null} for one that is not.
     *
     * <p>How a string column is written is a per-field choice, so the format is built with a bridge to the
     * mapper rather than with one set of options for every field: what suits a field of a handful of repeated
     * terms is not what suits one whose values are long and all different. The format is therefore built per
     * index, since the bridge closes over that index's mapping.
     *
     * @param indexSettings the index settings to base the decision on
     * @param fieldTypes    what kind of column each field routed here is written as
     * @param stringOptions what to write a string column with, asked once per field
     */
    @Nullable
    public static DocValuesFormat select(
        final IndexSettings indexSettings,
        final ColumnarFieldTypeSelector fieldTypes,
        final StringColumnOptionsSelector stringOptions
    ) {
        if (useColumnarCodec(indexSettings) == false) {
            return null;
        }
        return new ColumNARDocValuesFormat(
            (fieldName, type) -> NumericPipeline::defaultPipeline,
            fieldTypes,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            stringOptions
        );
    }

    /**
     * @param indexSettings the index settings to base the decision on
     * @return {@code true} if the ColumNAR codec is eligible for the given index
     */
    public static boolean useColumnarCodec(final IndexSettings indexSettings) {
        return indexSettings.getMode().isStrictColumnar()
            && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG);
    }

    /**
     * @param indexSettings the index settings to base the decision on
     * @return {@code true} if numeric fields of the given index are eligible for the ColumNAR codec
     */
    public static boolean useColumnarNumericCodec(final IndexSettings indexSettings) {
        return COLUMNAR_CODEC_NUMERIC_FEATURE_FLAG.isEnabled()
            && useColumnarCodec(indexSettings)
            && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.COLUMNAR_CODEC_NUMERIC_FF)
            && indexSettings.isColumnarCodecNumericEnabled();
    }
}
