/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;

/**
 * Decides whether an index is eligible for the ColumNAR doc values codec, based on index settings and index
 * version. Selection is gated behind the {@code columnar_codec} feature flag while the format is under
 * development. The per-field decision of which fields use the codec is made by the caller.
 */
public final class ColumnarDocValuesFormatSelector {

    public static final FeatureFlag COLUMNAR_CODEC_FEATURE_FLAG = new FeatureFlag("columnar_codec");

    private ColumnarDocValuesFormatSelector() {}

    /**
     * @param indexSettings the index settings to base the decision on
     * @return {@code true} if the ColumNAR codec is eligible for the given index
     */
    public static boolean useColumnarCodec(final IndexSettings indexSettings) {
        return COLUMNAR_CODEC_FEATURE_FLAG.isEnabled()
            && indexSettings.getMode().isStrictColumnar()
            && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.COLUMNAR_DOC_VALUES_CODEC_FEATURE_FLAG)
            && indexSettings.isColumnarCodecEnabled();
    }
}
