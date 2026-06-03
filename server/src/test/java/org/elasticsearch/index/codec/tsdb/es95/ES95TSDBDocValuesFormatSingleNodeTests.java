/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesFormatSingleNodeTests;

import static org.hamcrest.Matchers.instanceOf;

public class ES95TSDBDocValuesFormatSingleNodeTests extends AbstractTSDBDocValuesFormatSingleNodeTests {

    @Override
    protected void assumeCodecSelected() {
        assumeTrue("ES95 feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings tsdbSettings() {
        return Settings.builder()
            .put(super.tsdbSettings())
            .put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Override
    protected void assertTSDBDocValuesFormat(final DocValuesFormat format, final String field) {
        assertThat("field [" + field + "] should use ES95 TSDB doc values format", format, instanceOf(ES95TSDBDocValuesFormat.class));
    }

    @Override
    protected void assertStandardIndexDocValuesFormat(final DocValuesFormat format, final String field) {
        assertFalse("standard index should not use ES95 for field [" + field + "]", format instanceof ES95TSDBDocValuesFormat);
    }

    @Override
    protected String expectedCodecName() {
        // NOTE: when the adaptive ordinal blocks feature flag is enabled (snapshot
        // builds by default) the selector dispatches to ES95AdaptiveTSDBDocValuesFormat,
        // whose distinct CODEC_NAME is written into segment metadata. Mirror the
        // selector's logic here so the assertion stays accurate in both flavors.
        if (ES95TSDBDocValuesFormat.ADAPTIVE_ORDINAL_BLOCKS_FEATURE_FLAG.isEnabled()) {
            return ES95AdaptiveTSDBDocValuesFormat.CODEC_NAME;
        }
        return ES95TSDBDocValuesFormat.CODEC_NAME;
    }
}
