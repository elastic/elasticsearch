/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesFormatSingleNodeTests;

import static org.hamcrest.Matchers.startsWith;

public class TSDBDocValuesFormatSingleNodeTests extends AbstractTSDBDocValuesFormatSingleNodeTests {

    @Override
    protected Settings tsdbSettings() {
        return Settings.builder()
            .put(super.tsdbSettings())
            .put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), false)
            .build();
    }

    @Override
    protected void assertTSDBDocValuesFormat(final DocValuesFormat format, final String field) {
        assertThat("field [" + field + "] should use ES819 TSDB doc values format", format.getName(), startsWith("ES819"));
    }

    @Override
    protected void assertStandardIndexDocValuesFormat(final DocValuesFormat format, final String field) {
        assertThat(
            "field [" + field + "] should use ES819 TSDB doc values format for standard index",
            format.getName(),
            startsWith("ES819")
        );
    }

    @Override
    protected String expectedCodecName() {
        // NOTE: v3 is the latest ES819 version and the one written for current index versions
        return ES819Version3TSDBDocValuesFormat.CODEC_NAME;
    }
}
