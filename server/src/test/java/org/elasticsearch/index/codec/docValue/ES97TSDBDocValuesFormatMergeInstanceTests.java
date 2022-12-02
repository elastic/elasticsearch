/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.codec.docValue;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene94.Lucene94Codec;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.IndexSettings.TIME_SERIES_END_TIME;
import static org.elasticsearch.index.IndexSettings.TIME_SERIES_START_TIME;

/** Tests ES97TSDBDocValuesFormat's merge instance. */
public class ES97TSDBDocValuesFormatMergeInstanceTests extends ES97TSDBDocValuesFormatTests {

    @Override
    protected Codec getCodec() {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.DAYS.toMillis(1);
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .put(TIME_SERIES_START_TIME.getKey(), startTime)
            .put(TIME_SERIES_END_TIME.getKey(), endTime)
            .build();
        return new PerFieldMapperCodec(
            Lucene94Codec.Mode.BEST_SPEED,
            null,
            BigArrays.NON_RECYCLING_INSTANCE,
            IndexSettingsModule.newIndexSettings("test", settings)
        );
    }

    @Override
    protected boolean shouldTestMergeInstance() {
        return true;
    }
}
