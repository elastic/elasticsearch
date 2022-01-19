/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import static org.elasticsearch.xpack.rollup.v2.TransportRollupIndexerAction.isRollupTimeSeries;
import static org.hamcrest.Matchers.equalTo;

public class TransportRollupIndexerActionTests extends RollupTestCase {

    public void testIsRollupTimeSeries() {
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            new TermsGroupConfig(TimeSeriesIdFieldMapper.NAME)
        );

        assertThat(isRollupTimeSeries(newTimeSeriesIndexSettings(), groupConfig), equalTo(true));
    }

    public void testRollupNullGroupConfig() {
        assertThat(isRollupTimeSeries(newTimeSeriesIndexSettings(), null), equalTo(false));
    }

    public void testRollupStandardIndex() {
        IndexSettings indexSettings = newIndexSettings(Settings.EMPTY);
        RollupActionGroupConfig groupConfig = ConfigTestHelpers.randomRollupActionGroupConfig(random());
        assertThat(isRollupTimeSeries(indexSettings, groupConfig), equalTo(false));
    }

    public void testRollupNotTimestamp() {
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                randomAlphaOfLength(5),
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            null
        );
        assertThat(isRollupTimeSeries(newTimeSeriesIndexSettings(), groupConfig), equalTo(false));
    }

    public void testRollupNoTerms() {
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            null
        );
        assertThat(isRollupTimeSeries(newTimeSeriesIndexSettings(), groupConfig), equalTo(false));
    }

    public void testRollupHistogram() {
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            new HistogramGroupConfig(randomLongBetween(10, 100), randomAlphaOfLength(5)),
            new TermsGroupConfig(TimeSeriesIdFieldMapper.NAME)
        );
        assertThat(isRollupTimeSeries(newTimeSeriesIndexSettings(), groupConfig), equalTo(false));
    }

    public void testRollupNotTsid() {
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            new TermsGroupConfig(randomAlphaOfLength(5))
        );

        assertThat(isRollupTimeSeries(newTimeSeriesIndexSettings(), groupConfig), equalTo(false));
    }

    public void testRollupTooManyTerms() {
        RollupActionGroupConfig groupConfig = new RollupActionGroupConfig(
            new RollupActionDateHistogramGroupConfig.FixedInterval(
                DataStreamTimestampFieldMapper.DEFAULT_PATH,
                ConfigTestHelpers.randomInterval(),
                randomZone().getId()
            ),
            null,
            new TermsGroupConfig(TimeSeriesIdFieldMapper.NAME, randomAlphaOfLength(5))
        );

        assertThat(isRollupTimeSeries(newTimeSeriesIndexSettings(), groupConfig), equalTo(false));
    }

    private IndexSettings newTimeSeriesIndexSettings() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();
        return newIndexSettings(settings);
    }
}
