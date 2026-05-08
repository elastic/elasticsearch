/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.postings.ES812PostingsFormat;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PerFieldMapperCodecTests extends ESTestCase {

    private static final String METRIC_MAPPING = """
        {
            "_data_stream_timestamp": {
                "enabled": true
            },
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "gauge": {
                    "type": "long"
                }
            }
        }
        """;

    private static final String MULTI_METRIC_MAPPING = """
        {
            "_data_stream_timestamp": {
                "enabled": true
            },
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "counter": {
                    "type": "long"
                },
                "gauge": {
                    "type": "long"
                }
            }
        }
        """;

    private static final String LOGS_MAPPING = """
        {
            "_data_stream_timestamp": {
                "enabled": true
            },
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "hostname": {
                    "type": "keyword"
                },
                "response_size": {
                    "type": "long"
                },
                "message": {
                    "type": "text"
                }
            }
        }
        """;

    public void testUseBloomFilter() throws IOException {
        final boolean timeSeries = randomBoolean();
        final boolean randomSyntheticId = syntheticId(timeSeries);
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(false, timeSeries, false, randomSyntheticId);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(true));
        assertThat(
            perFieldMapperCodec.getPostingsFormatForField("_id"),
            instanceOf(randomSyntheticId ? TSDBSyntheticIdPostingsFormat.class : ES87BloomFilterPostingsFormat.class)
        );
        assertThat(perFieldMapperCodec.useBloomFilter("another_field"), is(false));

        Class<? extends PostingsFormat> expectedPostingsFormat = timeSeries ? ES812PostingsFormat.class : Lucene104PostingsFormat.class;
        assertThat(perFieldMapperCodec.getPostingsFormatForField("another_field"), instanceOf(expectedPostingsFormat));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled() throws IOException {
        final boolean randomSyntheticId = syntheticId(true);
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, true, false, randomSyntheticId);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(true));
        assertThat(
            perFieldMapperCodec.getPostingsFormatForField("_id"),
            instanceOf(randomSyntheticId ? TSDBSyntheticIdPostingsFormat.class : ES87BloomFilterPostingsFormat.class)
        );
        assertThat(perFieldMapperCodec.useBloomFilter("another_field"), is(false));
        assertThat(perFieldMapperCodec.getPostingsFormatForField("another_field"), instanceOf(ES812PostingsFormat.class));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_noTimeSeriesMode() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, false, false, false);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
        assertThat(perFieldMapperCodec.getPostingsFormatForField("_id"), instanceOf(ES812PostingsFormat.class));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_disableBloomFilter() throws IOException {
        final boolean randomSyntheticId = syntheticId(true);
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, true, true, randomSyntheticId);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
        assertThat(
            perFieldMapperCodec.getPostingsFormatForField("_id"),
            instanceOf(randomSyntheticId ? TSDBSyntheticIdPostingsFormat.class : ES812PostingsFormat.class)
        );
        assertWarnings(
            "[index.bloom_filter_for_id_field.enabled] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the deprecation documentation for the next major version."
        );
    }

    public void testUseEs812PostingsFormat() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec;

        // standard index mode
        perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.STANDARD, METRIC_MAPPING);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("gauge"), instanceOf(Lucene104PostingsFormat.class));

        perFieldMapperCodec = createFormatSupplier(false, true, IndexMode.STANDARD, METRIC_MAPPING);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("gauge"), instanceOf(ES812PostingsFormat.class));

        // LogsDB index mode
        // by default, logsdb uses the ES 8.12 postings format
        perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.LOGSDB, LOGS_MAPPING);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("message"), instanceOf(ES812PostingsFormat.class));

        perFieldMapperCodec = createFormatSupplier(false, true, IndexMode.LOGSDB, LOGS_MAPPING);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("message"), instanceOf(ES812PostingsFormat.class));

        // time series index mode
        // by default, logsdb uses the ES 8.12 postings format
        perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.TIME_SERIES, METRIC_MAPPING);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("gauge"), instanceOf(ES812PostingsFormat.class));

        perFieldMapperCodec = createFormatSupplier(false, true, IndexMode.TIME_SERIES, METRIC_MAPPING);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("gauge"), instanceOf(ES812PostingsFormat.class));
    }

    public void testUseEs812PostingsFormatForIdField() throws IOException {
        int numIterations = randomIntBetween(2, 64);
        for (int i = 0; i < numIterations; i++) {
            var indexMode = randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB, IndexMode.TIME_SERIES);
            String mapping = randomFrom(METRIC_MAPPING, MULTI_METRIC_MAPPING, LOGS_MAPPING);
            final boolean randomSyntheticId = syntheticId(indexMode.equals(IndexMode.TIME_SERIES));
            PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(
                randomBoolean(),
                randomBoolean(),
                indexMode,
                mapping,
                randomSyntheticId
            );
            var result = perFieldMapperCodec.getPostingsFormatForField("_id");
            if (result instanceof ES87BloomFilterPostingsFormat es87BloomFilterPostingsFormat) {
                Function<String, PostingsFormat> postingsFormats = es87BloomFilterPostingsFormat.getPostingsFormats();
                result = postingsFormats.apply("_id");
            }
            assertThat(result, (instanceOf(randomSyntheticId ? TSDBSyntheticIdPostingsFormat.class : ES812PostingsFormat.class)));
        }
    }

    public void testUseES87TSDBEncodingForTimestampField() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, true, true, syntheticId(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
    }

    public void testDoNotUseES87TSDBEncodingForTimestampFieldNonTimeSeriesIndex() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, false, true, false);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(false));
    }

    public void testEnableES87TSDBCodec() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, false, IndexMode.TIME_SERIES, METRIC_MAPPING);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
    }

    public void testDisableES87TSDBCodec() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.TIME_SERIES, METRIC_MAPPING);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(false));
    }

    private PerFieldFormatSupplier createFormatSupplier(
        boolean timestampField,
        boolean timeSeries,
        boolean disableBloomFilter,
        boolean syntheticId
    ) throws IOException {
        Settings.Builder settings = Settings.builder();
        if (timeSeries) {
            settings.put(IndexSettings.MODE.getKey(), "time_series");
            settings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field");
            settings.put(IndexSettings.SYNTHETIC_ID.getKey(), syntheticId);
        }
        if (disableBloomFilter) {
            settings.put(IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.getKey(), false);
        }
        MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), settings.build(), "test");
        if (timestampField) {
            String mapping = """
                {
                    "_data_stream_timestamp": {
                        "enabled": true
                    },
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        }
                    }
                }
                """;
            mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        }
        return new PerFieldFormatSupplier(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
    }

    public void testUseES87TSDBEncodingSettingDisabled() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.TIME_SERIES, MULTI_METRIC_MAPPING);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("counter")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(false));
    }

    public void testUseTimeSeriesModeDisabledCodecDisabled() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(IndexMode.STANDARD, MULTI_METRIC_MAPPING);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("counter")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(false));
    }

    public void testUseTimeSeriesDocValuesCodecSetting() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(
            true,
            null,
            false,
            IndexMode.STANDARD,
            MULTI_METRIC_MAPPING,
            false
        );
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("counter")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(true));
    }

    public void testUseTimeSeriesModeAndCodecEnabled() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, false, IndexMode.TIME_SERIES, MULTI_METRIC_MAPPING);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("counter")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(true));
    }

    public void testLogsIndexMode() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(IndexMode.LOGSDB, LOGS_MAPPING);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("hostname")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("response_size")), is(true));
    }

    public void testMetaFields() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(IndexMode.LOGSDB, LOGS_MAPPING);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(TimeSeriesIdFieldMapper.NAME)), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(TimeSeriesRoutingHashFieldMapper.NAME)), is(true));
        // See: PerFieldFormatSupplier why these fields shouldn't use tsdb codec
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(SourceFieldMapper.RECOVERY_SOURCE_NAME)), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME)), is(false));
    }

    public void testSeqnoField() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(IndexMode.LOGSDB, LOGS_MAPPING);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(SeqNoFieldMapper.NAME)), is(true));
    }

    private PerFieldFormatSupplier createFormatSupplier(IndexMode mode, String mapping) throws IOException {
        return createFormatSupplier(null, false, mode, mapping);
    }

    private PerFieldFormatSupplier createFormatSupplier(
        Boolean enableES87TSDBCodec,
        Boolean useEs812PostingsFormat,
        IndexMode mode,
        String mapping
    ) throws IOException {
        return createFormatSupplier(null, enableES87TSDBCodec, useEs812PostingsFormat, mode, mapping, null);
    }

    private PerFieldFormatSupplier createFormatSupplier(
        Boolean enableES87TSDBCodec,
        Boolean useEs812PostingsFormat,
        IndexMode mode,
        String mapping,
        boolean syntheticId
    ) throws IOException {
        return createFormatSupplier(null, enableES87TSDBCodec, useEs812PostingsFormat, mode, mapping, syntheticId);
    }

    private PerFieldFormatSupplier createFormatSupplier(
        Boolean useTimeSeriesDocValuesFormatSetting,
        Boolean enableES87TSDBCodec,
        Boolean useEs812PostingsFormat,
        IndexMode mode,
        String mapping,
        Boolean syntheticId
    ) throws IOException {
        Settings.Builder settings = Settings.builder();
        settings.put(IndexSettings.MODE.getKey(), mode);
        if (mode == IndexMode.TIME_SERIES) {
            settings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field");
        }
        if (syntheticId != null) {
            settings.put(IndexSettings.SYNTHETIC_ID.getKey(), syntheticId);
        }
        if (enableES87TSDBCodec != null) {
            settings.put(IndexSettings.TIME_SERIES_ES87TSDB_CODEC_ENABLED_SETTING.getKey(), enableES87TSDBCodec);
        }
        if (useTimeSeriesDocValuesFormatSetting != null) {
            settings.put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), useTimeSeriesDocValuesFormatSetting);
        }
        if (useEs812PostingsFormat) {
            settings.put(IndexSettings.USE_ES_812_POSTINGS_FORMAT.getKey(), true);
        }
        MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), settings.build(), "test");
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        return new PerFieldFormatSupplier(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
    }

    private static final List<IndexMode> INDEX_MODES_UNDER_TEST = List.of(IndexMode.TIME_SERIES, IndexMode.STANDARD, IndexMode.LOGSDB);

    private static String mappingFor(final IndexMode mode) {
        return mode == IndexMode.LOGSDB ? LOGS_MAPPING : METRIC_MAPPING;
    }

    private static String fieldFor(final IndexMode mode) {
        return mode == IndexMode.LOGSDB ? "hostname" : "gauge";
    }

    public void testES95UsedAcrossModesWhenSettingEnabled() throws IOException {
        assumeTrue("es95_codec feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
        for (IndexMode mode : INDEX_MODES_UNDER_TEST) {
            final PerFieldFormatSupplier supplier = createFormatSupplierWithVersion(mode, mappingFor(mode), IndexVersion.current());
            final DocValuesFormat format = supplier.getDocValuesFormatForField(fieldFor(mode));
            assertThat("mode=" + mode, format, instanceOf(ES95TSDBDocValuesFormat.class));
        }
    }

    public void testES95DocValuesFormatUsedForTimestampField() throws IOException {
        assumeTrue("es95_codec feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
        final PerFieldFormatSupplier supplier = createFormatSupplierWithVersion(
            IndexMode.TIME_SERIES,
            METRIC_MAPPING,
            IndexVersion.current()
        );
        final DocValuesFormat format = supplier.getDocValuesFormatForField("@timestamp");
        assertThat(format, instanceOf(ES95TSDBDocValuesFormat.class));
    }

    public void testES819UsedAcrossModesWhenSettingDisabled() throws IOException {
        assumeTrue("es95_codec feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
        for (IndexMode mode : INDEX_MODES_UNDER_TEST) {
            final PerFieldFormatSupplier supplier = createFormatSupplierWithVersion(
                mode,
                mappingFor(mode),
                IndexVersion.current(),
                false,
                true
            );
            final DocValuesFormat format = supplier.getDocValuesFormatForField(fieldFor(mode));
            assertFalse("mode=" + mode + " expected non-ES95", format instanceof ES95TSDBDocValuesFormat);
        }
    }

    public void testES819UsedAcrossModesWithOldIndexVersion() throws IOException {
        assumeTrue("es95_codec feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
        final IndexVersion oldVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG);
        for (IndexMode mode : INDEX_MODES_UNDER_TEST) {
            final PerFieldFormatSupplier supplier = createFormatSupplierWithVersion(mode, mappingFor(mode), oldVersion);
            assertThat("mode=" + mode, supplier.useTSDBDocValuesFormat(fieldFor(mode)), is(true));
            final DocValuesFormat format = supplier.getDocValuesFormatForField(fieldFor(mode));
            assertFalse("mode=" + mode + " expected non-ES95", format instanceof ES95TSDBDocValuesFormat);
        }
    }

    public void testES95NotUsedForStandardWithDefaultDocValuesFormat() throws IOException {
        assumeTrue("es95_codec feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
        final PerFieldFormatSupplier supplier = createFormatSupplierWithVersion(
            IndexMode.STANDARD,
            METRIC_MAPPING,
            IndexVersion.current(),
            true,
            false
        );
        final DocValuesFormat format = supplier.getDocValuesFormatForField("gauge");
        assertFalse("Expected non-ES95 format", format instanceof ES95TSDBDocValuesFormat);
    }

    private PerFieldFormatSupplier createFormatSupplierWithVersion(
        final IndexMode mode,
        final String mapping,
        final IndexVersion indexVersion
    ) throws IOException {
        return createFormatSupplierWithVersion(mode, mapping, indexVersion, true, true);
    }

    private PerFieldFormatSupplier createFormatSupplierWithVersion(
        final IndexMode mode,
        final String mapping,
        final IndexVersion indexVersion,
        final boolean es95Enabled,
        final boolean useTimeSeriesDocValuesFormat
    ) throws IOException {
        final Settings.Builder settings = Settings.builder();
        settings.put(IndexSettings.MODE.getKey(), mode);
        settings.put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion);
        if (mode == IndexMode.TIME_SERIES) {
            settings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field");
        }
        if (useTimeSeriesDocValuesFormat) {
            settings.put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true);
        }
        if (es95Enabled && IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled()) {
            settings.put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), true);
        }
        final MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), settings.build(), "test");
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        return new PerFieldFormatSupplier(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
    }

    private static boolean syntheticId(boolean timeSeries) {
        return timeSeries && randomBoolean();
    }
}
