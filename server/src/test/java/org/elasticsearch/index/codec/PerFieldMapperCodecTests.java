/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103PostingsFormat;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.postings.ES812PostingsFormat;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PerFieldMapperCodecTests extends ESTestCase {

    private static final String MAPPING_1 = """
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

    private static final String MAPPING_2 = """
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

    private static final String MAPPING_3 = """
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
        boolean timeSeries = randomBoolean();
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(false, timeSeries, false);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(true));
        assertThat(perFieldMapperCodec.getPostingsFormatForField("_id"), instanceOf(ES87BloomFilterPostingsFormat.class));
        assertThat(perFieldMapperCodec.useBloomFilter("another_field"), is(false));

        Class<? extends PostingsFormat> expectedPostingsFormat = timeSeries ? ES812PostingsFormat.class : Lucene103PostingsFormat.class;
        assertThat(perFieldMapperCodec.getPostingsFormatForField("another_field"), instanceOf(expectedPostingsFormat));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, true, false);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(true));
        assertThat(perFieldMapperCodec.getPostingsFormatForField("_id"), instanceOf(ES87BloomFilterPostingsFormat.class));
        assertThat(perFieldMapperCodec.useBloomFilter("another_field"), is(false));
        assertThat(perFieldMapperCodec.getPostingsFormatForField("another_field"), instanceOf(ES812PostingsFormat.class));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_noTimeSeriesMode() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, false, false);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
        assertThat(perFieldMapperCodec.getPostingsFormatForField("_id"), instanceOf(Lucene103PostingsFormat.class));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_disableBloomFilter() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, true, true);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
        assertThat(perFieldMapperCodec.getPostingsFormatForField("_id"), instanceOf(ES812PostingsFormat.class));
        assertWarnings(
            "[index.bloom_filter_for_id_field.enabled] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the deprecation documentation for the next major version."
        );
    }

    public void testUseEs812PostingsFormat() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec;

        // standard index mode
        perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.STANDARD, MAPPING_1);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("gauge"), instanceOf(Lucene103PostingsFormat.class));

        perFieldMapperCodec = createFormatSupplier(false, true, IndexMode.STANDARD, MAPPING_1);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("gauge"), instanceOf(ES812PostingsFormat.class));

        // LogsDB index mode
        // by default, logsdb uses the ES 8.12 postings format
        perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.LOGSDB, MAPPING_3);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("message"), instanceOf(ES812PostingsFormat.class));

        perFieldMapperCodec = createFormatSupplier(false, true, IndexMode.LOGSDB, MAPPING_3);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("message"), instanceOf(ES812PostingsFormat.class));

        // time series index mode
        // by default, logsdb uses the ES 8.12 postings format
        perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.TIME_SERIES, MAPPING_1);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("gauge"), instanceOf(ES812PostingsFormat.class));

        perFieldMapperCodec = createFormatSupplier(false, true, IndexMode.TIME_SERIES, MAPPING_1);
        assertThat(perFieldMapperCodec.getPostingsFormatForField("gauge"), instanceOf(ES812PostingsFormat.class));
    }

    public void testUseES87TSDBEncodingForTimestampField() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, true, true);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
    }

    public void testDoNotUseES87TSDBEncodingForTimestampFieldNonTimeSeriesIndex() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, false, true);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(false));
    }

    public void testEnableES87TSDBCodec() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, false, IndexMode.TIME_SERIES, MAPPING_1);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
    }

    public void testDisableES87TSDBCodec() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.TIME_SERIES, MAPPING_1);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(false));
    }

    private PerFieldFormatSupplier createFormatSupplier(boolean timestampField, boolean timeSeries, boolean disableBloomFilter)
        throws IOException {
        Settings.Builder settings = Settings.builder();
        if (timeSeries) {
            settings.put(IndexSettings.MODE.getKey(), "time_series");
            settings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field");
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
        return new PerFieldFormatSupplier(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
    }

    public void testUseES87TSDBEncodingSettingDisabled() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(false, false, IndexMode.TIME_SERIES, MAPPING_2);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("counter")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(false));
    }

    public void testUseTimeSeriesModeDisabledCodecDisabled() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(IndexMode.STANDARD, MAPPING_2);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("counter")), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(false));
    }

    public void testUseTimeSeriesDocValuesCodecSetting() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, null, false, IndexMode.STANDARD, MAPPING_2);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("counter")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(true));
    }

    public void testUseTimeSeriesModeAndCodecEnabled() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(true, false, IndexMode.TIME_SERIES, MAPPING_2);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("counter")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("gauge")), is(true));
    }

    public void testLogsIndexMode() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(IndexMode.LOGSDB, MAPPING_3);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("@timestamp")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("hostname")), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat("response_size")), is(true));
    }

    public void testMetaFields() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(IndexMode.LOGSDB, MAPPING_3);
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(TimeSeriesIdFieldMapper.NAME)), is(true));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(TimeSeriesRoutingHashFieldMapper.NAME)), is(true));
        // See: PerFieldFormatSupplier why these fields shouldn't use tsdb codec
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(SourceFieldMapper.RECOVERY_SOURCE_NAME)), is(false));
        assertThat((perFieldMapperCodec.useTSDBDocValuesFormat(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME)), is(false));
    }

    public void testSeqnoField() throws IOException {
        PerFieldFormatSupplier perFieldMapperCodec = createFormatSupplier(IndexMode.LOGSDB, MAPPING_3);
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
        return createFormatSupplier(null, enableES87TSDBCodec, useEs812PostingsFormat, mode, mapping);
    }

    private PerFieldFormatSupplier createFormatSupplier(
        Boolean useTimeSeriesDocValuesFormatSetting,
        Boolean enableES87TSDBCodec,
        Boolean useEs812PostingsFormat,
        IndexMode mode,
        String mapping
    ) throws IOException {
        Settings.Builder settings = Settings.builder();
        settings.put(IndexSettings.MODE.getKey(), mode);
        if (mode == IndexMode.TIME_SERIES) {
            settings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field");
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
        return new PerFieldFormatSupplier(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
    }

}
