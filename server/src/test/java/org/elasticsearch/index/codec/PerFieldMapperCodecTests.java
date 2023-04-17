/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class PerFieldMapperCodecTests extends ESTestCase {

    public void testUseBloomFilter() throws IOException {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(false, randomBoolean(), false);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(true));
        assertThat(perFieldMapperCodec.useBloomFilter("another_field"), is(false));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled() throws IOException {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(true, true, false);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(true));
        assertThat(perFieldMapperCodec.useBloomFilter("another_field"), is(false));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_noTimeSeriesMode() throws IOException {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(true, false, false);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_disableBloomFilter() throws IOException {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(true, true, true);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
        assertWarnings(
            "[index.bloom_filter_for_id_field.enabled] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    private PerFieldMapperCodec createCodec(boolean timestampField, boolean timeSeries, boolean disableBloomFilter) throws IOException {
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
        return new PerFieldMapperCodec(Lucene95Codec.Mode.BEST_SPEED, mapperService, BigArrays.NON_RECYCLING_INSTANCE);
    }

}
