/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.lucene94.Lucene94Codec;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class PerFieldMapperCodecTests extends ESTestCase {

    public void testUseBloomFilter() {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(false, randomBoolean(), false, VersionUtils.randomVersion(random()));
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(true));
        assertThat(perFieldMapperCodec.useBloomFilter("another_field"), is(false));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled() {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(true, true, false, Version.V_8_7_0);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(true));
        assertThat(perFieldMapperCodec.useBloomFilter("another_field"), is(false));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_noTimeSeriesMode() {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(true, false, false, Version.V_8_7_0);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_disableBloomFilter() {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(true, true, true, Version.V_8_7_0);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
        assertWarnings(
            "[index.bloom_filter_for_id_field.enabled] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
    }

    public void testUseBloomFilterWithTimestampFieldEnabled_olderVersion() {
        PerFieldMapperCodec perFieldMapperCodec = createCodec(true, true, false, Version.V_8_6_0);
        assertThat(perFieldMapperCodec.useBloomFilter("_id"), is(false));
    }

    private static PerFieldMapperCodec createCodec(
        boolean timestampField,
        boolean timeSeries,
        boolean disableBloomFilter,
        Version createdVersion
    ) {
        Settings.Builder settings = Settings.builder();
        settings.put(IndexMetadata.SETTING_VERSION_CREATED, createdVersion);
        if (timeSeries) {
            settings.put(IndexSettings.MODE.getKey(), "time_series");
            settings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field");
        }
        if (disableBloomFilter) {
            settings.put(IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.getKey(), false);
        }
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", settings.build());
        RootObjectMapper.Builder root = new RootObjectMapper.Builder("_doc", ObjectMapper.Defaults.SUBOBJECTS);

        MetadataFieldMapper[] metaFieldMappers;
        if (timestampField) {
            metaFieldMappers = new MetadataFieldMapper[] { DataStreamTestHelper.getDataStreamTimestampFieldMapper() };
            root.add(
                new DateFieldMapper.Builder(
                    "@timestamp",
                    DateFieldMapper.Resolution.MILLISECONDS,
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                    ScriptCompiler.NONE,
                    true,
                    Version.CURRENT
                )
            );
        } else {
            metaFieldMappers = new MetadataFieldMapper[] {};
        }
        Mapping mapping = new Mapping(root.build(MapperBuilderContext.root(false)), metaFieldMappers, Collections.emptyMap());
        return new PerFieldMapperCodec(
            Lucene94Codec.Mode.BEST_SPEED,
            indexSettings,
            MappingLookup.fromMapping(mapping),
            BigArrays.NON_RECYCLING_INSTANCE
        );
    }

}
