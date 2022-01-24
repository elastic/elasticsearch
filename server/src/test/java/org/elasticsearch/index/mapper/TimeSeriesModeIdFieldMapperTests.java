/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesModeIdFieldMapperTests extends MetadataMapperTestCase {
    public void testBwc() throws IOException {
        MapperService mapperService = createMapperService();
        /*
         * If these values change then ids for individual samples will shift. You may
         * modify them with a new index created version, but when you do you must copy
         * this test and continue to support the versions here so Elasticsearch can
         * continue to read older indices.
         */
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", "cat");
        }).id(), equalTo("-dJ9dnP2OW2AiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:01Z");
            b.field("k1", "cat");
        }).id(), equalTo("-dJ9dnP2OW1ojiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "1970-01-01T00:00:00Z");
            b.field("k1", "cat");
        }).id(), equalTo("-dJ9dnP2OW0AAAAAAAAAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "-9998-01-01T00:00:00Z");
            b.field("k1", "cat");
        }).id(), equalTo("-dJ9dnP2OW0AGGAEgqj-_w"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "9998-01-01T00:00:00Z");
            b.field("k1", "cat");
        }).id(), equalTo("-dJ9dnP2OW0AhL0iaeYAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k2", "cat");
        }).id(), equalTo("OXR4YxEET6CAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("k3", "cat").endObject();
        }).id(), equalTo("O1IRXMisesCAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", "cat");
            b.field("k2", "dog");
        }).id(), equalTo("wvb3F8pRbUGAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", "cat");
            b.startObject("o").field("k3", "dog").endObject();
        }).id(), equalTo("VhYqEelLvo2AiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("k3", "cat");
                b.field("k4", "dog");
            }
            b.endObject();
        }).id(), equalTo("BtFfTetE4jGAiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", 1);
        }).id(), equalTo("RimPmZlVpXmAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", Long.MIN_VALUE);
        }).id(), equalTo("bqguWOCsQGOAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L2", 1324);
        }).id(), equalTo("S3T647_L1LGAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("L3", Long.MAX_VALUE);
            }
            b.endObject();
        }).id(), equalTo("GdAzH__wKoOAiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", 1);
        }).id(), equalTo("qs9kfZmuahuAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", Integer.MIN_VALUE);
        }).id(), equalTo("lvJy6YE6J_qAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i2", 1324);
        }).id(), equalTo("FIZN6aG1meeAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("i3", Integer.MAX_VALUE);
            }
            b.endObject();
        }).id(), equalTo("mCo-SzAFe_qAiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", 1);
        }).id(), equalTo("e8d4Tb-bRiKAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", Short.MIN_VALUE);
        }).id(), equalTo("O_4Vxg2P8pWAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s2", 1324);
        }).id(), equalTo("3E4qp_MgE5aAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("s3", Short.MAX_VALUE);
            }
            b.endObject();
        }).id(), equalTo("tmGNdyMel-aAiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", 1);
        }).id(), equalTo("UfHCbEXhTeCAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", Byte.MIN_VALUE);
        }).id(), equalTo("93Gufkv3jgaAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b2", 12);
        }).id(), equalTo("v346ZCXvv4eAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("b3", Byte.MAX_VALUE);
            }
            b.endObject();
        }).id(), equalTo("uo5oFOUJqfyAiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "192.168.0.1");
        }).id(), equalTo("1LZRHZmlO4qAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "12.12.45.254");
        }).id(), equalTo("O6IYuvpIe7yAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip2", "FE80:CD00:0000:0CDE:1257:0000:211E:729C");
        }).id(), equalTo("mZL0uHt6xq-AiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
            }
            b.endObject();
        }).id(), equalTo("6HWYjmGVnXqAiiYTfgEAAA"));
    }

    public void testNullKeyword() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
            b.field("k2", (String) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullLong() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
            b.field("L1", (Long) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullInt() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
            b.field("i1", (Integer) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullShort() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
            b.field("s1", (Short) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullByte() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
            b.field("b1", (Byte) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullIp() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
            b.field("ip1", (String) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("k1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testDotsInFieldNames() throws IOException {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 100))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "o.*")
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("o.f1").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("o").startObject("properties");
            {
                b.startObject("f2").field("type", "keyword").field("time_series_dimension", true).endObject();
            }
            b.endObject().endObject();
        }));

        // o\.*
        
        // {"o": {"f1": f1, "f2": f2}}
        // {"o": {"f1": f1}, "o.f2": f2}
        // {"o.f1": f1, "o": {"f2": f2}}

        String f1 = randomAlphaOfLength(12);
        String f2 = randomAlphaOfLength(12);
        String canonical = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("f1", f1).field("f2", f2).endObject();
        }).id();
        assertThat(parse(mapperService, b -> {         // NOCOMMIT I don't think this is ok!?
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.f1", f1);
            b.startObject("o").field("f2", f2).endObject();
        }).id(), equalTo(canonical));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("f1", f1).endObject();
            b.field("o.f2", f2);
        }).id(), equalTo(canonical));
    }

    private ParsedDocument parse(MapperService mapperService, CheckedConsumer<XContentBuilder, IOException> source) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
            builder.startObject();
            source.accept(builder);
            builder.endObject();
            SourceToParse sourceToParse = new SourceToParse("replaced", BytesReference.bytes(builder), builder.contentType());
            return mapperService.documentParser().parseDocument(sourceToParse, mapperService.mappingLookup());
        }
    }

    private MapperService createMapperService() throws IOException {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 100))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "r1,r2,o.r3")
            .put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), 100)
            .build();
        return createMapperService(settings, mapping(b -> {
            b.startObject("k1").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("k2").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("L1").field("type", "long").field("time_series_dimension", true).endObject();
            b.startObject("L2").field("type", "long").field("time_series_dimension", true).endObject();
            b.startObject("i1").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("i2").field("type", "integer").field("time_series_dimension", true).endObject();
            b.startObject("s1").field("type", "short").field("time_series_dimension", true).endObject();
            b.startObject("s2").field("type", "short").field("time_series_dimension", true).endObject();
            b.startObject("b1").field("type", "byte").field("time_series_dimension", true).endObject();
            b.startObject("b2").field("type", "byte").field("time_series_dimension", true).endObject();
            b.startObject("ip1").field("type", "ip").field("time_series_dimension", true).endObject();
            b.startObject("ip2").field("type", "ip").field("time_series_dimension", true).endObject();
            b.startObject("o").startObject("properties");
            {
                b.startObject("k3").field("type", "keyword").field("time_series_dimension", true).endObject();
                b.startObject("k4").field("type", "keyword").field("time_series_dimension", true).endObject();
                b.startObject("L3").field("type", "long").field("time_series_dimension", true).endObject();
                b.startObject("i3").field("type", "integer").field("time_series_dimension", true).endObject();
                b.startObject("s3").field("type", "short").field("time_series_dimension", true).endObject();
                b.startObject("b3").field("type", "byte").field("time_series_dimension", true).endObject();
                b.startObject("ip3").field("type", "ip").field("time_series_dimension", true).endObject();
            }
            b.endObject().endObject();
        }));
    }

    @Override
    protected String fieldName() {
        return IdFieldMapper.NAME;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}
}
