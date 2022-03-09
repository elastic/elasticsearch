/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.name.Named;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TsidExtractingIdFieldMapperTests extends MetadataMapperTestCase {
    private static class TestCase {
        private final String name;
        private final String expectedId;
        private final CheckedConsumer<XContentBuilder, IOException> source;
        private final List<CheckedConsumer<XContentBuilder, IOException>> equivalentSources = new ArrayList<>();

        TestCase(String name, String expectedId, CheckedConsumer<XContentBuilder, IOException> source) {
            this.name = name;
            this.expectedId = expectedId;
            this.source = source;
        }

        public TestCase and(CheckedConsumer<XContentBuilder, IOException> equivalentSource) {
            this.equivalentSources.add(equivalentSource);
            return this;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> params() {
        List<TestCase> items = new ArrayList<>();
        /*
         * If these values change then ids for individual samples will shift. You may
         * modify them with a new index created version, but when you do you must copy
         * this test and continue to support the versions here so Elasticsearch can
         * continue to read older indices.
         */

        // Dates
        items.add(new TestCase("2022-01-01T01:00:00Z", "XsFI2ezm5OViFixWgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
        }));
        items.add(new TestCase("2022-01-01T01:00:01Z", "XsFI2ezm5OViFixWaI4mE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:01Z");
            b.field("r1", "cat");
        }));
        items.add(new TestCase("1970-01-01T00:00:00Z", "XsFI2ezm5OViFixWAAAAAAAAAAA", b -> {
            b.field("@timestamp", "1970-01-01T00:00:00Z");
            b.field("r1", "cat");
        }));
        items.add(new TestCase("-9998-01-01T00:00:00Z", "XsFI2ezm5OViFixWABhgBIKo_v8", b -> {
            b.field("@timestamp", "-9998-01-01T00:00:00Z");
            b.field("r1", "cat");
        }));
        items.add(new TestCase("9998-01-01T00:00:00Z", "XsFI2ezm5OViFixWAIS9ImnmAAA", b -> {
            b.field("@timestamp", "9998-01-01T00:00:00Z");
            b.field("r1", "cat");
        }));

        // routing keywords
        items.add(new TestCase("r1", "XsFI2ezm5OViFixWgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k1", (String) null);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", (Long) null);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", (Integer) null);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", (Short) null);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", (Byte) null);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", (String) null);
        }));
        items.add(new TestCase("r2", "1y-UzdYi98F0UVRigIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r2", "cat");
        }));
        items.add(new TestCase("o.r3", "zh4dcftpIU55Ond-gIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
        }));

        // non-routing keyword
        items.add(new TestCase("k1=dog", "XsFI2dL8sZeQhBgxgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k1", "dog");
        }));
        items.add(new TestCase("k1=pumpkin", "XsFI2VlD6_SkSo4MgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k1", "pumpkin");
        }));
        items.add(new TestCase("k1=empty string", "XsFI2aBA6UgrxLRqgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k1", "");
        }));
        items.add(new TestCase("k2", "XsFI2W2e5Ycw0o5_gIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k2", "dog");
        }));
        items.add(new TestCase("o.k3", "XsFI2ZAfOI6DMQhFgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.startObject("o").field("k3", "dog").endObject();
        }));
        items.add(new TestCase("o.r3", "zh4dcbFtT1qHtjl8gIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("k3", "dog");
            }
            b.endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.startObject("o").field("k3", "dog").endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
            b.field("o.k3", "dog");
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.field("o.k3", "dog");
        }));

        // long
        items.add(new TestCase("L1=1", "XsFI2eGMFOYjW7LLgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", 1);
        }));
        items.add(new TestCase("L1=min", "XsFI2f9V0yuDfkRWgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", Long.MIN_VALUE);
        }));
        items.add(new TestCase("L2=1234", "XsFI2S8PYEBSm6QYgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L2", 1234);
        }));
        items.add(new TestCase("o.L3=max", "zh4dcaI-57LdG7-cgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("L3", Long.MAX_VALUE);
            }
            b.endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.startObject("o").field("L3", Long.MAX_VALUE).endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
            b.field("o.L3", Long.MAX_VALUE);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.field("o.L3", Long.MAX_VALUE);
        }));

        // int
        items.add(new TestCase("i1=1", "XsFI2R3LiMZSeUGKgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", 1);
        }));
        items.add(new TestCase("i1=min", "XsFI2fC7DMEVFaU9gIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", Integer.MIN_VALUE);
        }));
        items.add(new TestCase("i2=1234", "XsFI2ZVte8HK90RJgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i2", 1324);
        }));
        items.add(new TestCase("o.i3=max", "zh4dcQy_QJRCqIx7gIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("i3", Integer.MAX_VALUE);
            }
            b.endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.startObject("o").field("i3", Integer.MAX_VALUE).endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
            b.field("o.i3", Integer.MAX_VALUE);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.field("o.i3", Integer.MAX_VALUE);
        }));

        // short
        items.add(new TestCase("s1=1", "XsFI2axCr11Q93m7gIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", 1);
        }));
        items.add(new TestCase("s1=min", "XsFI2Rbs9Ua9BH1wgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", Short.MIN_VALUE);
        }));
        items.add(new TestCase("s2=1234", "XsFI2SBKaLBqXMBYgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s2", 1234);
        }));
        items.add(new TestCase("o.s3=max", "zh4dcYIFo98LQWs4gIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("s3", Short.MAX_VALUE);
            }
            b.endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.startObject("o").field("s3", Short.MAX_VALUE).endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
            b.field("o.s3", Short.MAX_VALUE);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.field("o.s3", Short.MAX_VALUE);
        }));

        // byte
        items.add(new TestCase("b1=1", "XsFI2dDrcWaf3zDPgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", 1);
        }));
        items.add(new TestCase("b1=min", "XsFI2cTzLrNqHtxngIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", Byte.MIN_VALUE);
        }));
        items.add(new TestCase("b2=12", "XsFI2Sb77VB9AswjgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b2", 12);
        }));
        items.add(new TestCase("o.s3=max", "zh4dcfFauKzj6lgxgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("b3", Byte.MAX_VALUE);
            }
            b.endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.startObject("o").field("b3", Byte.MAX_VALUE).endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
            b.field("o.b3", Byte.MAX_VALUE);
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.field("o.b3", Byte.MAX_VALUE);
        }));

        // ip
        items.add(new TestCase("ip1=192.168.0.1", "XsFI2dJ1cyrrjNa2gIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "192.168.0.1");
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "::ffff:c0a8:1");
        }));
        items.add(new TestCase("ip1=12.12.45.254", "XsFI2ZUAcRxOwhHKgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "12.12.45.254");
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "::ffff:c0c:2dfe");
        }));
        items.add(new TestCase("ip2=FE80:CD00:0000:0CDE:1257:0000:211E:729C", "XsFI2XTGWAekP_oGgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip2", "FE80:CD00:0000:0CDE:1257:0000:211E:729C");
        }));
        items.add(new TestCase("o.ip3=2001:db8:85a3:8d3:1319:8a2e:370:7348", "zh4dcU_FSGP9GuHjgIomE34BAAA", b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
            }
            b.endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.startObject("o").field("ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348").endObject();
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
            b.field("o.ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
        }).and(b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("o.r3", "cat");
            b.field("o.ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
        }));

        return items.stream().map(td -> new Object[] { td }).toList();
    }

    private final TestCase testCase;

    public TsidExtractingIdFieldMapperTests(@Named("testCase") TestCase testCase) throws IOException {
        this.testCase = testCase;
    }

    public void testExpectedId() throws IOException {
        assertThat(parse(null, mapperService(), testCase.source).id(), equalTo(testCase.expectedId));
    }

    public void testProvideExpectedId() throws IOException {
        assertThat(parse(testCase.expectedId, mapperService(), testCase.source).id(), equalTo(testCase.expectedId));
    }

    public void testProvideWrongId() throws IOException {
        String wrongId = testCase.expectedId + "wrong";
        Exception e = expectThrows(MapperParsingException.class, () -> parse(wrongId, mapperService(), testCase.source));
        assertThat(
            e.getCause().getMessage(),
            equalTo(
                "_id must be unset or set to ["
                    + testCase.expectedId
                    + "] but was ["
                    + testCase.expectedId
                    + "wrong] because [index] is in time_series mode"
            )
        );
    }

    public void testEquivalentSources() throws IOException {
        MapperService mapperService = mapperService();
        for (CheckedConsumer<XContentBuilder, IOException> equivalent : testCase.equivalentSources) {
            assertThat(parse(null, mapperService, equivalent).id(), equalTo(testCase.expectedId));
        }
    }

    private ParsedDocument parse(@Nullable String id, MapperService mapperService, CheckedConsumer<XContentBuilder, IOException> source)
        throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
            builder.startObject();
            source.accept(builder);
            builder.endObject();
            SourceToParse sourceToParse = new SourceToParse(id, BytesReference.bytes(builder), builder.contentType());
            return mapperService.documentParser().parseDocument(sourceToParse, mapperService.mappingLookup());
        }
    }

    public void testRoutingPathCompliant() throws IOException {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        IndexRouting indexRouting = createIndexSettings(version, indexSettings(version)).getIndexRouting();
        int indexShard = indexShard(indexRouting);
        assertThat(indexRouting.getShard(testCase.expectedId, null), equalTo(indexShard));
        assertThat(indexRouting.deleteShard(testCase.expectedId, null), equalTo(indexShard));
    }

    private int indexShard(IndexRouting indexRouting) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
            builder.startObject();
            testCase.source.accept(builder);
            builder.endObject();
            return indexRouting.indexShard(null, null, builder.contentType(), BytesReference.bytes(builder));
        }
    }

    private Settings indexSettings(Version version) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 100))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "r1,r2,o.r3")
            .put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), 100)
            .build();
    }

    private MapperService mapperService() throws IOException {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        return createMapperService(indexSettings(version), mapping(b -> {
            b.startObject("r1").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("r2").field("type", "keyword").field("time_series_dimension", true).endObject();
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
                b.startObject("r3").field("type", "keyword").field("time_series_dimension", true).endObject();
                b.startObject("k3").field("type", "keyword").field("time_series_dimension", true).endObject();
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
