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

public class TsdbIdFieldMapperTests extends MetadataMapperTestCase {
    public void testAlignsWithIndexRouting() throws IOException {
        MapperService mapperService = createMapperService();
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:01Z");
            b.field("r1", "cat");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "1970-01-01T00:00:00Z");
            b.field("r1", "cat");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "-9998-01-01T00:00:00Z");
            b.field("r1", "cat");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "9998-01-01T00:00:00Z");
            b.field("r1", "cat");
        });

        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r2", "cat");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k1", "dog");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k2", "dog");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.startObject("o").field("k3", "dog").endObject();
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("k3", "dog");
            }
            b.endObject();
        });

        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", 1);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", Long.MIN_VALUE);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L2", 1324);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("L3", Long.MAX_VALUE);
            }
            b.endObject();
        });

        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", 1);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", Integer.MIN_VALUE);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i2", 1324);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("i3", Integer.MAX_VALUE);
            }
            b.endObject();
        });

        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", 1);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", Short.MIN_VALUE);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s2", 1324);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("s3", Short.MAX_VALUE);
            }
            b.endObject();
        });

        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", 1);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", Byte.MIN_VALUE);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b2", 12);
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("b3", Byte.MAX_VALUE);
            }
            b.endObject();
        });

        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "192.168.0.1");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "12.12.45.254");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip2", "FE80:CD00:0000:0CDE:1257:0000:211E:729C");
        });
        assertIdAligns(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
            }
            b.endObject();
        });
    }

    private void assertIdAligns(MapperService mapperService, CheckedConsumer<XContentBuilder, IOException> source) throws IOException {
        IndexRouting routing = IndexRouting.fromIndexMetadata(mapperService.getIndexSettings().getIndexMetadata());
        String id;
        int indexShard;
        try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
            builder.startObject();
            source.accept(builder);
            builder.endObject();
            BytesReference bytes = BytesReference.bytes(builder);
            SourceToParse sourceToParse = new SourceToParse("replaced", bytes, builder.contentType());
            id = mapperService.documentParser().parseDocument(sourceToParse, mapperService.mappingLookup()).id();
            indexShard = routing.indexShard(null, null, builder.contentType(), bytes);
        }
        assertThat(routing.getShard(id, null), equalTo(indexShard));
        assertThat(routing.deleteShard(id, null), equalTo(indexShard));
    }

    public void testBwc() throws IOException {
        MapperService mapperService = createMapperService();
        /*
         * If these values change then ids for individual samples will shift. You may
         * modify them with a new index created version, but when you do you must copy
         * this test and patch the versions at the top. Because newer versions of
         * Elasticsearch must continue to route based on the version on the index.
         */
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
        }).id(), equalTo("f8MTSAAAAACAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:01Z");
            b.field("r1", "cat");
        }).id(), equalTo("f8MTSAAAAABojiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "1970-01-01T00:00:00Z");
            b.field("r1", "cat");
        }).id(), equalTo("f8MTSAAAAAAAAAAAAAAAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "-9998-01-01T00:00:00Z");
            b.field("r1", "cat");
        }).id(), equalTo("f8MTSAAAAAAAGGAEgqj-_w"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "9998-01-01T00:00:00Z");
            b.field("r1", "cat");
        }).id(), equalTo("f8MTSAAAAAAAhL0iaeYAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r2", "cat");
        }).id(), equalTo("9i3PXAAAAACAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o").field("r3", "cat").endObject();
        }).id(), equalTo("7xxG4AAAAACAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k1", "dog");
        }).id(), equalTo("f8MTSIOpi8GAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("k2", "dog");
        }).id(), equalTo("f8MTSGG2e-SAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.startObject("o").field("k3", "dog").endObject();
        }).id(), equalTo("f8MTSNI9HjqAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("k3", "dog");
            }
            b.endObject();
        }).id(), equalTo("7xxG4NI9HjqAiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", 1);
        }).id(), equalTo("f8MTSOnsnjSAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L1", Long.MIN_VALUE);
        }).id(), equalTo("f8MTSFgfjoiAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("L2", 1324);
        }).id(), equalTo("f8MTSLWOnAyAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("L3", Long.MAX_VALUE);
            }
            b.endObject();
        }).id(), equalTo("7xxG4MqfAs2AiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", 1);
        }).id(), equalTo("f8MTSNqNadGAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i1", Integer.MIN_VALUE);
        }).id(), equalTo("f8MTSKLk9e6AiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("i2", 1324);
        }).id(), equalTo("f8MTSCRzWuKAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("i3", Integer.MAX_VALUE);
            }
            b.endObject();
        }).id(), equalTo("7xxG4F8JUG2AiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", 1);
        }).id(), equalTo("f8MTSL4xi0KAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s1", Short.MIN_VALUE);
        }).id(), equalTo("f8MTSDWqX6KAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("s2", 1324);
        }).id(), equalTo("f8MTSLOkLeCAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("s3", Short.MAX_VALUE);
            }
            b.endObject();
        }).id(), equalTo("7xxG4DESZU6AiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", 1);
        }).id(), equalTo("f8MTSA5vdrSAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b1", Byte.MIN_VALUE);
        }).id(), equalTo("f8MTSIvfUVyAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("b2", 12);
        }).id(), equalTo("f8MTSOHWJ9mAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("b3", Byte.MAX_VALUE);
            }
            b.endObject();
        }).id(), equalTo("7xxG4H6hmUSAiiYTfgEAAA"));

        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "192.168.0.1");
        }).id(), equalTo("f8MTSOpX0veAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip1", "12.12.45.254");
        }).id(), equalTo("f8MTSIDcdLKAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", "cat");
            b.field("ip2", "FE80:CD00:0000:0CDE:1257:0000:211E:729C");
        }).id(), equalTo("f8MTSC17BjyAiiYTfgEAAA"));
        assertThat(parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.startObject("o");
            {
                b.field("r3", "cat");
                b.field("ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
            }
            b.endObject();
        }).id(), equalTo("7xxG4KdsAaSAiiYTfgEAAA"));
    }

    public void testNullRouting() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
            b.field("r2", (String) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullKeyword() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
            b.field("k1", (String) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullLong() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
            b.field("L1", (Long) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullInt() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
            b.field("i1", (Integer) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullShort() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
            b.field("s1", (Short) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullByte() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
            b.field("b1", (Byte) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
    }

    public void testNullIp() throws IOException {
        MapperService mapperService = createMapperService();
        String str = randomAlphaOfLength(12);
        String withNull = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
            b.field("ip1", (String) null);
        }).id();
        String withoutField = parse(mapperService, b -> {
            b.field("@timestamp", "2022-01-01T01:00:00Z");
            b.field("r1", str);
        }).id();
        assertThat(withNull, equalTo(withoutField));
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
