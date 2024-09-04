/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.injection.guice.name.Named;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class TsidExtractingIdFieldMapperTests extends MetadataMapperTestCase {

    private static class TestCase {
        private final String name;
        private final String expectedId;
        private final String expectedTsid;
        private final String expectedTimestamp;
        private final CheckedConsumer<XContentBuilder, IOException> source;
        private final List<CheckedConsumer<XContentBuilder, IOException>> equivalentSources = new ArrayList<>();

        TestCase(
            String name,
            String expectedId,
            String expectedTsid,
            String expectedTimestamp,
            CheckedConsumer<XContentBuilder, IOException> source
        ) {
            this.name = name;
            this.expectedId = expectedId;
            this.expectedTsid = expectedTsid;
            this.expectedTimestamp = expectedTimestamp;
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

        public CheckedConsumer<XContentBuilder, IOException> randomSource() {
            return randomFrom(Stream.concat(Stream.of(source), equivalentSources.stream()).toList());
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
        items.add(
            new TestCase(
                "2022-01-01T01:00:00Z",
                "BwAAAKjcFfi45iV3AAABfhMmioA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                }
            )
        );
        items.add(
            new TestCase(
                "2022-01-01T01:00:01Z",
                "BwAAAKjcFfi45iV3AAABfhMmjmg",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "2022-01-01T01:00:01.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:01Z");
                    b.field("r1", "cat");
                }
            )
        );
        items.add(
            new TestCase(
                "1970-01-01T00:00:00Z",
                "BwAAAKjcFfi45iV3AAAAAAAAAAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "1970-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "1970-01-01T00:00:00Z");
                    b.field("r1", "cat");
                }
            )
        );
        items.add(
            new TestCase(
                "-9998-01-01T00:00:00Z",
                "BwAAAKjcFfi45iV3__6oggRgGAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "-9998-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "-9998-01-01T00:00:00Z");
                    b.field("r1", "cat");
                }
            )
        );
        items.add(
            new TestCase(
                "9998-01-01T00:00:00Z",
                "BwAAAKjcFfi45iV3AADmaSK9hAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "9998-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "9998-01-01T00:00:00Z");
                    b.field("r1", "cat");
                }
            )
        );

        // routing keywords
        items.add(
            new TestCase(
                "r1",
                "BwAAAKjcFfi45iV3AAABfhMmioA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                }
            ).and(b -> {
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
            })
        );
        items.add(
            new TestCase(
                "r2",
                "BwAAAB0iuE1-sOQpAAABfhMmioA",
                "JNY_frTR9GmCbhXgK4Y8W44GlT_5e6_NYGOZWULpmMG9IAlZlA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r2", "cat");
                }
            )
        );
        items.add(
            new TestCase(
                "o.r3",
                "BwAAAC1h1gf2J5a8AAABfhMmioA",
                "JEyfZsJIp3UNyfWG-4SjKFIGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o").field("r3", "cat").endObject();
                }
            ).and(b -> {
                b.field("@timestamp", "2022-01-01T01:00:00Z");
                b.field("o.r3", "cat");
            })
        );

        // non-routing keyword
        items.add(
            new TestCase(
                "k1=dog",
                "BwAAACrEiVgZlSsYAAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bipyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "dog");
                }
            )
        );
        items.add(
            new TestCase(
                "k1=pumpkin",
                "BwAAAG8GX8-0QcFxAAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bibzw1JBpU_-VsHjSz5HC1yy_swPEM1iGo",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "pumpkin");
                }
            )
        );
        items.add(
            new TestCase(
                "k1=empty string",
                "BwAAAMna58i6D-Q6AAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bhaCD7uBpU_-SWGG0Uv9tZ1mLO2gi9rC1I",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "");
                }
            )
        );
        items.add(
            new TestCase(
                "k2",
                "BwAAAFqlzAuv-06kAAABfhMmioA",
                "KB9H-tGrL_UzqMcqXcgBtzypyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k2", "dog");
                }
            )
        );
        items.add(
            new TestCase(
                "o.k3",
                "BwAAAC_VhridAKDUAAABfhMmioA",
                "KGXATwN7ISd1_EycFRJ9h6qpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.startObject("o").field("k3", "dog").endObject();
                }
            )
        );
        items.add(
            new TestCase(
                "o.r3",
                "BwAAAEwfL7x__2oPAAABfhMmioA",
                "KJaYZVZz8plfkEvvPBpi1EWpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("k3", "dog");
                    }
                    b.endObject();
                }
            ).and(b -> {
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
            })
        );

        // long
        items.add(
            new TestCase(
                "L1=1",
                "BwAAAPIe53BtV9PCAAABfhMmioA",
                "KI4kVxcCLIMM2_VQGD575d-tm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L1", 1);
                }
            )
        );
        items.add(
            new TestCase(
                "L1=min",
                "BwAAAAhu7hy1RoXRAAABfhMmioA",
                "KI4kVxcCLIMM2_VQGD575d8caJ3TBpU_-cLpg-VnCBnhYk33HZBle6E",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L1", Long.MIN_VALUE);
                }
            )
        );
        items.add(
            new TestCase(
                "L2=1234",
                "BwAAAATrNu7TTpc-AAABfhMmioA",
                "KI_1WxF60L0IczG5ftUCWdndcGtgBpU_-QfM2BaR0DMagIfw3TDu_mA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L2", 1234);
                }
            )
        );
        items.add(
            new TestCase(
                "o.L3=max",
                "BwAAAGBQI6THHqxoAAABfhMmioA",
                "KN4a6QzKhzc3nwzNLuZkV51xxTOVBpU_-erUU1qSW4eJ0kP0RmAB9TE",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00.000Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("L3", Long.MAX_VALUE);
                    }
                    b.endObject();
                }
            ).and(b -> {
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
            })
        );

        // int
        items.add(
            new TestCase(
                "i1=1",
                "BwAAAEMS_RWRoHYjAAABfhMmioA",
                "KLGFpvAV8QkWSmX54kXFMgitm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i1", 1);
                }
            )
        );
        items.add(
            new TestCase(
                "i1=min",
                "BwAAAKdlQM5ILoA1AAABfhMmioA",
                "KLGFpvAV8QkWSmX54kXFMgjV8hFQBpU_-WG2MicRGWwJdBKWq2F4qy4",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i1", Integer.MIN_VALUE);
                }
            )
        );
        items.add(
            new TestCase(
                "i2=1234",
                "BwAAALhxfB6J0kBFAAABfhMmioA",
                "KJc4-5eN1uAlYuAknQQLUlxavn2sBpU_-UEXBjgaH1uYcbayrOhdgpc",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i2", 1324);
                }
            )
        );
        items.add(
            new TestCase(
                "o.i3=max",
                "BwAAAOlxKf19CbfdAAABfhMmioA",
                "KKqnzPNBe8ObksSo8rNaIFPZPCcBBpU_-Rhd_U6Jn2pjQz2zpmBuJb4",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("i3", Integer.MAX_VALUE);
                    }
                    b.endObject();
                }
            ).and(b -> {
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
            })
        );

        // short
        items.add(
            new TestCase(
                "s1=1",
                "BwAAAI_y-8kD_BFeAAABfhMmioA",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5rZuNb-1ruHTTZhtsXRZpZRwWFoc",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s1", 1);
                }
            )
        );
        items.add(
            new TestCase(
                "s1=min",
                "BwAAAGV8VNVnmPVNAAABfhMmioA",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5JgBZj9BSCms2_jgeFFhsmDlNFdM",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s1", Short.MIN_VALUE);
                }
            )
        );
        items.add(
            new TestCase(
                "s2=1234",
                "BwAAAFO8mUr-J5CpAAABfhMmioA",
                "KKEQ2p3CkpMH61hNk_SuvI0GlT_53XBrYP5TPdmCR-vREPnt20e9f9w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s2", 1234);
                }
            )
        );
        items.add(
            new TestCase(
                "o.s3=max",
                "BwAAAAKh6K11zWeuAAABfhMmioA",
                "KKVMoT_-GS95fvIBtR7XK9oGlT_5Dme9-H3sen0WZ7leJpCj7-vXau4",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("s3", Short.MAX_VALUE);
                    }
                    b.endObject();
                }
            ).and(b -> {
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
            })
        );

        // byte
        items.add(
            new TestCase(
                "b1=1",
                "BwAAANKxqgT5JDQfAAABfhMmioA",
                "KGPAUhTjWOsRfDmYp3SUELatm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b1", 1);
                }
            )
        );
        items.add(
            new TestCase(
                "b1=min",
                "BwAAAN_PD--DgUvoAAABfhMmioA",
                "KGPAUhTjWOsRfDmYp3SUELYoK6qHBpU_-d8HkZFJ3aL2ZV1lgHAjT1g",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b1", Byte.MIN_VALUE);
                }
            )
        );
        items.add(
            new TestCase(
                "b2=12",
                "BwAAAKqX5QjiuhsEAAABfhMmioA",
                "KA58oUMzXeX1V5rh51Ste0K5K9vPBpU_-Wn8JQplO-x3CgoslYO5Vks",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b2", 12);
                }
            )
        );
        items.add(
            new TestCase(
                "o.s3=max",
                "BwAAAMJ4YtN_21XHAAABfhMmioA",
                "KIwZH-StJBobjk9tCV-0OgjKmuwGBpU_-Sd-SdnoH3sbfKLgse-briE",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("b3", Byte.MAX_VALUE);
                    }
                    b.endObject();
                }
            ).and(b -> {
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
            })
        );

        // ip
        items.add(
            new TestCase(
                "ip1=192.168.0.1",
                "BwAAAD5km9raIz_rAAABfhMmioA",
                "KNj6cLPRNEkqdjfOPIbg0wULrOlWBpU_-efWDsz6B6AnnwbZ7GeeocE",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip1", "192.168.0.1");
                }
            ).and(b -> {
                b.field("@timestamp", "2022-01-01T01:00:00Z");
                b.field("r1", "cat");
                b.field("ip1", "::ffff:c0a8:1");
            })
        );
        items.add(
            new TestCase(
                "ip1=12.12.45.254",
                "BwAAAAWfEH_e_6wIAAABfhMmioA",
                "KNj6cLPRNEkqdjfOPIbg0wVhJ08TBpU_-bANzLhvKPczlle7Pq0z8Qw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip1", "12.12.45.254");
                }
            ).and(b -> {
                b.field("@timestamp", "2022-01-01T01:00:00Z");
                b.field("r1", "cat");
                b.field("ip1", "::ffff:c0c:2dfe");
            })
        );
        items.add(
            new TestCase(
                "ip2=FE80:CD00:0000:0CDE:1257:0000:211E:729C",
                "BwAAAGrrLHr1O4iQAAABfhMmioA",
                "KNDo3zGxO9HfN9XYJwKw2Z20h-WsBpU_-f4dSOLGSRlL1hoY2mgERuo",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip2", "FE80:CD00:0000:0CDE:1257:0000:211E:729C");
                }
            )
        );
        items.add(
            new TestCase(
                "o.ip3=2001:db8:85a3:8d3:1319:8a2e:370:7348",
                "BwAAAK7d-9aKOS1MAAABfhMmioA",
                "KLXDcBBWJAjgJvjSdF_EJwraAQUzBpU_-ba6HZsIyKnGcbmc3KRLlmI",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
                    }
                    b.endObject();
                }
            ).and(b -> {
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
            })
        );

        String huge = "foo ".repeat(200);
        items.add(
            new TestCase(
                "huge",
                "BwAAAPdECvXBSl3xAAABfhMmioA",
                "LIe18i0rRU_Bt9vB82F46LaS9mrUkvZq1K_2Gi7UEFMhFwNXrLA_H8TLpUr4",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("k1", huge);
                    b.field("k2", huge);
                    b.field("r1", "foo");
                }
            )
        );

        return items.stream().map(td -> new Object[] { td }).toList();
    }

    private final TestCase testCase;

    private static final int ROUTING_HASH = 7;

    public TsidExtractingIdFieldMapperTests(@Named("testCase") TestCase testCase) {
        this.testCase = testCase;
    }

    public void testExpectedId() throws IOException {
        assertThat(parse(mapperService(), testCase.source).id(), equalTo(testCase.expectedId));
    }

    public void testProvideExpectedId() throws IOException {
        assertThat(parse(testCase.expectedId, mapperService(), testCase.source).id(), equalTo(testCase.expectedId));
    }

    public void testEquivalentSources() throws IOException {
        MapperService mapperService = mapperService();
        for (CheckedConsumer<XContentBuilder, IOException> equivalent : testCase.equivalentSources) {
            assertThat(parse(mapperService, equivalent).id(), equalTo(testCase.expectedId));
        }
    }

    private ParsedDocument parse(MapperService mapperService, CheckedConsumer<XContentBuilder, IOException> source) throws IOException {
        return parse(null, mapperService, source);
    }

    private ParsedDocument parse(@Nullable String id, MapperService mapperService, CheckedConsumer<XContentBuilder, IOException> source)
        throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
            builder.startObject();
            source.accept(builder);
            builder.endObject();
            SourceToParse sourceToParse = new SourceToParse(
                id,
                BytesReference.bytes(builder),
                builder.contentType(),
                TimeSeriesRoutingHashFieldMapper.encode(ROUTING_HASH)
            );
            return mapperService.documentParser().parseDocument(sourceToParse, mapperService.mappingLookup());
        }
    }

    public void testRoutingPathCompliant() throws IOException {
        byte[] bytes = Base64.getUrlDecoder().decode(testCase.expectedId);
        assertEquals(ROUTING_HASH, ByteUtils.readIntLE(bytes, 0));
    }

    private Settings indexSettings(IndexVersion version) {
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
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());
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
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    public void testSourceDescription() throws IOException {
        assertThat(TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext()), equalTo("a time series document"));
        ParsedDocument d = parse(mapperService(), testCase.randomSource());
        IndexableField timestamp = d.rootDoc().getField(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(timestamp)),
            equalTo("a time series document at [" + testCase.expectedTimestamp + "]")
        );
        IndexableField tsid = d.rootDoc().getField(TimeSeriesIdFieldMapper.NAME);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(tsid)),
            equalTo("a time series document with tsid " + testCase.expectedTsid)
        );
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(tsid, timestamp)),
            equalTo("a time series document with tsid " + testCase.expectedTsid + " at [" + testCase.expectedTimestamp + "]")
        );
    }

    private TestDocumentParserContext documentParserContext(IndexableField... fields) throws IOException {
        TestDocumentParserContext ctx = new TestDocumentParserContext(
            mapperService().mappingLookup(),
            source(null, testCase.randomSource(), null)
        );
        for (IndexableField f : fields) {
            ctx.doc().add(f);
        }
        return ctx;
    }

    public void testParsedDescription() throws IOException {
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(parse(mapperService(), testCase.randomSource())),
            equalTo("[" + testCase.expectedId + "][" + testCase.expectedTsid + "@" + testCase.expectedTimestamp + "]")
        );
    }
}
