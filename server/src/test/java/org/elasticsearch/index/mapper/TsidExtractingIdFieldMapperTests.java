/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
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
        private final String expectedIdWithRoutingPath;
        private final String expectedIdWithIndexDimensions;
        private final String expectedTsidWithRoutingPath;
        private final String expectedTsidWithIndexDimensions;
        private final String expectedTimestamp;
        private final CheckedConsumer<XContentBuilder, IOException> source;
        private final boolean syntheticId;
        private final List<CheckedConsumer<XContentBuilder, IOException>> equivalentSources = new ArrayList<>();

        TestCase(
            String name,
            String expectedIdWithRoutingPath,
            String expectedIdWithIndexDimensions,
            String expectedTsidWithRoutingPath,
            String expectedTsidWithIndexDimensions,
            String expectedTimestamp,
            CheckedConsumer<XContentBuilder, IOException> source,
            boolean syntheticId
        ) {
            this.name = name;
            this.expectedIdWithRoutingPath = expectedIdWithRoutingPath;
            this.expectedIdWithIndexDimensions = expectedIdWithIndexDimensions;
            this.expectedTsidWithRoutingPath = expectedTsidWithRoutingPath;
            this.expectedTsidWithIndexDimensions = expectedTsidWithIndexDimensions;
            this.expectedTimestamp = expectedTimestamp;
            this.source = source;
            this.syntheticId = syntheticId;
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
                "BwAAAEk383E-IPhiAAABfhMmioA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "2022-01-01T01:00:01Z",
                "BwAAAKjcFfi45iV3AAABfhMmjmg",
                "BwAAAEk383E-IPhiAAABfhMmjmg",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "2022-01-01T01:00:01.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:01Z");
                    b.field("r1", "cat");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "1970-01-01T00:00:00Z",
                "BwAAAKjcFfi45iV3AAAAAAAAAAA",
                "BwAAAEk383E-IPhiAAAAAAAAAAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "1970-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "1970-01-01T00:00:00Z");
                    b.field("r1", "cat");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "-9998-01-01T00:00:00Z",
                "BwAAAKjcFfi45iV3__6oggRgGAA",
                "BwAAAEk383E-IPhi__6oggRgGAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "-9998-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "-9998-01-01T00:00:00Z");
                    b.field("r1", "cat");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "9998-01-01T00:00:00Z",
                "BwAAAKjcFfi45iV3AADmaSK9hAA",
                "BwAAAEk383E-IPhiAADmaSK9hAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "9998-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "9998-01-01T00:00:00Z");
                    b.field("r1", "cat");
                },
                false
            )
        );

        // routing keywords
        items.add(
            new TestCase(
                "r1",
                "BwAAAKjcFfi45iV3AAABfhMmioA",
                "BwAAAEk383E-IPhiAAABfhMmioA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                },
                false
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
                "BwAAAAjKcgBsE3XEAAABfhMmioA",
                "JNY_frTR9GmCbhXgK4Y8W44GlT_5e6_NYGOZWULpmMG9IAlZlA",
                "NHaFg-5cCESqVrBaDs6o9DZ4",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r2", "cat");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "o.r3",
                "BwAAAC1h1gf2J5a8AAABfhMmioA",
                "BwAAANZEz_KPYz9jAAABfhMmioA",
                "JEyfZsJIp3UNyfWG-4SjKFIGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "K3bwGazghpbQWy_dEU8UL4Ec",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o").field("r3", "cat").endObject();
                },
                false
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
                "BwAAACyv9sTdBEWNAAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bipyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "f8B2zv4qzO0Eq9YLPLUIlzRL8g",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "dog");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "k1=pumpkin",
                "BwAAAG8GX8-0QcFxAAABfhMmioA",
                "BwAAABp_KncX55XzAAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bibzw1JBpU_-VsHjSz5HC1yy_swPEM1iGo",
                "f0F2VfGtg5ltThy1tF7mWC_PGQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "pumpkin");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "k1=empty string",
                "BwAAAMna58i6D-Q6AAABfhMmioA",
                "BwAAAD-6gB-6kGvDAAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bhaCD7uBpU_-SWGG0Uv9tZ1mLO2gi9rC1I",
                "f8t2FhZTCAA2SSVLPSknT8Nf_A",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "k2",
                "BwAAAFqlzAuv-06kAAABfhMmioA",
                "BwAAAGEjL8LoBxUJAAABfhMmioA",
                "KB9H-tGrL_UzqMcqXcgBtzypyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "wMB2LHEQrqbi5fgySxCJAU5NhQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k2", "dog");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "o.k3",
                "BwAAAC_VhridAKDUAAABfhMmioA",
                "BwAAALNlofvJns7KAAABfhMmioA",
                "KGXATwN7ISd1_EycFRJ9h6qpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "esB2eX4haJwoe0Gz0Hxpr6mIcQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.startObject("o").field("k3", "dog").endObject();
                },
                false
            )
        );
        items.add(
            new TestCase(
                "o.r3,o.k3",
                "BwAAAEwfL7x__2oPAAABfhMmioA",
                "BwAAAOPQAwI_cPXTAAABfhMmioA",
                "KJaYZVZz8plfkEvvPBpi1EWpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "S8B2MTOcF1wV1_fEBh3Xpv0JJA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("k3", "dog");
                    }
                    b.endObject();
                },
                false
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
                "BwAAALQa3_B5oSRHAAABfhMmioA",
                "KI4kVxcCLIMM2_VQGD575d-tm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "C8t2xhEnokpbS-rpcaL9_JYgRQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L1", 1);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "L1=min",
                "BwAAAAhu7hy1RoXRAAABfhMmioA",
                "BwAAAHJevaZ42qO0AAABfhMmioA",
                "KI4kVxcCLIMM2_VQGD575d8caJ3TBpU_-cLpg-VnCBnhYk33HZBle6E",
                "C-92gIC6326BrsHHzrPq6L6b7w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L1", Long.MIN_VALUE);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "L2=1234",
                "BwAAAATrNu7TTpc-AAABfhMmioA",
                "BwAAAAykDD72vvVsAAABfhMmioA",
                "KI_1WxF60L0IczG5ftUCWdndcGtgBpU_-QfM2BaR0DMagIfw3TDu_mA",
                "tCp2ar0mjlGW0gXveQToNzewqg",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L2", 1234);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "o.L3=max",
                "BwAAAGBQI6THHqxoAAABfhMmioA",
                "BwAAAMipdDqKd-XPAAABfhMmioA",
                "KN4a6QzKhzc3nwzNLuZkV51xxTOVBpU_-erUU1qSW4eJ0kP0RmAB9TE",
                "S5l2EpF1iWUfxVvyegvENonWEw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00.000Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("L3", Long.MAX_VALUE);
                    }
                    b.endObject();
                },
                false
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
                "BwAAABKQqNW1IG5AAAABfhMmioA",
                "KLGFpvAV8QkWSmX54kXFMgitm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "nct2JZgFouaJFx9FPyphfxTTBw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i1", 1);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "i1=min",
                "BwAAAKdlQM5ILoA1AAABfhMmioA",
                "BwAAAFVne7eYMxr1AAABfhMmioA",
                "KLGFpvAV8QkWSmX54kXFMgjV8hFQBpU_-WG2MicRGWwJdBKWq2F4qy4",
                "ncx27Tdr-Fw8YsGkFZT_X2isdg",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i1", Integer.MIN_VALUE);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "i2=1234",
                "BwAAALhxfB6J0kBFAAABfhMmioA",
                "BwAAAOfBxmKrLd6MAAABfhMmioA",
                "KJc4-5eN1uAlYuAknQQLUlxavn2sBpU_-UEXBjgaH1uYcbayrOhdgpc",
                "7RN22sJMTvFnwNcgZkI3oCIBvQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i2", 1324);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "o.i3=max",
                "BwAAAOlxKf19CbfdAAABfhMmioA",
                "BwAAAPasG-voJ30IAAABfhMmioA",
                "KKqnzPNBe8ObksSo8rNaIFPZPCcBBpU_-Rhd_U6Jn2pjQz2zpmBuJb4",
                "ae1249zvwt9WR5M0TsFbT-_R4A",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("i3", Integer.MAX_VALUE);
                    }
                    b.endObject();
                },
                false
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
                "BwAAAFXFZfzCD9-tAAABfhMmioA",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5rZuNb-1ruHTTZhtsXRZpZRwWFoc",
                "E3bLGR5i_E-AjHaQj6NSgLqXqQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s1", 1);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "s1=min",
                "BwAAAGV8VNVnmPVNAAABfhMmioA",
                "BwAAAHMdz2yGuWQ5AAABfhMmioA",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5JgBZj9BSCms2_jgeFFhsmDlNFdM",
                "E3YA0Uj0mUMBIfe85M1eDvKJVg",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s1", Short.MIN_VALUE);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "s2=1234",
                "BwAAAFO8mUr-J5CpAAABfhMmioA",
                "BwAAAKYKsByk_H1KAAABfhMmioA",
                "KKEQ2p3CkpMH61hNk_SuvI0GlT_53XBrYP5TPdmCR-vREPnt20e9f9w",
                "CHYqMfreOGDp369Up7wkbxfD7g",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s2", 1234);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "o.s3=min",
                "BwAAAAKh6K11zWeuAAABfhMmioA",
                "BwAAAJ00j5PXlZF8AAABfhMmioA",
                "KKVMoT_-GS95fvIBtR7XK9oGlT_5Dme9-H3sen0WZ7leJpCj7-vXau4",
                "BXaV38EUXIsWps32nOam3wdX-g",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("s3", Short.MAX_VALUE);
                    }
                    b.endObject();
                },
                false
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
                "BwAAACaAVo28B16ZAAABfhMmioA",
                "KGPAUhTjWOsRfDmYp3SUELatm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "Uct2hEdPEZMCnGZ0_NM3so6Z3w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b1", 1);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "b1=min",
                "BwAAAN_PD--DgUvoAAABfhMmioA",
                "BwAAAMkeTGox24cdAAABfhMmioA",
                "KGPAUhTjWOsRfDmYp3SUELYoK6qHBpU_-d8HkZFJ3aL2ZV1lgHAjT1g",
                "USF2EOdV0G1EpXE-GDtFWr7jHg",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b1", Byte.MIN_VALUE);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "b2=12",
                "BwAAAKqX5QjiuhsEAAABfhMmioA",
                "BwAAAB3kQls40KoVAAABfhMmioA",
                "KA58oUMzXeX1V5rh51Ste0K5K9vPBpU_-Wn8JQplO-x3CgoslYO5Vks",
                "06J2CccOS3823wXT-Ntmaaz8nw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b2", 12);
                },
                false
            )
        );
        items.add(
            new TestCase(
                "o.s3=max",
                "BwAAAMJ4YtN_21XHAAABfhMmioA",
                "BwAAAD25hXYPluEiAAABfhMmioA",
                "KIwZH-StJBobjk9tCV-0OgjKmuwGBpU_-Sd-SdnoH3sbfKLgse-briE",
                "P5N2wn3fYvtVKKIOWB2n7AQweA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("b3", Byte.MAX_VALUE);
                    }
                    b.endObject();
                },
                false
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
                "BwAAABJYg92lKoxdAAABfhMmioA",
                "KNj6cLPRNEkqdjfOPIbg0wULrOlWBpU_-efWDsz6B6AnnwbZ7GeeocE",
                "3ft2E9wEcQ7qLhAhG-bgQxIC_w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip1", "192.168.0.1");
                },
                false
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
                "BwAAAEqgz7x99nlVAAABfhMmioA",
                "KNj6cLPRNEkqdjfOPIbg0wVhJ08TBpU_-bANzLhvKPczlle7Pq0z8Qw",
                "3RV2CkXc8fttjm4g1xCOxnlI7A",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip1", "12.12.45.254");
                },
                false
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
                "BwAAAHTrphGgaaxEAAABfhMmioA",
                "KNDo3zGxO9HfN9XYJwKw2Z20h-WsBpU_-f4dSOLGSRlL1hoY2mgERuo",
                "4ch2pulC-JV1tGryTUWFGMsHdQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip2", "FE80:CD00:0000:0CDE:1257:0000:211E:729C");
                },
                false
            )
        );
        items.add(
            new TestCase(
                "o.ip3=2001:db8:85a3:8d3:1319:8a2e:370:7348",
                "BwAAAK7d-9aKOS1MAAABfhMmioA",
                "BwAAANak4OD67dUVAAABfhMmioA",
                "KLXDcBBWJAjgJvjSdF_EJwraAQUzBpU_-ba6HZsIyKnGcbmc3KRLlmI",
                "ZKJ28_e_PHW9GEdDVaEHOBblSw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
                    }
                    b.endObject();
                },
                false
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
                "BwAAAEDsUEqRn2O2AAABfhMmioA",
                "LIe18i0rRU_Bt9vB82F46LaS9mrUkvZq1K_2Gi7UEFMhFwNXrLA_H8TLpUr4",
                "tIaGaQ90mXYrnllFi07m7A2pRMI",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("k1", huge);
                    b.field("k2", huge);
                    b.field("r1", "foo");
                },
                false
            )
        );

        // WITH SYNTHETIC ID BELOW

        // Dates
        items.add(
            new TestCase(
                "2022-01-01T01:00:00Z (syntheticId)",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlH___oHs2XV_AAAABw",
                "0XbnnsE9AoHbGpRryIzXGQ0wf__-gezZdX8AAAAH",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "2022-01-01T01:00:01Z (syntheticId)",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlH___oHs2XGXAAAABw",
                "0XbnnsE9AoHbGpRryIzXGQ0wf__-gezZcZcAAAAH",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "2022-01-01T01:00:01.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:01Z");
                    b.field("r1", "cat");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "1970-01-01T00:00:00Z (syntheticId)",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlH__________AAAABw",
                "0XbnnsE9AoHbGpRryIzXGQ0wf_________8AAAAH",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "1970-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "1970-01-01T00:00:00Z");
                    b.field("r1", "cat");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "-9998-01-01T00:00:00Z (syntheticId)",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlIABV337n-f_AAAABw",
                "0XbnnsE9AoHbGpRryIzXGQ0wgAFXffuf5_8AAAAH",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "-9998-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "-9998-01-01T00:00:00Z");
                    b.field("r1", "cat");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "9998-01-01T00:00:00Z (syntheticId)",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlH__GZbdQnv_AAAABw",
                "0XbnnsE9AoHbGpRryIzXGQ0wf_8Zlt1Ce_8AAAAH",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "9998-01-01T00:00:00.000Z",
                b -> {
                    b.field("@timestamp", "9998-01-01T00:00:00Z");
                    b.field("r1", "cat");
                },
                true
            )
        );

        // routing keywords
        items.add(
            new TestCase(
                "r1 (syntheticId)",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlH___oHs2XV_AAAABw",
                "0XbnnsE9AoHbGpRryIzXGQ0wf__-gezZdX8AAAAH",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                },
                true
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
                "r2 (syntheticId)",
                "JNY_frTR9GmCbhXgK4Y8W44GlT_5e6_NYGOZWULpmMG9IAlZlH___oHs2XV_AAAABw",
                "NHaFg-5cCESqVrBaDs6o9DZ4f__-gezZdX8AAAAH",
                "JNY_frTR9GmCbhXgK4Y8W44GlT_5e6_NYGOZWULpmMG9IAlZlA",
                "NHaFg-5cCESqVrBaDs6o9DZ4",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r2", "cat");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "o.r3 (syntheticId)",
                "JEyfZsJIp3UNyfWG-4SjKFIGlT_5e6_NYGOZWULpmMG9IAlZlH___oHs2XV_AAAABw",
                "K3bwGazghpbQWy_dEU8UL4Ecf__-gezZdX8AAAAH",
                "JEyfZsJIp3UNyfWG-4SjKFIGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "K3bwGazghpbQWy_dEU8UL4Ec",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o").field("r3", "cat").endObject();
                },
                true
            ).and(b -> {
                b.field("@timestamp", "2022-01-01T01:00:00Z");
                b.field("o.r3", "cat");
            })
        );

        // non-routing keyword
        items.add(
            new TestCase(
                "k1=dog (syntheticId)",
                "KJQKpjU9U63jhh-eNJ1f8bipyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOZ___6B7Nl1fwAAAAc",
                "f8B2zv4qzO0Eq9YLPLUIlzRL8n___oHs2XV_AAAABw",
                "KJQKpjU9U63jhh-eNJ1f8bipyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "f8B2zv4qzO0Eq9YLPLUIlzRL8g",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "dog");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "k1=pumpkin (syntheticId)",
                "KJQKpjU9U63jhh-eNJ1f8bibzw1JBpU_-VsHjSz5HC1yy_swPEM1iGp___6B7Nl1fwAAAAc",
                "f0F2VfGtg5ltThy1tF7mWC_PGX___oHs2XV_AAAABw",
                "KJQKpjU9U63jhh-eNJ1f8bibzw1JBpU_-VsHjSz5HC1yy_swPEM1iGo",
                "f0F2VfGtg5ltThy1tF7mWC_PGQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "pumpkin");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "k1=empty string (syntheticId)",
                "KJQKpjU9U63jhh-eNJ1f8bhaCD7uBpU_-SWGG0Uv9tZ1mLO2gi9rC1J___6B7Nl1fwAAAAc",
                "f8t2FhZTCAA2SSVLPSknT8Nf_H___oHs2XV_AAAABw",
                "KJQKpjU9U63jhh-eNJ1f8bhaCD7uBpU_-SWGG0Uv9tZ1mLO2gi9rC1I",
                "f8t2FhZTCAA2SSVLPSknT8Nf_A",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k1", "");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "k2 (syntheticId)",
                "KB9H-tGrL_UzqMcqXcgBtzypyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOZ___6B7Nl1fwAAAAc",
                "wMB2LHEQrqbi5fgySxCJAU5NhX___oHs2XV_AAAABw",
                "KB9H-tGrL_UzqMcqXcgBtzypyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "wMB2LHEQrqbi5fgySxCJAU5NhQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("k2", "dog");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "o.k3 (syntheticId)",
                "KGXATwN7ISd1_EycFRJ9h6qpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOZ___6B7Nl1fwAAAAc",
                "esB2eX4haJwoe0Gz0Hxpr6mIcX___oHs2XV_AAAABw",
                "KGXATwN7ISd1_EycFRJ9h6qpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "esB2eX4haJwoe0Gz0Hxpr6mIcQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.startObject("o").field("k3", "dog").endObject();
                },
                true
            )
        );
        items.add(
            new TestCase(
                "o.r3,o.k3 (syntheticId)",
                "KJaYZVZz8plfkEvvPBpi1EWpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOZ___6B7Nl1fwAAAAc",
                "S8B2MTOcF1wV1_fEBh3Xpv0JJH___oHs2XV_AAAABw",
                "KJaYZVZz8plfkEvvPBpi1EWpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "S8B2MTOcF1wV1_fEBh3Xpv0JJA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("k3", "dog");
                    }
                    b.endObject();
                },
                true
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
                "L1=1 (syntheticId)",
                "KI4kVxcCLIMM2_VQGD575d-tm41vBpU_-TUExUU_bL3Puq_EBgIaLad___6B7Nl1fwAAAAc",
                "C8t2xhEnokpbS-rpcaL9_JYgRX___oHs2XV_AAAABw",
                "KI4kVxcCLIMM2_VQGD575d-tm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "C8t2xhEnokpbS-rpcaL9_JYgRQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L1", 1);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "L1=min (syntheticId)",
                "KI4kVxcCLIMM2_VQGD575d8caJ3TBpU_-cLpg-VnCBnhYk33HZBle6F___6B7Nl1fwAAAAc",
                "C-92gIC6326BrsHHzrPq6L6b73___oHs2XV_AAAABw",
                "KI4kVxcCLIMM2_VQGD575d8caJ3TBpU_-cLpg-VnCBnhYk33HZBle6E",
                "C-92gIC6326BrsHHzrPq6L6b7w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L1", Long.MIN_VALUE);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "L2=1234 (syntheticId)",
                "KI_1WxF60L0IczG5ftUCWdndcGtgBpU_-QfM2BaR0DMagIfw3TDu_mB___6B7Nl1fwAAAAc",
                "tCp2ar0mjlGW0gXveQToNzewqn___oHs2XV_AAAABw",
                "KI_1WxF60L0IczG5ftUCWdndcGtgBpU_-QfM2BaR0DMagIfw3TDu_mA",
                "tCp2ar0mjlGW0gXveQToNzewqg",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("L2", 1234);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "o.L3=max (syntheticId)",
                "KN4a6QzKhzc3nwzNLuZkV51xxTOVBpU_-erUU1qSW4eJ0kP0RmAB9TF___6B7Nl1fwAAAAc",
                "S5l2EpF1iWUfxVvyegvENonWE3___oHs2XV_AAAABw",
                "KN4a6QzKhzc3nwzNLuZkV51xxTOVBpU_-erUU1qSW4eJ0kP0RmAB9TE",
                "S5l2EpF1iWUfxVvyegvENonWEw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00.000Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("L3", Long.MAX_VALUE);
                    }
                    b.endObject();
                },
                true
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
                "i1=1 (syntheticId)",
                "KLGFpvAV8QkWSmX54kXFMgitm41vBpU_-TUExUU_bL3Puq_EBgIaLad___6B7Nl1fwAAAAc",
                "nct2JZgFouaJFx9FPyphfxTTB3___oHs2XV_AAAABw",
                "KLGFpvAV8QkWSmX54kXFMgitm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "nct2JZgFouaJFx9FPyphfxTTBw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i1", 1);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "i1=min (syntheticId)",
                "KLGFpvAV8QkWSmX54kXFMgjV8hFQBpU_-WG2MicRGWwJdBKWq2F4qy5___6B7Nl1fwAAAAc",
                "ncx27Tdr-Fw8YsGkFZT_X2isdn___oHs2XV_AAAABw",
                "KLGFpvAV8QkWSmX54kXFMgjV8hFQBpU_-WG2MicRGWwJdBKWq2F4qy4",
                "ncx27Tdr-Fw8YsGkFZT_X2isdg",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i1", Integer.MIN_VALUE);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "i2=1234 (syntheticId)",
                "KJc4-5eN1uAlYuAknQQLUlxavn2sBpU_-UEXBjgaH1uYcbayrOhdgpd___6B7Nl1fwAAAAc",
                "7RN22sJMTvFnwNcgZkI3oCIBvX___oHs2XV_AAAABw",
                "KJc4-5eN1uAlYuAknQQLUlxavn2sBpU_-UEXBjgaH1uYcbayrOhdgpc",
                "7RN22sJMTvFnwNcgZkI3oCIBvQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("i2", 1324);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "o.i3=max (syntheticId)",
                "KKqnzPNBe8ObksSo8rNaIFPZPCcBBpU_-Rhd_U6Jn2pjQz2zpmBuJb5___6B7Nl1fwAAAAc",
                "ae1249zvwt9WR5M0TsFbT-_R4H___oHs2XV_AAAABw",
                "KKqnzPNBe8ObksSo8rNaIFPZPCcBBpU_-Rhd_U6Jn2pjQz2zpmBuJb4",
                "ae1249zvwt9WR5M0TsFbT-_R4A",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("i3", Integer.MAX_VALUE);
                    }
                    b.endObject();
                },
                true
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
                "s1=1 (syntheticId)",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5rZuNb-1ruHTTZhtsXRZpZRwWFod___6B7Nl1fwAAAAc",
                "E3bLGR5i_E-AjHaQj6NSgLqXqX___oHs2XV_AAAABw",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5rZuNb-1ruHTTZhtsXRZpZRwWFoc",
                "E3bLGR5i_E-AjHaQj6NSgLqXqQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s1", 1);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "s1=min (syntheticId)",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5JgBZj9BSCms2_jgeFFhsmDlNFdN___6B7Nl1fwAAAAc",
                "E3YA0Uj0mUMBIfe85M1eDvKJVn___oHs2XV_AAAABw",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5JgBZj9BSCms2_jgeFFhsmDlNFdM",
                "E3YA0Uj0mUMBIfe85M1eDvKJVg",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s1", Short.MIN_VALUE);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "s2=1234 (syntheticId)",
                "KKEQ2p3CkpMH61hNk_SuvI0GlT_53XBrYP5TPdmCR-vREPnt20e9f9x___6B7Nl1fwAAAAc",
                "CHYqMfreOGDp369Up7wkbxfD7n___oHs2XV_AAAABw",
                "KKEQ2p3CkpMH61hNk_SuvI0GlT_53XBrYP5TPdmCR-vREPnt20e9f9w",
                "CHYqMfreOGDp369Up7wkbxfD7g",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("s2", 1234);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "o.s3=min (syntheticId)",
                "KKVMoT_-GS95fvIBtR7XK9oGlT_5Dme9-H3sen0WZ7leJpCj7-vXau5___6B7Nl1fwAAAAc",
                "BXaV38EUXIsWps32nOam3wdX-n___oHs2XV_AAAABw",
                "KKVMoT_-GS95fvIBtR7XK9oGlT_5Dme9-H3sen0WZ7leJpCj7-vXau4",
                "BXaV38EUXIsWps32nOam3wdX-g",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("s3", Short.MAX_VALUE);
                    }
                    b.endObject();
                },
                true
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
                "b1=1 (syntheticId)",
                "KGPAUhTjWOsRfDmYp3SUELatm41vBpU_-TUExUU_bL3Puq_EBgIaLad___6B7Nl1fwAAAAc",
                "Uct2hEdPEZMCnGZ0_NM3so6Z33___oHs2XV_AAAABw",
                "KGPAUhTjWOsRfDmYp3SUELatm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "Uct2hEdPEZMCnGZ0_NM3so6Z3w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b1", 1);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "b1=min (syntheticId)",
                "KGPAUhTjWOsRfDmYp3SUELYoK6qHBpU_-d8HkZFJ3aL2ZV1lgHAjT1h___6B7Nl1fwAAAAc",
                "USF2EOdV0G1EpXE-GDtFWr7jHn___oHs2XV_AAAABw",
                "KGPAUhTjWOsRfDmYp3SUELYoK6qHBpU_-d8HkZFJ3aL2ZV1lgHAjT1g",
                "USF2EOdV0G1EpXE-GDtFWr7jHg",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b1", Byte.MIN_VALUE);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "b2=12 (syntheticId)",
                "KA58oUMzXeX1V5rh51Ste0K5K9vPBpU_-Wn8JQplO-x3CgoslYO5Vkt___6B7Nl1fwAAAAc",
                "06J2CccOS3823wXT-Ntmaaz8n3___oHs2XV_AAAABw",
                "KA58oUMzXeX1V5rh51Ste0K5K9vPBpU_-Wn8JQplO-x3CgoslYO5Vks",
                "06J2CccOS3823wXT-Ntmaaz8nw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("b2", 12);
                },
                true
            )
        );
        items.add(
            new TestCase(
                "o.s3=max (syntheticId)",
                "KIwZH-StJBobjk9tCV-0OgjKmuwGBpU_-Sd-SdnoH3sbfKLgse-briF___6B7Nl1fwAAAAc",
                "P5N2wn3fYvtVKKIOWB2n7AQweH___oHs2XV_AAAABw",
                "KIwZH-StJBobjk9tCV-0OgjKmuwGBpU_-Sd-SdnoH3sbfKLgse-briE",
                "P5N2wn3fYvtVKKIOWB2n7AQweA",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("b3", Byte.MAX_VALUE);
                    }
                    b.endObject();
                },
                true
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
                "ip1=192.168.0.1 (syntheticId)",
                "KNj6cLPRNEkqdjfOPIbg0wULrOlWBpU_-efWDsz6B6AnnwbZ7GeeocF___6B7Nl1fwAAAAc",
                "3ft2E9wEcQ7qLhAhG-bgQxIC_3___oHs2XV_AAAABw",
                "KNj6cLPRNEkqdjfOPIbg0wULrOlWBpU_-efWDsz6B6AnnwbZ7GeeocE",
                "3ft2E9wEcQ7qLhAhG-bgQxIC_w",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip1", "192.168.0.1");
                },
                true
            ).and(b -> {
                b.field("@timestamp", "2022-01-01T01:00:00Z");
                b.field("r1", "cat");
                b.field("ip1", "::ffff:c0a8:1");
            })
        );
        items.add(
            new TestCase(
                "ip1=12.12.45.254 (syntheticId)",
                "KNj6cLPRNEkqdjfOPIbg0wVhJ08TBpU_-bANzLhvKPczlle7Pq0z8Qx___6B7Nl1fwAAAAc",
                "3RV2CkXc8fttjm4g1xCOxnlI7H___oHs2XV_AAAABw",
                "KNj6cLPRNEkqdjfOPIbg0wVhJ08TBpU_-bANzLhvKPczlle7Pq0z8Qw",
                "3RV2CkXc8fttjm4g1xCOxnlI7A",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip1", "12.12.45.254");
                },
                true
            ).and(b -> {
                b.field("@timestamp", "2022-01-01T01:00:00Z");
                b.field("r1", "cat");
                b.field("ip1", "::ffff:c0c:2dfe");
            })
        );
        items.add(
            new TestCase(
                "ip2=FE80:CD00:0000:0CDE:1257:0000:211E:729C (syntheticId)",
                "KNDo3zGxO9HfN9XYJwKw2Z20h-WsBpU_-f4dSOLGSRlL1hoY2mgERup___6B7Nl1fwAAAAc",
                "4ch2pulC-JV1tGryTUWFGMsHdX___oHs2XV_AAAABw",
                "KNDo3zGxO9HfN9XYJwKw2Z20h-WsBpU_-f4dSOLGSRlL1hoY2mgERuo",
                "4ch2pulC-JV1tGryTUWFGMsHdQ",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("r1", "cat");
                    b.field("ip2", "FE80:CD00:0000:0CDE:1257:0000:211E:729C");
                },
                true
            )
        );
        items.add(
            new TestCase(
                "o.ip3=2001:db8:85a3:8d3:1319:8a2e:370:7348 (syntheticId)",
                "KLXDcBBWJAjgJvjSdF_EJwraAQUzBpU_-ba6HZsIyKnGcbmc3KRLlmJ___6B7Nl1fwAAAAc",
                "ZKJ28_e_PHW9GEdDVaEHOBblS3___oHs2XV_AAAABw",
                "KLXDcBBWJAjgJvjSdF_EJwraAQUzBpU_-ba6HZsIyKnGcbmc3KRLlmI",
                "ZKJ28_e_PHW9GEdDVaEHOBblSw",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.startObject("o");
                    {
                        b.field("r3", "cat");
                        b.field("ip3", "2001:db8:85a3:8d3:1319:8a2e:370:7348");
                    }
                    b.endObject();
                },
                true
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

        items.add(
            new TestCase(
                "huge (syntheticId)",
                "LIe18i0rRU_Bt9vB82F46LaS9mrUkvZq1K_2Gi7UEFMhFwNXrLA_H8TLpUr4f__-gezZdX8AAAAH",
                "tIaGaQ90mXYrnllFi07m7A2pRMJ___6B7Nl1fwAAAAc",
                "LIe18i0rRU_Bt9vB82F46LaS9mrUkvZq1K_2Gi7UEFMhFwNXrLA_H8TLpUr4",
                "tIaGaQ90mXYrnllFi07m7A2pRMI",
                "2022-01-01T01:00:00.000Z",
                b -> {
                    b.field("@timestamp", "2022-01-01T01:00:00Z");
                    b.field("k1", huge);
                    b.field("k2", huge);
                    b.field("r1", "foo");
                },
                true
            )
        );

        return items.stream().map(td -> new Object[] { td }).toList();
    }

    private final TestCase testCase;
    private final BlockLoaderTestRunner blockLoaderTestRunner;

    private static final int ROUTING_HASH = 7;

    public TsidExtractingIdFieldMapperTests(@Named("testCase") TestCase testCase) {
        this.testCase = testCase;
        this.blockLoaderTestRunner = new BlockLoaderTestRunner(
            new BlockLoaderTestCase.Params(false, randomFrom(MappedFieldType.FieldExtractPreference.values()))
        ).breaker(newLimitedBreaker(ByteSizeValue.ofMb(1)));
    }

    public void testExpectedIdWithRoutingPath() throws IOException {
        assumeFalse("BlockLoaderTestRunner does not support synthetic ID", testCase.syntheticId);
        MapperService mapperService = mapperService(
            false,
            false,
            IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)
        );
        blockLoaderTestRunner.mapperService(mapperService);
        blockLoaderTestRunner.document(parse(mapperService, testCase.source));
        assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithRoutingPath));
        verifyIdFromBlockLoader(testCase.expectedIdWithRoutingPath);
    }

    public void testExpectedIdWithIndexDimensions() throws IOException {
        assumeFalse("BlockLoaderTestRunner does not support synthetic ID", testCase.syntheticId);
        MapperService mapperService = mapperService(
            true,
            false,
            IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_CREATED_DURING_ROUTING)
        );
        blockLoaderTestRunner.mapperService(mapperService);
        blockLoaderTestRunner.document(parse(mapperService, testCase.source));
        assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithIndexDimensions));
        verifyIdFromBlockLoader(testCase.expectedIdWithIndexDimensions);
    }

    public void testProvideExpectedIdWithRoutingPath() throws IOException {
        assumeFalse("BlockLoaderTestRunner does not support synthetic ID", testCase.syntheticId);
        MapperService mapperService = mapperService(
            false,
            false,
            IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)
        );
        blockLoaderTestRunner.mapperService(mapperService);
        blockLoaderTestRunner.document(parse(testCase.expectedIdWithRoutingPath, mapperService, testCase.source));
        assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithRoutingPath));
        verifyIdFromBlockLoader(testCase.expectedIdWithRoutingPath);
    }

    public void testProvideExpectedIdWithIndexDimensions() throws IOException {
        assumeFalse("BlockLoaderTestRunner does not support synthetic ID", testCase.syntheticId);
        MapperService mapperService = mapperService(
            true,
            false,
            IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_CREATED_DURING_ROUTING)
        );
        blockLoaderTestRunner.mapperService(mapperService);
        blockLoaderTestRunner.document(parse(testCase.expectedIdWithIndexDimensions, mapperService, testCase.source));
        assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithIndexDimensions));
        verifyIdFromBlockLoader(testCase.expectedIdWithIndexDimensions);
    }

    public void testEquivalentSourcesWithRoutingPath() throws IOException {
        assumeFalse("BlockLoaderTestRunner does not support synthetic ID", testCase.syntheticId);
        MapperService mapperService = mapperService(
            false,
            false,
            IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)
        );
        for (CheckedConsumer<XContentBuilder, IOException> equivalent : testCase.equivalentSources) {
            blockLoaderTestRunner.mapperService(mapperService);
            blockLoaderTestRunner.document(parse(mapperService, equivalent));
            assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithRoutingPath));
            verifyIdFromBlockLoader(testCase.expectedIdWithRoutingPath);
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

    public void testRoutingHashCompliantWithRoutingPath() {
        byte[] bytes = Base64.getUrlDecoder().decode(testCase.expectedIdWithRoutingPath);
        int routingHash = getRoutingHash(bytes, testCase.syntheticId);
        assertEquals(ROUTING_HASH, routingHash);
    }

    public void testRoutingHashCompliantWithIndexDimensions() {
        byte[] bytes = Base64.getUrlDecoder().decode(testCase.expectedIdWithIndexDimensions);
        int routingHash = getRoutingHash(bytes, testCase.syntheticId);
        assertEquals(ROUTING_HASH, routingHash);
    }

    /**
     * Extract the routing hash from the bytes of a decoded id string.
     * This method mirrors the logic of how the routingHash is encoded at
     * {@link TsidExtractingIdFieldMapper#createId(int, BytesRef, long)} and
     * {@link TsidExtractingIdFieldMapper#createSyntheticId(BytesRef, long, int)}.
     */
    private static int getRoutingHash(byte[] bytes, boolean syntheticId) {
        if (syntheticId) {
            return ByteUtils.readIntBE(bytes, bytes.length - Integer.BYTES);
        } else {
            return ByteUtils.readIntLE(bytes, 0);
        }
    }

    private Settings indexSettings(IndexVersion version, boolean indexDimensions, boolean syntheticId) {
        Settings.Builder builder = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 100))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), 100);
        if (indexDimensions) {
            builder.put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "r1,r2,k1,k2,L1,L2,i1,i2,s1,s2,b1,b2,ip1,ip2,o.*");
        } else {
            builder.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "r1,r2,o.r3");
        }
        if (IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG && version.onOrAfter(IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94)) {
            builder.put(IndexSettings.SYNTHETIC_ID.getKey(), syntheticId);
        }
        return builder.build();
    }

    private MapperService mapperService(boolean indexDimensions, boolean syntheticId, IndexVersion version) throws IOException {
        if (syntheticId) {
            assumeTrue("Only run with syntheticId if feature flag is enabled", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        }

        return createMapperService(version, indexSettings(version, indexDimensions, syntheticId), mapping(b -> {
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

    public void testSourceDescriptionWithRoutingPath() throws IOException {
        IndexVersion minimalVersion = testCase.syntheticId
            ? IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94
            : IndexVersions.TIME_SERIES_ID_HASHING;
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(minimalVersion);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(version, false)),
            equalTo("a time series document")
        );
        ParsedDocument d = parse(mapperService(false, testCase.syntheticId, version), testCase.randomSource());
        IndexableField timestamp = d.rootDoc().getField(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(version, false, timestamp)),
            equalTo("a time series document at [" + testCase.expectedTimestamp + "]")
        );
        IndexableField tsid = d.rootDoc().getField(TimeSeriesIdFieldMapper.NAME);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(version, false, tsid)),
            equalTo("a time series document with tsid " + testCase.expectedTsidWithRoutingPath)
        );
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(version, false, tsid, timestamp)),
            equalTo("a time series document with tsid " + testCase.expectedTsidWithRoutingPath + " at [" + testCase.expectedTimestamp + "]")
        );
    }

    public void testSourceDescriptionWithIndexDimensions() throws IOException {
        IndexVersion minimalVersion = testCase.syntheticId
            ? IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94
            : IndexVersions.TIME_SERIES_ID_HASHING;
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(minimalVersion);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(version, false)),
            equalTo("a time series document")
        );
        ParsedDocument d = parse(mapperService(false, testCase.syntheticId, version), testCase.randomSource());
        IndexableField timestamp = d.rootDoc().getField(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(version, false, timestamp)),
            equalTo("a time series document at [" + testCase.expectedTimestamp + "]")
        );
        IndexableField tsid = d.rootDoc().getField(TimeSeriesIdFieldMapper.NAME);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(version, false, tsid)),
            equalTo("a time series document with tsid " + testCase.expectedTsidWithRoutingPath)
        );
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(version, false, tsid, timestamp)),
            equalTo("a time series document with tsid " + testCase.expectedTsidWithRoutingPath + " at [" + testCase.expectedTimestamp + "]")
        );
    }

    private TestDocumentParserContext documentParserContext(IndexVersion version, boolean indexDimensions, IndexableField... fields)
        throws IOException {
        CheckedConsumer<XContentBuilder, IOException> source;
        // not using a random source here as the index.dimensions id is sensitive to how ips are represented (e.g. equivalent ipv4 vs ipv6)
        if (indexDimensions) {
            source = testCase.source;
        } else {
            source = testCase.randomSource();
        }
        TestDocumentParserContext ctx = new TestDocumentParserContext(
            mapperService(indexDimensions, testCase.syntheticId, version).mappingLookup(),
            source(null, source, null),
            version
        );
        for (IndexableField f : fields) {
            ctx.doc().add(f);
        }
        return ctx;
    }

    public void testParsedDescriptionWithRoutingPath() throws IOException {
        IndexVersion minimumVersion = testCase.syntheticId
            ? IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94
            : IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID;
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(minimumVersion);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(
                parse(mapperService(false, testCase.syntheticId, version), testCase.randomSource())
            ),
            equalTo(
                "["
                    + testCase.expectedIdWithRoutingPath
                    + "]["
                    + testCase.expectedTsidWithRoutingPath
                    + "@"
                    + testCase.expectedTimestamp
                    + "]"
            )
        );
    }

    public void testParsedDescriptionWithIndexDimensions() throws IOException {
        // not using a random source here as the index.dimensions id is sensitive to how ips are represented (e.g. equivalent ipv4 vs ipv6)
        IndexVersion minimumVersion = testCase.syntheticId
            ? IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_94
            : IndexVersions.TSID_CREATED_DURING_ROUTING;
        IndexVersion version = IndexVersionUtils.randomVersionOnOrAfter(minimumVersion);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(
                parse(mapperService(true, testCase.syntheticId, version), testCase.source)
            ),
            equalTo(
                "["
                    + testCase.expectedIdWithIndexDimensions
                    + "]["
                    + testCase.expectedTsidWithIndexDimensions
                    + "@"
                    + testCase.expectedTimestamp
                    + "]"
            )
        );
    }

    private void verifyIdFromBlockLoader(String expectedId) throws IOException {
        blockLoaderTestRunner.fieldName("_id").run(new BytesRef(expectedId));
    }
}
