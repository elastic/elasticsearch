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
        private final String expectedIdWithRoutingPath;
        private final String expectedIdWithIndexDimensions;
        private final String expectedTsidWithRoutingPath;
        private final String expectedTsidWithIndexDimensions;
        private final String expectedTimestamp;
        private final CheckedConsumer<XContentBuilder, IOException> source;
        private final List<CheckedConsumer<XContentBuilder, IOException>> equivalentSources = new ArrayList<>();

        TestCase(
            String name,
            String expectedIdWithRoutingPath,
            String expectedIdWithIndexDimensions,
            String expectedTsidWithRoutingPath,
            String expectedTsidWithIndexDimensions,
            String expectedTimestamp,
            CheckedConsumer<XContentBuilder, IOException> source
        ) {
            this.name = name;
            this.expectedIdWithRoutingPath = expectedIdWithRoutingPath;
            this.expectedIdWithIndexDimensions = expectedIdWithIndexDimensions;
            this.expectedTsidWithRoutingPath = expectedTsidWithRoutingPath;
            this.expectedTsidWithIndexDimensions = expectedTsidWithIndexDimensions;
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
                "BwAAAEk383E-IPhiAAABfhMmioA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
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
                "BwAAAEk383E-IPhiAAABfhMmjmg",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
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
                "BwAAAEk383E-IPhiAAAAAAAAAAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
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
                "BwAAAEk383E-IPhi__6oggRgGAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
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
                "BwAAAEk383E-IPhiAADmaSK9hAA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
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
                "BwAAAEk383E-IPhiAAABfhMmioA",
                "JJSLNivCxv3hDTQtWd6qGUwGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "0XbnnsE9AoHbGpRryIzXGQ0w",
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
                "BwAAAAjKcgBsE3XEAAABfhMmioA",
                "JNY_frTR9GmCbhXgK4Y8W44GlT_5e6_NYGOZWULpmMG9IAlZlA",
                "NHaFg-5cCESqVrBaDs6o9DZ4",
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
                "BwAAANZEz_KPYz9jAAABfhMmioA",
                "JEyfZsJIp3UNyfWG-4SjKFIGlT_5e6_NYGOZWULpmMG9IAlZlA",
                "K3bwGazghpbQWy_dEU8UL4Ec",
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
                "BwAAACyv9sTdBEWNAAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bipyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "f8B2zv4qzO0Eq9YLPLUIlzRL8g",
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
                "BwAAABp_KncX55XzAAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bibzw1JBpU_-VsHjSz5HC1yy_swPEM1iGo",
                "f0F2VfGtg5ltThy1tF7mWC_PGQ",
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
                "BwAAAD-6gB-6kGvDAAABfhMmioA",
                "KJQKpjU9U63jhh-eNJ1f8bhaCD7uBpU_-SWGG0Uv9tZ1mLO2gi9rC1I",
                "f8t2FhZTCAA2SSVLPSknT8Nf_A",
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
                "BwAAAGEjL8LoBxUJAAABfhMmioA",
                "KB9H-tGrL_UzqMcqXcgBtzypyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "wMB2LHEQrqbi5fgySxCJAU5NhQ",
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
                "BwAAALNlofvJns7KAAABfhMmioA",
                "KGXATwN7ISd1_EycFRJ9h6qpyU08BpU_-ZJxnTYtoe9Lsg-QvzL-qOY",
                "esB2eX4haJwoe0Gz0Hxpr6mIcQ",
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
                "BwAAALQa3_B5oSRHAAABfhMmioA",
                "KI4kVxcCLIMM2_VQGD575d-tm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "C8t2xhEnokpbS-rpcaL9_JYgRQ",
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
                "BwAAAHJevaZ42qO0AAABfhMmioA",
                "KI4kVxcCLIMM2_VQGD575d8caJ3TBpU_-cLpg-VnCBnhYk33HZBle6E",
                "C-92gIC6326BrsHHzrPq6L6b7w",
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
                "BwAAAAykDD72vvVsAAABfhMmioA",
                "KI_1WxF60L0IczG5ftUCWdndcGtgBpU_-QfM2BaR0DMagIfw3TDu_mA",
                "tCp2ar0mjlGW0gXveQToNzewqg",
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
                "BwAAABKQqNW1IG5AAAABfhMmioA",
                "KLGFpvAV8QkWSmX54kXFMgitm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "nct2JZgFouaJFx9FPyphfxTTBw",
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
                "BwAAAFVne7eYMxr1AAABfhMmioA",
                "KLGFpvAV8QkWSmX54kXFMgjV8hFQBpU_-WG2MicRGWwJdBKWq2F4qy4",
                "ncx27Tdr-Fw8YsGkFZT_X2isdg",
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
                "BwAAAOfBxmKrLd6MAAABfhMmioA",
                "KJc4-5eN1uAlYuAknQQLUlxavn2sBpU_-UEXBjgaH1uYcbayrOhdgpc",
                "7RN22sJMTvFnwNcgZkI3oCIBvQ",
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
                "BwAAAFXFZfzCD9-tAAABfhMmioA",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5rZuNb-1ruHTTZhtsXRZpZRwWFoc",
                "E3bLGR5i_E-AjHaQj6NSgLqXqQ",
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
                "BwAAAHMdz2yGuWQ5AAABfhMmioA",
                "KFi_JDbvzWyAawmh8IEXedwGlT_5JgBZj9BSCms2_jgeFFhsmDlNFdM",
                "E3YA0Uj0mUMBIfe85M1eDvKJVg",
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
                "BwAAAKYKsByk_H1KAAABfhMmioA",
                "KKEQ2p3CkpMH61hNk_SuvI0GlT_53XBrYP5TPdmCR-vREPnt20e9f9w",
                "CHYqMfreOGDp369Up7wkbxfD7g",
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
                "BwAAACaAVo28B16ZAAABfhMmioA",
                "KGPAUhTjWOsRfDmYp3SUELatm41vBpU_-TUExUU_bL3Puq_EBgIaLac",
                "Uct2hEdPEZMCnGZ0_NM3so6Z3w",
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
                "BwAAAMkeTGox24cdAAABfhMmioA",
                "KGPAUhTjWOsRfDmYp3SUELYoK6qHBpU_-d8HkZFJ3aL2ZV1lgHAjT1g",
                "USF2EOdV0G1EpXE-GDtFWr7jHg",
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
                "BwAAAB3kQls40KoVAAABfhMmioA",
                "KA58oUMzXeX1V5rh51Ste0K5K9vPBpU_-Wn8JQplO-x3CgoslYO5Vks",
                "06J2CccOS3823wXT-Ntmaaz8nw",
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
                "BwAAABJYg92lKoxdAAABfhMmioA",
                "KNj6cLPRNEkqdjfOPIbg0wULrOlWBpU_-efWDsz6B6AnnwbZ7GeeocE",
                "3ft2E9wEcQ7qLhAhG-bgQxIC_w",
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
                "BwAAAEqgz7x99nlVAAABfhMmioA",
                "KNj6cLPRNEkqdjfOPIbg0wVhJ08TBpU_-bANzLhvKPczlle7Pq0z8Qw",
                "3RV2CkXc8fttjm4g1xCOxnlI7A",
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
                "BwAAAHTrphGgaaxEAAABfhMmioA",
                "KNDo3zGxO9HfN9XYJwKw2Z20h-WsBpU_-f4dSOLGSRlL1hoY2mgERuo",
                "4ch2pulC-JV1tGryTUWFGMsHdQ",
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
                "BwAAAEDsUEqRn2O2AAABfhMmioA",
                "LIe18i0rRU_Bt9vB82F46LaS9mrUkvZq1K_2Gi7UEFMhFwNXrLA_H8TLpUr4",
                "tIaGaQ90mXYrnllFi07m7A2pRMI",
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
    private final BlockLoaderTestRunner blockLoaderTestRunner;

    private static final int ROUTING_HASH = 7;

    public TsidExtractingIdFieldMapperTests(@Named("testCase") TestCase testCase) {
        this.testCase = testCase;
        this.blockLoaderTestRunner = new BlockLoaderTestRunner(
            new BlockLoaderTestCase.Params(false, randomFrom(MappedFieldType.FieldExtractPreference.values()))
        );
    }

    public void testExpectedIdWithRoutingPath() throws IOException {
        MapperService mapperService = mapperService(false);
        blockLoaderTestRunner.mapperService(mapperService);
        blockLoaderTestRunner.document(parse(mapperService, testCase.source));
        assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithRoutingPath));
        verifyIdFromBlockLoader(testCase.expectedIdWithRoutingPath);
    }

    public void testExpectedIdWithIndexDimensions() throws IOException {
        MapperService mapperService = mapperService(true);
        blockLoaderTestRunner.mapperService(mapperService);
        blockLoaderTestRunner.document(parse(mapperService, testCase.source));
        assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithIndexDimensions));
        verifyIdFromBlockLoader(testCase.expectedIdWithIndexDimensions);
    }

    public void testProvideExpectedIdWithRoutingPath() throws IOException {
        MapperService mapperService = mapperService(false);
        blockLoaderTestRunner.mapperService(mapperService);
        blockLoaderTestRunner.document(parse(testCase.expectedIdWithRoutingPath, mapperService, testCase.source));
        assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithRoutingPath));
        verifyIdFromBlockLoader(testCase.expectedIdWithRoutingPath);
    }

    public void testProvideExpectedIdWithIndexDimensions() throws IOException {
        MapperService mapperService = mapperService(true);
        blockLoaderTestRunner.mapperService(mapperService);
        blockLoaderTestRunner.document(parse(testCase.expectedIdWithIndexDimensions, mapperService, testCase.source));
        assertThat(blockLoaderTestRunner.document().id(), equalTo(testCase.expectedIdWithIndexDimensions));
        verifyIdFromBlockLoader(testCase.expectedIdWithIndexDimensions);
    }

    public void testEquivalentSourcesWithRoutingPath() throws IOException {
        MapperService mapperService = mapperService(false);
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

    public void testRoutingHashCompliantWithRoutingPath() throws IOException {
        byte[] bytes = Base64.getUrlDecoder().decode(testCase.expectedIdWithRoutingPath);
        assertEquals(ROUTING_HASH, ByteUtils.readIntLE(bytes, 0));
    }

    public void testRoutingHashCompliantWithIndexDimensions() throws IOException {
        byte[] bytes = Base64.getUrlDecoder().decode(testCase.expectedIdWithIndexDimensions);
        assertEquals(ROUTING_HASH, ByteUtils.readIntLE(bytes, 0));
    }

    private Settings indexSettings(IndexVersion version, boolean indexDimensions) {
        Settings.Builder builder = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 100))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey(), 100);
        if (indexDimensions) {
            builder.put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "r1,r2,k1,k2,L1,L2,i1,i2,s1,s2,b1,b2,ip1,ip2,o.*");
        } else {
            builder.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "r1,r2,o.r3");
        }
        return builder.build();
    }

    private MapperService mapperService(boolean indexDimensions) throws IOException {
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion();
        return createMapperService(indexSettings(version, indexDimensions), mapping(b -> {
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
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(false)),
            equalTo("a time series document")
        );
        ParsedDocument d = parse(mapperService(false), testCase.randomSource());
        IndexableField timestamp = d.rootDoc().getField(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(false, timestamp)),
            equalTo("a time series document at [" + testCase.expectedTimestamp + "]")
        );
        IndexableField tsid = d.rootDoc().getField(TimeSeriesIdFieldMapper.NAME);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(false, tsid)),
            equalTo("a time series document with tsid " + testCase.expectedTsidWithRoutingPath)
        );
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(false, tsid, timestamp)),
            equalTo("a time series document with tsid " + testCase.expectedTsidWithRoutingPath + " at [" + testCase.expectedTimestamp + "]")
        );
    }

    public void testSourceDescriptionWithIndexDimensions() throws IOException {
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(false)),
            equalTo("a time series document")
        );
        ParsedDocument d = parse(mapperService(false), testCase.randomSource());
        IndexableField timestamp = d.rootDoc().getField(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(false, timestamp)),
            equalTo("a time series document at [" + testCase.expectedTimestamp + "]")
        );
        IndexableField tsid = d.rootDoc().getField(TimeSeriesIdFieldMapper.NAME);
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(false, tsid)),
            equalTo("a time series document with tsid " + testCase.expectedTsidWithRoutingPath)
        );
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(documentParserContext(false, tsid, timestamp)),
            equalTo("a time series document with tsid " + testCase.expectedTsidWithRoutingPath + " at [" + testCase.expectedTimestamp + "]")
        );
    }

    private TestDocumentParserContext documentParserContext(boolean indexDimensions, IndexableField... fields) throws IOException {
        CheckedConsumer<XContentBuilder, IOException> source;
        // not using a random source here as the index.dimensions id is sensitive to how ips are represented (e.g. equivalent ipv4 vs ipv6)
        if (indexDimensions) {
            source = testCase.source;
        } else {
            source = testCase.randomSource();
        }
        TestDocumentParserContext ctx = new TestDocumentParserContext(
            mapperService(indexDimensions).mappingLookup(),
            source(null, source, null)
        );
        for (IndexableField f : fields) {
            ctx.doc().add(f);
        }
        return ctx;
    }

    public void testParsedDescriptionWithRoutingPath() throws IOException {
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(parse(mapperService(false), testCase.randomSource())),
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
        assertThat(
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(parse(mapperService(true), testCase.source)),
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
