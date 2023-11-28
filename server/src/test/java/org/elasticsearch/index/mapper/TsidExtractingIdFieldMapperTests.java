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
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.name.Named;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
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
                "XsFI2S5yHCiDlfM-AAABfhMmioA",
                "lIs2K8LG_eENNC1Z3qoZTAaVP_l7r81gY5lZQumYwb0gCVmU",
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
                "XsFI2S5yHCiDlfM-AAABfhMmjmg",
                "lIs2K8LG_eENNC1Z3qoZTAaVP_l7r81gY5lZQumYwb0gCVmU",
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
                "XsFI2S5yHCiDlfM-AAAAAAAAAAA",
                "lIs2K8LG_eENNC1Z3qoZTAaVP_l7r81gY5lZQumYwb0gCVmU",
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
                "XsFI2S5yHCiDlfM-__6oggRgGAA",
                "lIs2K8LG_eENNC1Z3qoZTAaVP_l7r81gY5lZQumYwb0gCVmU",
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
                "XsFI2S5yHCiDlfM-AADmaSK9hAA",
                "lIs2K8LG_eENNC1Z3qoZTAaVP_l7r81gY5lZQumYwb0gCVmU",
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
                "XsFI2S5yHCiDlfM-AAABfhMmioA",
                "lIs2K8LG_eENNC1Z3qoZTAaVP_l7r81gY5lZQumYwb0gCVmU",
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
                "1y-Uzfo-1P_bJDK2AAABfhMmioA",
                "1j9-tNH0aYJuFeArhjxbjgaVP_l7r81gY5lZQumYwb0gCVmU",
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
                "zh4dcW73kG72NCdnAAABfhMmioA",
                "TJ9mwkindQ3J9Yb7hKMoUgaVP_l7r81gY5lZQumYwb0gCVmU",
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
                "XsFI2SC_bHbBSYpBAAABfhMmioA",
                "lAqmNT1TreOGH540nV_xuKnJTTwGlT_5knGdNi2h70uyD5C_Mv6o5g",
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
                "XsFI2XxAh7vYQakJAAABfhMmioA",
                "lAqmNT1TreOGH540nV_xuJvPDUkGlT_5WweNLPkcLXLL-zA8QzWIag",
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
                "XsFI2fKoX0rSe3_6AAABfhMmioA",
                "lAqmNT1TreOGH540nV_xuFoIPu4GlT_5JYYbRS_21nWYs7aCL2sLUg",
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
                "XsFI2TfQGYgMHGkBAAABfhMmioA",
                "H0f60asv9TOoxypdyAG3PKnJTTwGlT_5knGdNi2h70uyD5C_Mv6o5g",
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
                "XsFI2e1YPwS9YAkjAAABfhMmioA",
                "ZcBPA3shJ3X8TJwVEn2HqqnJTTwGlT_5knGdNi2h70uyD5C_Mv6o5g",
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
                "zh4dcbjCi5GWqNYzAAABfhMmioA",
                "lphlVnPymV-QS-88GmLURanJTTwGlT_5knGdNi2h70uyD5C_Mv6o5g",
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
                "XsFI2T10ZbvtTyt0AAABfhMmioA",
                "jiRXFwIsgwzb9VAYPnvl362bjW8GlT_5NQTFRT9svc-6r8QGAhotpw",
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
                "XsFI2bgn8k5477GSAAABfhMmioA",
                "jiRXFwIsgwzb9VAYPnvl3xxondMGlT_5wumD5WcIGeFiTfcdkGV7oQ",
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
                "XsFI2S6u8qCv1UL0AAABfhMmioA",
                "j_VbEXrQvQhzMbl-1QJZ2d1wa2AGlT_5B8zYFpHQMxqAh_DdMO7-YA",
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
                "zh4dcSavha_vxz6GAAABfhMmioA",
                "3hrpDMqHNzefDM0u5mRXnXHFM5UGlT_56tRTWpJbh4nSQ_RGYAH1MQ",
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
                "XsFI2fT0A5_AuFzKAAABfhMmioA",
                "sYWm8BXxCRZKZfniRcUyCK2bjW8GlT_5NQTFRT9svc-6r8QGAhotpw",
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
                "XsFI2TUTTqNc5uyAAAABfhMmioA",
                "sYWm8BXxCRZKZfniRcUyCNXyEVAGlT_5YbYyJxEZbAl0EparYXirLg",
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
                "XsFI2bnAeI4UglLNAAABfhMmioA",
                "lzj7l43W4CVi4CSdBAtSXFq-fawGlT_5QRcGOBofW5hxtrKs6F2Clw",
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
                "zh4dcZVYlwGWElFBAAABfhMmioA",
                "qqfM80F7w5uSxKjys1ogU9k8JwEGlT_5GF39TomfamNDPbOmYG4lvg",
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
                "XsFI2e5Vf7PFkb9LAAABfhMmioA",
                "WL8kNu_NbIBrCaHwgRd53AaVP_mtm41v7Wu4dNNmG2xdFmllHBYWhw",
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
                "XsFI2dHghN3s9f1cAAABfhMmioA",
                "WL8kNu_NbIBrCaHwgRd53AaVP_kmAFmP0FIKazb-OB4UWGyYOU0V0w",
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
                "XsFI2eU52c3ww-79AAABfhMmioA",
                "oRDancKSkwfrWE2T9K68jQaVP_ndcGtg_lM92YJH69EQ-e3bR71_3A",
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
                "zh4dcYaxM2mQy-WyAAABfhMmioA",
                "pUyhP_4ZL3l-8gG1Htcr2gaVP_kOZ734fex6fRZnuV4mkKPv69dq7g",
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
                "XsFI2f86PdSxFW_BAAABfhMmioA",
                "Y8BSFONY6xF8OZindJQQtq2bjW8GlT_5NQTFRT9svc-6r8QGAhotpw",
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
                "XsFI2TUkLTLOLx3OAAABfhMmioA",
                "Y8BSFONY6xF8OZindJQQtigrqocGlT_53weRkUndovZlXWWAcCNPWA",
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
                "XsFI2bCS8HbUffZ-AAABfhMmioA",
                "DnyhQzNd5fVXmuHnVK17Qrkr288GlT_5afwlCmU77HcKCiyVg7lWSw",
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
                "zh4dcYE4JNpL4t_4AAABfhMmioA",
                "jBkf5K0kGhuOT20JX7Q6CMqa7AYGlT_5J35J2egfext8ouCx75uuIQ",
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
                "XsFI2UeTkcMPSivHAAABfhMmioA",
                "2Ppws9E0SSp2N848huDTBQus6VYGlT_559YOzPoHoCefBtnsZ56hwQ",
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
                "XsFI2eovUFDajzh-AAABfhMmioA",
                "2Ppws9E0SSp2N848huDTBWEnTxMGlT_5sA3MuG8o9zOWV7s-rTPxDA",
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
                "XsFI2coO800yZDtEAAABfhMmioA",
                "0OjfMbE70d831dgnArDZnbSH5awGlT_5_h1I4sZJGUvWGhjaaARG6g",
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
                "zh4dcVcvZNXO8eMqAAABfhMmioA",
                "tcNwEFYkCOAm-NJ0X8QnCtoBBTMGlT_5trodmwjIqcZxuZzcpEuWYg",
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
                "WZKJR55_Xxk3QYCwAAABfhMmioA",
                "h7XyLStFT8G328HzYXjotpL2atSS9mrUr_YaLtQQUyEXA1essD8fxMulSvg",
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

    public TsidExtractingIdFieldMapperTests(@Named("testCase") TestCase testCase) {
        this.testCase = testCase;
    }

    public void testExpectedId() throws IOException {
        assertThat(parse(null, mapperService(), testCase.source).id(), equalTo(testCase.expectedId));
    }

    public void testProvideExpectedId() throws IOException {
        assertThat(parse(testCase.expectedId, mapperService(), testCase.source).id(), equalTo(testCase.expectedId));
    }

    public void testProvideWrongId() {
        String wrongId = testCase.expectedId + "wrong";
        Exception e = expectThrows(DocumentParsingException.class, () -> parse(wrongId, mapperService(), testCase.source));
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
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());
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
        ParsedDocument d = parse(null, mapperService(), testCase.randomSource());
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
            TsidExtractingIdFieldMapper.INSTANCE.documentDescription(parse(null, mapperService(), testCase.randomSource())),
            equalTo("[" + testCase.expectedId + "][" + testCase.expectedTsid + "@" + testCase.expectedTimestamp + "]")
        );
    }
}
