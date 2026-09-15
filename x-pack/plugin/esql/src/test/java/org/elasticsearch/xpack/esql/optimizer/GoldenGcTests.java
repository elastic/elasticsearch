/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/** The source rewrites {@code -Dgolden.gc.fix} performs, on the shapes the golden suites actually use. */
public class GoldenGcTests extends ESTestCase {

    public void testRemovesChainedCallsAndUnusedConstant() {
        String source = """
            public class FooGoldenTests extends GoldenTestCase {
                private static final String DIMENSION_VALUES = "dimension_values";
                private static final String PACK_DIMS_AGG = "pack_dims_agg";

                public void testOne() {
                    builder("FROM a").expectationChangesAt(DIMENSION_VALUES)
                        .expectationChangesAt(PACK_DIMS_AGG)
                        .run();
                }

                public void testTwo() {
                    builder("FROM b")
                        .expectationChangesAt(DIMENSION_VALUES) // the plan changes shape here
                        .run();
                }
            }
            """;
        String expected = """
            public class FooGoldenTests extends GoldenTestCase {
                private static final String PACK_DIMS_AGG = "pack_dims_agg";

                public void testOne() {
                    builder("FROM a")
                        .expectationChangesAt(PACK_DIMS_AGG)
                        .run();
                }

                public void testTwo() {
                    builder("FROM b")
                        .run();
                }
            }
            """;
        assertThat(GoldenGc.removeDeclarations(source, "dimension_values"), equalTo(expected));
    }

    public void testKeepsConstantStillReferencedElsewhere() {
        String source = """
            private static final String DIMENSION_VALUES = "dimension_values";
            void test() {
                builder("FROM a").since(DIMENSION_VALUES).run();
                assumeTrue(DIMENSION_VALUES, true);
            }
            """;
        String expected = """
            private static final String DIMENSION_VALUES = "dimension_values";
            void test() {
                builder("FROM a").run();
                assumeTrue(DIMENSION_VALUES, true);
            }
            """;
        assertThat(GoldenGc.removeDeclarations(source, "dimension_values"), equalTo(expected));
    }

    public void testRemovesLiteralFormAndLeavesOtherVersionsAlone() {
        String source = """
            builder("FROM a").since("dimension_values").expectationChangesAt("pack_dims_agg").run();
            """;
        String expected = """
            builder("FROM a").expectationChangesAt("pack_dims_agg").run();
            """;
        assertThat(GoldenGc.removeDeclarations(source, "dimension_values"), equalTo(expected));
    }

    public void testSecondPassChangesNothing() throws IOException {
        Path file = createTempFile();
        Files.writeString(file, """
            private static final String X = "x_version";
            void test() { builder("FROM a").expectationChangesAt(X).run(); }
            """);
        assertTrue(GoldenGc.removeDeclarations(file, "x_version"));
        assertFalse(GoldenGc.removeDeclarations(file, "x_version"));
        assertThat(Files.readString(file), equalTo("void test() { builder(\"FROM a\").run(); }\n"));
    }

    public void testRemovesConstantFormSinceByVersionName() {
        String source = """
            public class FooGoldenTests extends GoldenTestCase {
                public void testOne() {
                    nullify(query).since(DimensionValues.DIMENSION_VALUES_VERSION);
                    load(query).since(DimensionValues.DIMENSION_VALUES_VERSION);
                }

                public void testTwo() {
                    builder("FROM b").since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).run();
                }

                public void testThree() {
                    builder("FROM c").since(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION).run();
                }

                public void testFour() {
                    builder("FROM d").since(CompactMultiTypeEsField.CompactMultiTypeEsField).run();
                }
            }
            """;
        String afterDimensionValues = source.replace(".since(DimensionValues.DIMENSION_VALUES_VERSION)", "");
        assertThat(GoldenGc.removeDeclarations(source, "dimension_values"), equalTo(afterDimensionValues));
        assertThat(
            GoldenGc.removeDeclarations(source, "esql_mv_single_value_or_null"),
            equalTo(source.replace(".since(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION)", ""))
        );
        assertThat(
            GoldenGc.removeDeclarations(source, "compact_multi_type_es_field"),
            equalTo(source.replace(".since(CompactMultiTypeEsField.CompactMultiTypeEsField)", ""))
        );
        assertThat(
            GoldenGc.removeDeclarations(source, "esql_sum_long_overflow_fix").contains("ESQL_SUM_LONG_OVERFLOW_FIX"),
            equalTo(false)
        );
    }

    public void testLeavesLookalikeConstantAloneButReportsIt() {
        String source = """
            builder("FROM a").since(TimeSeriesCollapse.TS_COLLAPSE_V2).run();
            """;
        assertThat(GoldenGc.removeDeclarations(source, "ts_collapse"), equalTo(source));
        assertTrue(GoldenGc.mentions(source, "ts_collapse"));
        assertFalse(GoldenGc.mentions(source, "pack_dims_agg"));
    }

    public void testDeletesDirectoriesOfThatNameAcrossTheSuite() throws IOException {
        Path root = createTempDir();
        Path dead1 = root.resolve("testFoo").resolve("before_x_version");
        Path dead2 = root.resolve("testBar").resolve("nested").resolve("before_x_version");
        Path alive = root.resolve("testFoo").resolve("before_y_version");
        for (Path dir : List.of(dead1, dead2, alive)) {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("analysis.expected"), "plan");
        }
        GoldenGc.deleteDirectoriesNamed(root, "before_x_version");
        assertFalse(Files.exists(dead1));
        assertFalse(Files.exists(dead2));
        assertTrue(Files.exists(alive.resolve("analysis.expected")));
        GoldenGc.deleteDirectoriesNamed(root, "before_x_version");
        GoldenGc.deleteDirectoriesNamed(root.resolve("missing"), "before_x_version");
    }
}
