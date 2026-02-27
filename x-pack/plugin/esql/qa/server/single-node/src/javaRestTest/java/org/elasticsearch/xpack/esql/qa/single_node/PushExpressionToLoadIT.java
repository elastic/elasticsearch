/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.entityToMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.commonProfile;
import static org.elasticsearch.xpack.esql.qa.single_node.RestEsqlIT.fixTypesOnProfile;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for pushing expressions into field loading.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PushExpressionToLoadIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    public void testLengthToKeyword() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("keyword"),
            b -> b.field("test", value),
            "| EVAL test = LENGTH(test)",
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:Utf8CodePointsFromOrds.Singleton", 1)
        );
    }

    /**
     * We don't support fusing {@code LENGTH} into loading {@code wildcard} fields because
     * we haven't written support for fusing functions to loading from its source format.
     * We haven't done that because {@code wildcard} fields aren't super common.
     */
    public void testLengthNotPushedToWildcard() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("wildcard"),
            b -> b.field("test", value),
            "| EVAL test = LENGTH(test)",
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:BlockDocValuesReader.BytesCustom", 1)
        );
    }

    /**
     * We don't support fusing {@code LENGTH} into loading {@code text} fields because
     * we haven't written support for fusing functions to loading from {@code _source}.
     * Usually folks that want to go superfast will use doc values. But those aren't
     * even available for {@code text} fields.
     */
    public void testLengthNotPushedToText() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("text"),
            b -> b.field("test", value),
            "| EVAL test = LENGTH(test)",
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:null", 1)
                .entry("stored_fields[requires_source:true, fields:0, sequential: false]", 1)
                .entry("test:row_stride:BlockSourceReader.Bytes", 1)
        );
    }

    public void testMvMinToKeyword() throws IOException {
        String min = "a".repeat(between(1, 256));
        String max = "b".repeat(between(1, 256));
        test(
            justType("keyword"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(min),
            matchesMap().entry("test:column_at_a_time:MvMinBytesRefsFromOrds.SortedSet", 1)
        );
    }

    public void testMvMinToIp() throws IOException {
        String min = "192.168.0." + between(0, 255);
        String max = "192.168.3." + between(0, 255);
        test(
            justType("ip"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(min),
            matchesMap().entry("test:column_at_a_time:MvMinBytesRefsFromOrds.SortedSet", 1)
        );
    }

    public void testMvMinToHalfFloat() throws IOException {
        double min = randomDouble();
        double max = 1 + randomDouble();
        test(
            justType("half_float"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(closeTo(min, .1)),
            matchesMap().entry("test:column_at_a_time:MvMinDoublesFromDocValues.Sorted", 1)
        );
    }

    public void testMvMinToFloat() throws IOException {
        double min = randomDouble();
        double max = 1 + randomDouble();
        test(
            justType("float"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(closeTo(min, .1)),
            matchesMap().entry("test:column_at_a_time:MvMinDoublesFromDocValues.Sorted", 1)
        );
    }

    public void testMvMinToDouble() throws IOException {
        double min = randomDouble();
        double max = 1 + randomDouble();
        test(
            justType("double"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(min),
            matchesMap().entry("test:column_at_a_time:MvMinDoublesFromDocValues.Sorted", 1)
        );
    }

    public void testMvMinToByte() throws IOException {
        int min = between(Byte.MIN_VALUE, Byte.MAX_VALUE - 10);
        int max = between(min + 1, Byte.MAX_VALUE);
        test(
            justType("byte"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(min),
            matchesMap().entry("test:column_at_a_time:MvMinIntsFromDocValues.Sorted", 1)
        );
    }

    public void testMvMinToShort() throws IOException {
        int min = between(Short.MIN_VALUE, Short.MAX_VALUE - 10);
        int max = between(min + 1, Short.MAX_VALUE);
        test(
            justType("short"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(min),
            matchesMap().entry("test:column_at_a_time:MvMinIntsFromDocValues.Sorted", 1)
        );
    }

    public void testMvMinToInt() throws IOException {
        int min = between(Integer.MIN_VALUE, Integer.MAX_VALUE - 10);
        int max = between(min + 1, Integer.MAX_VALUE);
        test(
            justType("integer"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(min),
            matchesMap().entry("test:column_at_a_time:MvMinIntsFromDocValues.Sorted", 1)
        );
    }

    public void testMvMinToLong() throws IOException {
        long min = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - 10);
        long max = randomLongBetween(min + 1, Long.MAX_VALUE);
        test(
            justType("long"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MIN(test)",
            matchesList().item(min),
            matchesMap().entry("test:column_at_a_time:MvMinLongsFromDocValues.Sorted", 1)
        );
    }

    public void testMvMaxToKeyword() throws IOException {
        String min = "a".repeat(between(1, 256));
        String max = "b".repeat(between(1, 256));
        test(
            justType("keyword"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(max),
            matchesMap().entry("test:column_at_a_time:MvMaxBytesRefsFromOrds.SortedSet", 1)
        );
    }

    public void testMvMaxToIp() throws IOException {
        String min = "192.168.0." + between(0, 255);
        String max = "192.168.3." + between(0, 255);
        test(
            justType("ip"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(max),
            matchesMap().entry("test:column_at_a_time:MvMaxBytesRefsFromOrds.SortedSet", 1)
        );
    }

    public void testMvMaxToByte() throws IOException {
        int min = between(Byte.MIN_VALUE, Byte.MAX_VALUE - 10);
        int max = between(min + 1, Byte.MAX_VALUE);
        test(
            justType("byte"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(max),
            matchesMap().entry("test:column_at_a_time:MvMaxIntsFromDocValues.Sorted", 1)
        );
    }

    public void testMvMaxToShort() throws IOException {
        int min = between(Short.MIN_VALUE, Short.MAX_VALUE - 10);
        int max = between(min + 1, Short.MAX_VALUE);
        test(
            justType("short"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(max),
            matchesMap().entry("test:column_at_a_time:MvMaxIntsFromDocValues.Sorted", 1)
        );
    }

    public void testMvMaxToInt() throws IOException {
        int min = between(Integer.MIN_VALUE, Integer.MAX_VALUE - 10);
        int max = between(min + 1, Integer.MAX_VALUE);
        test(
            justType("integer"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(max),
            matchesMap().entry("test:column_at_a_time:MvMaxIntsFromDocValues.Sorted", 1)
        );
    }

    public void testMvMaxToLong() throws IOException {
        long min = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - 10);
        long max = randomLongBetween(min + 1, Long.MAX_VALUE);
        test(
            justType("long"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(max),
            matchesMap().entry("test:column_at_a_time:MvMaxLongsFromDocValues.Sorted", 1)
        );
    }

    public void testMvMaxToHalfFloat() throws IOException {
        double min = randomDouble();
        double max = 1 + randomDouble();
        test(
            justType("half_float"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(closeTo(max, .1)),
            matchesMap().entry("test:column_at_a_time:MvMaxDoublesFromDocValues.Sorted", 1)
        );
    }

    public void testMvMaxToFloat() throws IOException {
        double min = randomDouble();
        double max = 1 + randomDouble();
        test(
            justType("float"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(closeTo(max, .1)),
            matchesMap().entry("test:column_at_a_time:MvMaxDoublesFromDocValues.Sorted", 1)
        );
    }

    public void testMvMaxToDouble() throws IOException {
        double min = randomDouble();
        double max = 1 + randomDouble();
        test(
            justType("double"),
            b -> b.startArray("test").value(min).value(max).endArray(),
            "| EVAL test = MV_MAX(test)",
            matchesList().item(max),
            matchesMap().entry("test:column_at_a_time:MvMaxDoublesFromDocValues.Sorted", 1)
        );
    }

    public void testVCosine() throws IOException {
        test(
            justType("dense_vector"),
            b -> b.startArray("test").value(128).value(128).value(0).endArray(),
            "| EVAL test = V_COSINE(test, [0, 255, 255])",
            matchesList().item(0.5),
            matchesMap().entry("test:column_at_a_time:FloatDenseVectorFromDocValues.Normalized.V_COSINE", 1)
        );
    }

    public void testVHammingToByte() throws IOException {
        test(
            b -> b.startObject("test").field("type", "dense_vector").field("element_type", "byte").endObject(),
            b -> b.startArray("test").value(100).value(100).value(0).endArray(),
            "| EVAL test = V_HAMMING(test, [0, 100, 100])",
            matchesList().item(6.0),
            matchesMap().entry("test:column_at_a_time:ByteDenseVectorFromDocValues.V_HAMMING", 1)
        );
    }

    public void testVHammingToBit() throws IOException {
        test(
            b -> b.startObject("test").field("type", "dense_vector").field("element_type", "bit").endObject(),
            b -> b.startArray("test").value(100).value(100).value(0).endArray(),
            "| EVAL test = V_HAMMING(test, [0, 100, 100])",
            matchesList().item(6.0),
            matchesMap().entry("test:column_at_a_time:BitDenseVectorFromDocValues.V_HAMMING", 1)
        );
    }

    //
    // Tests without STATS at the end - check that node_reduce phase works correctly
    //
    public void testLengthPushedWithoutTopN() throws IOException {
        String textValue = "v".repeat(between(0, 256));
        test(
            b -> b.startObject("test").field("type", "keyword").endObject(),
            b -> b.field("test", textValue),
            """
                FROM test
                | EVAL fieldLength = LENGTH(test)
                | LIMIT 10
                | KEEP test, fieldLength
                """,
            matchesList().item(textValue).item(textValue.length()),
            matchesList().item(matchesMap().entry("name", "test").entry("type", any(String.class)))
                .item(matchesMap().entry("name", "fieldLength").entry("type", any(String.class))),
            Map.of(
                "data",
                List.of(
                    // Pushed down function
                    matchesMap().entry("test:column_at_a_time:BytesRefsFromOrds.Singleton", 1),
                    // Field
                    matchesMap().entry("test:row_stride:BytesRefsFromOrds.Singleton", 1)
                )
            ),
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("ProjectOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    public void testLengthPushedWithTopN() throws IOException {
        String textValue = "v".repeat(between(0, 256));
        Integer orderingValue = randomInt();
        test(b -> {
            b.startObject("test").field("type", "keyword").endObject();
            b.startObject("ordering").field("type", "integer").endObject();
        },
            b -> b.field("test", textValue).field("ordering", orderingValue),
            """
                FROM test
                | EVAL fieldLength = LENGTH(test)
                | SORT ordering DESC
                | LIMIT 10
                | KEEP test
                """,
            matchesList().item(textValue),
            matchesList().item(matchesMap().entry("name", "test").entry("type", any(String.class))),
            Map.of(
                "data",
                Build.current().isSnapshot()
                    ? List.of(matchesMap().entry("ordering:column_at_a_time:IntsFromDocValues.Singleton", 1))
                    : List.of(
                        matchesMap().entry("ordering:column_at_a_time:IntsFromDocValues.Singleton", 1)
                            .entry("test:column_at_a_time:BytesRefsFromOrds.Singleton", 1)
                    ),
                "node_reduce",
                Build.current().isSnapshot()
                    ? List.of(
                        // Pushed down function
                        matchesMap().entry("test:column_at_a_time:Utf8CodePointsFromOrds.Singleton", 1),
                        // Field
                        matchesMap().entry("test:column_at_a_time:BytesRefsFromOrds.Singleton", 1)
                    )
                    : List.of(matchesMap().entry("test:row_stride:BytesRefsFromOrds.Singleton", 1))
            ),
            sig -> assertMap(
                sig,
                matchesList().item("LuceneTopNSourceOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("ProjectOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    public void testLengthPushedWithTopNAsOrder() throws IOException {
        String textValue = "v".repeat(between(0, 256));
        test(
            b -> b.startObject("test").field("type", "keyword").endObject(),
            b -> b.field("test", textValue),
            """
                FROM test
                | EVAL fieldLength = LENGTH(test)
                | SORT fieldLength DESC
                | LIMIT 10
                | KEEP test, fieldLength
                """,
            matchesList().item(textValue).item(textValue.length()),
            matchesList().item(matchesMap().entry("name", "test").entry("type", any(String.class)))
                .item(matchesMap().entry("name", "fieldLength").entry("type", any(String.class))),
            Map.of(
                "data",
                List.of(
                    // Pushed down function
                    matchesMap().entry("test:column_at_a_time:Utf8CodePointsFromOrds.Singleton", 1),
                    // TODO It should not load the field value on the data node, but just on the node_reduce phase
                    matchesMap().entry("test:column_at_a_time:BytesRefsFromOrds.Singleton", 1)
                )
            ),
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("EvalOperator")
                    .item("TopNOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("ProjectOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    //
    // Tests for more complex shapes.
    //

    /**
     * Tests {@code LENGTH} on a field that comes from a {@code LOOKUP JOIN}.
     */
    public void testLengthNotPushedToLookupJoinKeyword() throws IOException {
        initLookupIndex();
        test(
            b -> b.startObject("main_matching").field("type", "keyword").endObject(),
            b -> b.field("main_matching", "lookup"),
            """
                | LOOKUP JOIN lookup ON matching == main_matching
                | EVAL test = LENGTH(test)
                """,
            matchesList().item(1),
            matchesMap().entry("main_matching:column_at_a_time:BytesRefsFromOrds.Singleton", 1),
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator") // the real work is here, checkOperatorProfile checks the status
                    .item("LookupOperator")
                    .item("EvalOperator") // this one just renames the field
                    .item("AggregationOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    /**
     * Tests {@code LENGTH} on a field that comes from a {@code LOOKUP JOIN} with
     * the added complexity that the field also exists in the index, but we're not
     * querying it.
     */
    public void testLengthNotPushedToLookupJoinKeywordSameName() throws IOException {
        String value = "v".repeat(between(0, 256));
        initLookupIndex();
        test(b -> {
            b.startObject("test").field("type", "keyword").endObject();
            b.startObject("main_matching").field("type", "keyword").endObject();
        },
            b -> b.field("test", value).field("main_matching", "lookup"),
            """
                | DROP test
                | LOOKUP JOIN lookup ON matching == main_matching
                | EVAL test = LENGTH(test)
                """,
            matchesList().item(1),
            matchesMap().entry("main_matching:column_at_a_time:BytesRefsFromOrds.Singleton", 1),
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator") // the real work is here, checkOperatorProfile checks the status
                    .item("LookupOperator")
                    .item("EvalOperator") // this one just renames the field
                    .item("AggregationOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    /**
     * Tests {@code LENGTH} on a field that comes from a {@code LOOKUP JOIN}.
     */
    public void testLengthPushedInsideInlineStats() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("keyword"),
            b -> b.field("test", value),
            """
                | INLINE STATS max_length = MAX(LENGTH(test))
                | EVAL test = LENGTH(test)
                | WHERE test == max_length
                """,
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:Utf8CodePointsFromOrds.Singleton", 1),
            sig -> {
                // There are two data node plans, one for each phase.
                if (sig.contains("FilterOperator")) {
                    assertMap(
                        sig,
                        matchesList().item("LuceneSourceOperator")
                            .item("ValuesSourceReaderOperator") // the real work is here, checkOperatorProfile checks the status
                            .item("FilterOperator")
                            .item("EvalOperator") // this one just renames the field
                            .item("AggregationOperator")
                            .item("ExchangeSinkOperator")
                    );
                } else {
                    assertMap(
                        sig,
                        matchesList().item("LuceneSourceOperator")
                            .item("ValuesSourceReaderOperator") // the real work is here, checkOperatorProfile checks the status
                            .item("EvalOperator") // this one just renames the field
                            .item("AggregationOperator")
                            .item("ExchangeSinkOperator")
                    );
                }
            }
        );
    }

    /**
     * Tests {@code LENGTH} on a field that comes from a {@code LOOKUP JOIN}.
     */
    public void testLengthNotPushedToInlineStatsResults() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(justType("keyword"), b -> b.field("test", value), """
            | INLINE STATS test2 = VALUES(test)
            | EVAL test = LENGTH(test2)
            """, matchesList().item(value.length()), matchesMap().entry("test:column_at_a_time:BytesRefsFromOrds.Singleton", 1), sig -> {
            // There are two data node plans, one for each phase.
            if (sig.contains("EvalOperator")) {
                assertMap(
                    sig,
                    matchesList().item("LuceneSourceOperator")
                        .item("EvalOperator") // The second phase of the INLINE STATS
                        .item("AggregationOperator")
                        .item("ExchangeSinkOperator")
                );
            } else {
                assertMap(
                    sig,
                    matchesList().item("LuceneSourceOperator")
                        .item("ValuesSourceReaderOperator")
                        .item("AggregationOperator")
                        .item("ExchangeSinkOperator")
                );
            }
        });
    }

    /**
     * Tests {@code LENGTH} on a field that comes from a {@code LOOKUP JOIN}.
     */
    public void testLengthNotPushedToGroupedInlineStatsResults() throws IOException {
        String value = "v".repeat(between(0, 256));
        CheckedConsumer<XContentBuilder, IOException> mapping = b -> {
            b.startObject("test").field("type", "keyword").endObject();
            b.startObject("group").field("type", "keyword").endObject();
        };
        test(mapping, b -> b.field("test", value).field("group", "g"), """
            | INLINE STATS test2 = VALUES(test) BY group
            | EVAL test = LENGTH(test2)
            """, matchesList().item(value.length()), matchesMap().extraOk(), sig -> {
            // There are two data node plans, one for each phase.
            if (sig.contains("EvalOperator")) {
                assertMap(
                    sig,
                    matchesList().item("LuceneSourceOperator")
                        .item("ValuesSourceReaderOperator")
                        .item("RowInTableLookup")
                        .item("ColumnLoad")
                        .item("ProjectOperator")
                        .item("EvalOperator")
                        .item("AggregationOperator")
                        .item("ExchangeSinkOperator")
                );
            } else {
                assertMap(
                    sig,
                    matchesList().item("LuceneSourceOperator")
                        .item("ValuesSourceReaderOperator")
                        .item("HashAggregationOperator")
                        .item("ExchangeSinkOperator")
                );
            }
        });
    }

    /**
     * LENGTH not pushed when on a fork branch.
     */
    public void testLengthNotPushedToFork() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("keyword"),
            b -> b.field("test", value),
            """
                | FORK
                    (EVAL test = LENGTH(test) + 1)
                    (EVAL test = LENGTH(test) + 2)
                """,
            matchesList().item(List.of(value.length() + 1, value.length() + 2)),
            matchesMap().entry("test:column_at_a_time:BytesRefsFromOrds.Singleton", 1),
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("ProjectOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    public void testLengthNotPushedBeforeFork() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("keyword"),
            b -> b.field("test", value),
            """
                | EVAL test = LENGTH(test)
                | FORK
                    (EVAL j = 1)
                    (EVAL j = 2)
                """,
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:BytesRefsFromOrds.Singleton", 1),
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("ProjectOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    public void testLengthNotPushedAfterFork() throws IOException {
        String value = "v".repeat(between(0, 256));
        test(
            justType("keyword"),
            b -> b.field("test", value),
            """
                | FORK
                    (EVAL j = 1)
                    (EVAL j = 2)
                | EVAL test = LENGTH(test)
                """,
            matchesList().item(value.length()),
            matchesMap().entry("test:column_at_a_time:BytesRefsFromOrds.Singleton", 1),
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator")
                    .item("ProjectOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    private void test(
        CheckedConsumer<XContentBuilder, IOException> mapping,
        CheckedConsumer<XContentBuilder, IOException> doc,
        String eval,
        Matcher<?> expectedValue,
        MapMatcher expectedLoaders
    ) throws IOException {
        test(
            mapping,
            doc,
            eval,
            expectedValue,
            expectedLoaders,
            sig -> assertMap(
                sig,
                matchesList().item("LuceneSourceOperator")
                    .item("ValuesSourceReaderOperator") // the real work is here, checkOperatorProfile checks the status
                    .item("EvalOperator") // this one just renames the field
                    .item("AggregationOperator")
                    .item("ExchangeSinkOperator")
            )
        );
    }

    private void test(
        CheckedConsumer<XContentBuilder, IOException> mapping,
        CheckedConsumer<XContentBuilder, IOException> doc,
        String eval,
        Matcher<?> expectedValue,
        MapMatcher expectedLoaders,
        Consumer<List<String>> assertDataNodeSig
    ) throws IOException {

        test(
            mapping,
            doc,
            """
                FROM test
                """ + eval + """
                | STATS test = MV_SORT(VALUES(test))
                """,
            expectedValue,
            matchesList().item(matchesMap().entry("name", "test").entry("type", any(String.class))),
            Map.of("data", List.of(expectedLoaders)),
            assertDataNodeSig
        );
    }

    private void test(
        CheckedConsumer<XContentBuilder, IOException> mapping,
        CheckedConsumer<XContentBuilder, IOException> doc,
        String query,
        Matcher<?> expectedValue,
        Matcher<?> columnMatcher,
        Map<String, List<MapMatcher>> expectedLoadersPerDriver,
        Consumer<List<String>> assertDataNodeSig
    ) throws IOException {
        indexValue(mapping, doc);
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(query);

        builder.profile(true);
        Map<String, Object> result = runEsql(builder, new AssertWarnings.NoWarnings(), profileLogger, RestEsqlTestCase.Mode.SYNC);

        assertResultMap(
            result,
            getResultMatcher(result).entry(
                "profile",
                matchesMap() //
                    .entry("drivers", instanceOf(List.class))
                    .entry("plans", instanceOf(List.class))
                    .entry("planning", matchesMap().extraOk())
                    .entry("parsing", matchesMap().extraOk())
                    .entry("preanalysis", matchesMap().extraOk())
                    .entry("dependency_resolution", matchesMap().extraOk())
                    .entry("analysis", matchesMap().extraOk())
                    .entry("query", matchesMap().extraOk())
                    .entry("field_caps_calls", instanceOf(Integer.class))
                    .entry("minimumTransportVersion", instanceOf(Integer.class))
            ),
            columnMatcher,
            matchesList().item(expectedValue)
        );
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> profiles = (List<Map<String, Object>>) ((Map<String, Object>) result.get("profile")).get("drivers");
        for (Map<String, Object> p : profiles) {
            fixTypesOnProfile(p);
            assertThat(p, commonProfile());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> operators = (List<Map<String, Object>>) p.get("operators");

            String driverDescription = (String) p.get("description");
            List<MapMatcher> mapMatcher = expectedLoadersPerDriver.get(driverDescription);
            List<String> sig = checkOperatorProfile(driverDescription, operators, mapMatcher);
            switch (driverDescription) {
                case "data" -> {
                    logger.info("data {}", sig);
                    assertDataNodeSig.accept(sig);
                }
                case "node_reduce" -> logger.info("node_reduce {}", sig);
                case "final" -> logger.info("final {}", sig);
                case "main.final" -> logger.info("main final {}", sig);
                case "subplan-0.final" -> logger.info("subplan-0 final {}", sig);
                case "subplan-1.final" -> logger.info("subplan-1 final {}", sig);
                default -> throw new IllegalArgumentException("can't match " + driverDescription);
            }
        }
    }

    private void indexValue(CheckedConsumer<XContentBuilder, IOException> mapping, CheckedConsumer<XContentBuilder, IOException> doc)
        throws IOException {
        try {
            // Delete the index if it has already been created.
            client().performRequest(new Request("DELETE", "test"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        Request createIndex = new Request("PUT", "test");
        try (XContentBuilder config = JsonXContent.contentBuilder()) {
            config.startObject();
            config.startObject("settings");
            {
                config.startObject("index");
                config.field("number_of_shards", 1);
                config.endObject();
            }
            config.endObject();
            config.startObject("mappings");
            {
                config.startObject("properties");
                mapping.accept(config);
                config.endObject();
            }
            config.endObject();

            createIndex.setJsonEntity(Strings.toString(config.endObject()));
        }
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "test").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        try (XContentBuilder docJson = JsonXContent.contentBuilder()) {
            docJson.startObject();
            doc.accept(docJson);
            docJson.endObject();
            bulk.setJsonEntity("""
                    {"create":{"_index":"test"}}
                """ + Strings.toString(docJson) + "\n");
        }
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    private void initLookupIndex() throws IOException {
        if (indexExists("lookup")) {
            return;
        }
        Request createIndex = new Request("PUT", "lookup");
        try (XContentBuilder config = JsonXContent.contentBuilder()) {
            config.startObject();
            config.startObject("settings");
            {
                config.startObject("index");
                config.field("number_of_shards", 1);
                config.field("mode", "lookup");
                config.endObject();
            }
            config.endObject();
            config.startObject("mappings");
            {
                config.startObject("properties");
                config.startObject("matching").field("type", "keyword").endObject();
                config.startObject("test").field("type", "keyword").endObject();
                config.endObject();
            }
            config.endObject();

            createIndex.setJsonEntity(Strings.toString(config.endObject()));
        }
        Response createResponse = client().performRequest(createIndex);
        assertThat(
            entityToMap(createResponse.getEntity(), XContentType.JSON),
            matchesMap().entry("shards_acknowledged", true).entry("index", "lookup").entry("acknowledged", true)
        );

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "");
        bulk.setJsonEntity("""
                {"create":{"_index":"lookup"}}
                {"test": "a", "matching": "lookup"}
            """);
        Response bulkResponse = client().performRequest(bulk);
        assertThat(entityToMap(bulkResponse.getEntity(), XContentType.JSON), matchesMap().entry("errors", false).extraOk());
    }

    private CheckedConsumer<XContentBuilder, IOException> justType(String type) {
        return justType("test", type);
    }

    private CheckedConsumer<XContentBuilder, IOException> justType(String fieldName, String type) {
        return b -> b.startObject(fieldName).field("type", type).endObject();
    }

    private static List<String> checkOperatorProfile(
        String driverDesc,
        List<Map<String, Object>> operators,
        List<MapMatcher> expectedLoaders
    ) {
        List<String> sig = new ArrayList<>();
        for (Map<String, Object> operator : operators) {
            String name = (String) operator.get("operator");
            name = PushQueriesIT.TO_NAME.matcher(name).replaceAll("");
            if (name.equals("ValuesSourceReaderOperator")) {
                assertNotNull("Expected loaders to match the ValuesSourceReaderOperator for driver " + driverDesc, expectedLoaders);
                MapMatcher expectedOp = matchesMap().entry("operator", startsWith(name))
                    .entry("status", matchesMap().entry("readers_built", anyOf(expectedLoaders.toArray(new MapMatcher[0]))).extraOk());
                assertMap("Error checking values loaded for driver " + driverDesc + "; ", operator, expectedOp);
            }
            sig.add(name);
        }

        return sig;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Preserve the cluser to speed up the semantic_text tests
        return true;
    }
}
