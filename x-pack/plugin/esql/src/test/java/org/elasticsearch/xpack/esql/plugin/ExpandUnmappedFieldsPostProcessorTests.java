/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ExpandUnmappedFieldsPostProcessorTests extends ComputeTestCase {
    public void testExpandsAcrossPagesUnioningFieldNames() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(
                page(bf, List.of(row(1, jsonObject("{'pet':'Rex','city':'Berlin'}")))),
                page(bf, List.of(row(2, jsonObject("{'pet':'Max','zip':'10115'}"))))
            )
        );

        Result expanded = expand(result, bf);
        try {
            assertThat(expanded, not(sameInstance(result)));
            assertThat(names(expanded), equalTo(List.of(INT_ATTR, "city", "pet", "zip")));
            assertThat(dataTypes(expanded), equalTo(List.of(DataType.INTEGER, DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD)));
            assertThat(
                nonNullRows(expanded),
                contains(
                    matchesMap().entry(INT_ATTR, 1).entry("city", "Berlin").entry("pet", "Rex"),
                    matchesMap().entry(INT_ATTR, 2).entry("pet", "Max").entry("zip", "10115")
                )
            );
            assertThat(expanded.configuration(), sameInstance(result.configuration()));
            assertThat(expanded.completionInfo(), sameInstance(result.completionInfo()));
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testReturnsSameResultWhenNoUnmappedFieldsAttribute() {
        BlockFactory bf = blockFactory();
        Result result = result(List.of(intAttr()), List.of(page(bf, List.of(row(1)))));

        Result expanded = expand(result, bf);
        try {
            assertThat(expanded, sameInstance(result));
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testNullAndEmptyUnmappedValuesContributeNoFields() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(page(bf, List.of(row(1, null), row(2, jsonObject("{}")), row(3, jsonObject("{'a':'x'}")))))
        );

        Result expanded = expand(result, bf);
        try {
            assertThat(names(expanded), equalTo(List.of(INT_ATTR, "a")));
            assertThat(
                nonNullRows(expanded),
                contains(matchesMap().entry(INT_ATTR, 1), matchesMap().entry(INT_ATTR, 2), matchesMap().entry(INT_ATTR, 3).entry("a", "x"))
            );
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testAllNullUnmappedProducesNoExpandedColumns() {
        BlockFactory bf = blockFactory();
        Result result = result(List.of(intAttr(), unmappedAttr()), List.of(page(bf, List.of(row(1, null), row(2, null)))));

        Result expanded = expand(result, bf);
        try {
            // _unmapped_fields is dropped and nothing replaces it.
            assertThat(names(expanded), equalTo(List.of(INT_ATTR)));
            assertThat(nonNullRows(expanded), contains(matchesMap().entry(INT_ATTR, 1), matchesMap().entry(INT_ATTR, 2)));
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testNetZeroProjectionEmptyJsonProducesZeroColumns() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(unmappedAttr()),
            List.of(page(bf, List.of(row(jsonObject("{}")), row(jsonObject("{}")), row(jsonObject("{}")))))
        );

        Result expanded = expand(result, bf);
        try {
            assertThat(names(expanded), equalTo(List.of()));
            assertThat(rowCount(expanded), equalTo(3));
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testNetZeroProjectionAllNullUnmappedProducesZeroColumns() {
        BlockFactory bf = blockFactory();
        Page page;
        try (BytesRefBlock.Builder builder = bf.newBytesRefBlockBuilder(2)) {
            builder.appendNull();
            builder.appendNull();
            page = new Page(builder.build());
        }
        Result result = result(List.of(unmappedAttr()), List.of(page));

        Result expanded = expand(result, bf);
        try {
            assertThat(names(expanded), equalTo(List.of()));
            assertThat(rowCount(expanded), equalTo(2));
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testNetZeroProjectionWithUnmappedNamesExpandsToUnmappedColumnsOnly() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(unmappedAttr()),
            List.of(page(bf, List.of(row(jsonObject("{'a':'x','b':'y'}")), row(jsonObject("{'a':'z'}")))))
        );

        Result expanded = expand(result, bf);
        try {
            assertThat(names(expanded), equalTo(List.of("a", "b")));
            assertThat(nonNullRows(expanded), contains(matchesMap().entry("a", "x").entry("b", "y"), matchesMap().entry("a", "z")));
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testRetainedColumnWithEmptyJsonRowsProducesNoExpandedColumns() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(page(bf, List.of(row(1, jsonObject("{}")), row(2, jsonObject("{}")))))
        );

        Result expanded = expand(result, bf);
        try {
            assertThat(names(expanded), equalTo(List.of(INT_ATTR)));
            assertThat(nonNullRows(expanded), contains(matchesMap().entry(INT_ATTR, 1), matchesMap().entry(INT_ATTR, 2)));
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testNetZeroProjectionAcrossMultiplePagesPreservesRowCount() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(unmappedAttr()),
            List.of(page(bf, List.of(row(jsonObject("{}")), row(jsonObject("{}")))), page(bf, List.of(row(jsonObject("{}")))))
        );

        Result expanded = expand(result, bf);
        try {
            assertThat(names(expanded), equalTo(List.of()));
            assertThat(rowCount(expanded), equalTo(3));
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    /**
     * The coordinator trusts the data node to only send keys that hold a value - {@code UnmappedFieldsBlockLoaderTests} pins down that
     * end of the contract. This is the guard rail for the other end: were a value-less key to arrive anyway, it would expand into a
     * column that is null in every row, which reads as "every document has this field, with no value" where the truth is "no document
     * has this field". That must not pass silently.
     */
    public void testAllNullExpandedColumnTripsGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(page(bf, List.of(row(1, jsonObject("{'a':null}")), row(2, jsonObject("{'a':null,'b':'y'}")))))
        );

        AssertionError e = expectThrows(AssertionError.class, () -> expand(result, bf));
        assertThat(e.getMessage(), containsString("Expanded unmapped field 'a' into a column that is null in every row"));

        // No manual release here: the point is that expand must have released both the input pages and the half-built expansion.
        assertThat("the guard rail leaked pages", bf.breaker().getUsed(), equalTo(0L));
    }

    /** The column has to be null in every row of every page, so the guard rail only trips once all pages agree. */
    public void testAllNullExpandedColumnAcrossPagesTripsGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(
                page(bf, List.of(row(1, jsonObject("{'a':null}")))),
                page(bf, List.of(row(2, jsonObject("{'a':null,'b':'y'}")))),
                page(bf, List.of(row(3, jsonObject("{'b':'z'}"))))
            )
        );

        AssertionError e = expectThrows(AssertionError.class, () -> expand(result, bf));
        assertThat(e.getMessage(), containsString("Expanded unmapped field 'a' into a column that is null in every row"));
        assertThat("the guard rail leaked pages", bf.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The flip side of {@link #testAllNullExpandedColumnAcrossPagesTripsGuardRail}: a single value in a single page is enough to
     * justify the column, so the guard rail must stay quiet however many other pages are null throughout.
     */
    public void testValueInOnePageOnlyDoesNotTripGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(
                page(bf, List.of(row(1, jsonObject("{'b':'y'}")))),
                page(bf, List.of(row(2, jsonObject("{'a':'x'}")), row(3, jsonObject("{'b':'z'}")))),
                page(bf, List.of(row(4, jsonObject("{'b':'w'}"))))
            )
        );

        Result expanded = expand(result, bf);
        try {
            assertThat(names(expanded), equalTo(List.of(INT_ATTR, "a", "b")));
            assertThat(
                nonNullRows(expanded),
                contains(
                    matchesMap().entry(INT_ATTR, 1).entry("b", "y"),
                    matchesMap().entry(INT_ATTR, 2).entry("a", "x"),
                    matchesMap().entry(INT_ATTR, 3).entry("b", "z"),
                    matchesMap().entry(INT_ATTR, 4).entry("b", "w")
                )
            );
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    /**
     * The data node strips nulls out of the arrays it keeps, because appendRow renders the whole value and a surviving null would
     * reach the user as a literal "null" inside a stringified array. This is the guard rail for that half of the contract.
     */
    public void testNullInsideArrayTripsGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(List.of(intAttr(), unmappedAttr()), List.of(page(bf, List.of(row(1, jsonObject("{'a':[null,'x']}"))))));

        AssertionError e = expectThrows(AssertionError.class, () -> expand(result, bf));
        assertThat(e.getMessage(), containsString("Unmapped field 'a' carries a null or an empty array or object"));
        assertThat("the guard rail leaked pages", bf.breaker().getUsed(), equalTo(0L));
    }

    /** Same guard rail, for a null buried under an object rather than sitting in an array. */
    public void testNullInsideObjectTripsGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(page(bf, List.of(row(1, jsonObject("{'a':{'keep':'me','drop':[]}}")))))
        );

        AssertionError e = expectThrows(AssertionError.class, () -> expand(result, bf));
        assertThat(e.getMessage(), containsString("Unmapped field 'a' carries a null or an empty array or object"));
        assertThat("the guard rail leaked pages", bf.breaker().getUsed(), equalTo(0L));
    }

    /** An object that pruned away to nothing should never have been sent, let alone rendered as a literal "{}". */
    public void testEmptyObjectTripsGuardRail() {
        BlockFactory bf = blockFactory();
        Result result = result(List.of(intAttr(), unmappedAttr()), List.of(page(bf, List.of(row(1, jsonObject("{'a':{}}"))))));

        AssertionError e = expectThrows(AssertionError.class, () -> expand(result, bf));
        assertThat(e.getMessage(), containsString("Unmapped field 'a' carries a null or an empty array or object"));
        assertThat("the guard rail leaked pages", bf.breaker().getUsed(), equalTo(0L));
    }

    public void testNonStringJsonValuesAreStringified() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr()),
            List.of(page(bf, List.of(row(1, jsonObject("{'count':5,'active':true,'nested':{'x':1}}")))))
        );

        Result expanded = expand(result, bf);
        try {
            assertThat(names(expanded), equalTo(List.of(INT_ATTR, "active", "count", "nested")));
            assertThat(
                nonNullRows(expanded),
                contains(matchesMap().entry(INT_ATTR, 1).entry("active", "true").entry("count", "5").entry("nested", "{x=1}"))
            );
        } finally {
            Releasables.close(expanded.pages());
        }
    }

    public void testExpandUnderCrankyBreakerDoesNotLeak() {
        testWithCrankyBlockFactory(bf -> {
            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                fieldNames.add("field_" + i);
            }
            StringBuilder json = new StringBuilder("{");
            for (int i = 0; i < fieldNames.size(); i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append('"').append(fieldNames.get(i)).append("\":\"v").append(i).append('"');
            }
            json.append('}');

            List<List<Object>> rows = new ArrayList<>();
            for (int row = 0; row < 20; row++) {
                rows.add(row(row, json.toString()));
            }
            Result result = result(List.of(intAttr(), unmappedAttr()), List.of(page(bf, rows)));
            // testWithCrankyBlockFactory catches the CircuitBreakingException and asserts its message; ComputeTestCase then
            // asserts every breaker is back to zero, which is what this test is about.
            Releasables.close(expand(result, bf).pages());
        });
    }

    public void testExpandReleasesInputPagesWhenExpansionFails() {
        BlockFactory bf = blockFactory();
        // Query column "a" collides with the "a" key discovered in the _unmapped_fields JSON, so buildSchema throws.
        Result result = result(List.of(keywordAttr("a"), unmappedAttr()), List.of(page(bf, List.of(row("v", jsonObject("{'a':'x'}"))))));
        assertThat("input pages should reserve breaker memory before expand runs", bf.breaker().getUsed(), greaterThan(0L));

        var e = expectThrows(IllegalStateException.class, () -> expand(result, bf));
        assertThat(e.getMessage(), containsString("Conflict in unmapped field name"));

        // No manual release here: the point is that expand must have released the input pages on its failure path.
        assertThat("expand leaked the input pages on failure", bf.breaker().getUsed(), equalTo(0L));
    }

    private static Result expand(Result result, BlockFactory blockFactory) {
        return ExpandUnmappedFieldsPostProcessor.expand(result, blockFactory, PlannerSettings.DEFAULTS);
    }

    private static Result result(List<Attribute> schema, List<Page> pages) {
        return new Result(schema, pages, Map.of(), EsqlTestUtils.TEST_CFG, DriverCompletionInfo.EMPTY, null);
    }

    private static Attribute intAttr() {
        return new ReferenceAttribute(Source.EMPTY, null, INT_ATTR, DataType.INTEGER);
    }

    private static Attribute keywordAttr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    private static UnmappedFieldsAttribute unmappedAttr() {
        return new UnmappedFieldsAttribute(Source.EMPTY, UnmappedFieldsPattern.ALL);
    }

    /** Builds a single page whose blocks are inferred from {@code rows} (one {@link #row} per position). */
    private static Page page(BlockFactory bf, List<List<Object>> rows) {
        return new Page(BlockUtils.fromList(bf, rows));
    }

    /** A single row of column values; accepts {@code null} cells (unlike {@link List#of}). */
    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /** Turns single quotes into double quotes so JSON literals read without escaping. */
    private static String jsonObject(String singleQuoted) {
        return singleQuoted.replace('\'', '"');
    }

    private static List<String> names(Result r) {
        return r.schema().stream().map(Attribute::name).toList();
    }

    private static int rowCount(Result r) {
        return r.pages().stream().mapToInt(Page::getPositionCount).sum();
    }

    private static List<DataType> dataTypes(Result r) {
        return r.schema().stream().map(Attribute::dataType).toList();
    }

    /** One map per row holding only the non-null cells (a null expanded cell means the row lacked that field). */
    private static List<Map<String, Object>> nonNullRows(Result r) {
        List<Attribute> schema = r.schema();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Page page : r.pages()) {
            for (int row = 0; row < page.getPositionCount(); row++) {
                Map<String, Object> cells = new LinkedHashMap<>();
                for (int col = 0; col < schema.size(); col++) {
                    Object value = valueAt(page.getBlock(col), row);
                    if (value != null) {
                        cells.put(schema.get(col).name(), value);
                    }
                }
                rows.add(cells);
            }
        }
        return rows;
    }

    private static @Nullable Object valueAt(Block block, int row) {
        Object value = BlockUtils.toJavaObject(block, row);
        return value instanceof BytesRef bytesRef ? bytesRef.utf8ToString() : value;
    }

    private static final String INT_ATTR = "emp_no";
}
