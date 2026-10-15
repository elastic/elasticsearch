/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests that the column of unmapped fields of {@code SET unmapped_fields="LOAD_ALL"} is returned as {@code _unmapped_fields},
 * with the values the shards wrote.
 */
public class UnmappedFieldsColumnTests extends ComputeTestCase {
    private static final String INT_ATTR = "emp_no";

    public void testNamesTheColumnInPlaceAndReturnsItsValuesAsTheyAre() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(intAttr(), unmappedAttr(), keywordAttr("last")),
            List.of(
                page(bf, List.of(row(1, "{\"b.c\":[1,2],\"a\":\"x\"}", "l1"), row(2, null, "l2"))),
                page(bf, List.of(row(3, "{\"zip\":\"10115\"}", "l3")))
            )
        );

        Result exposed = UnmappedFieldsColumn.expose(result);
        try {
            assertThat(names(exposed), equalTo(List.of(INT_ATTR, UnmappedFieldsColumn.NAME, "last")));
            assertThat(dataTypes(exposed), equalTo(List.of(DataType.INTEGER, DataType.SOURCE, DataType.KEYWORD)));
            assertThat(exposed.pages(), sameInstance(result.pages()));
            assertThat(
                rows(exposed),
                equalTo(List.of(row(1, "{\"b.c\":[1,2],\"a\":\"x\"}", "l1"), row(2, null, "l2"), row(3, "{\"zip\":\"10115\"}", "l3")))
            );
            assertThat(exposed.configuration(), sameInstance(result.configuration()));
            assertThat(exposed.completionInfo(), sameInstance(result.completionInfo()));
        } finally {
            Releasables.close(exposed.pages());
        }
    }

    public void testReturnsTheSameResultWithoutTheColumn() {
        BlockFactory bf = blockFactory();
        Result result = result(List.of(intAttr()), List.of(page(bf, List.of(row(1)))));
        try {
            assertThat(UnmappedFieldsColumn.expose(result), sameInstance(result));
        } finally {
            Releasables.close(result.pages());
        }
    }

    public void testAColumnOfTheQueryWithTheSameNameIsKept() {
        BlockFactory bf = blockFactory();
        Result result = result(
            List.of(keywordAttr(UnmappedFieldsColumn.NAME), unmappedAttr(), intAttr()),
            List.of(page(bf, List.of(row("mine", "{\"a\":\"x\"}", 1), row("yours", null, 2))), page(bf, List.of(row("theirs", null, 3))))
        );

        Result exposed = UnmappedFieldsColumn.expose(result);
        try {
            assertThat(names(exposed), equalTo(List.of(UnmappedFieldsColumn.NAME, INT_ATTR)));
            assertThat(dataTypes(exposed), equalTo(List.of(DataType.KEYWORD, DataType.INTEGER)));
            assertThat(rows(exposed), equalTo(List.of(row("mine", 1), row("yours", 2), row("theirs", 3))));
        } finally {
            Releasables.close(exposed.pages());
        }
    }

    private static Result result(List<Attribute> schema, List<Page> pages) {
        return new Result(schema, pages, Map.of(), EsqlTestUtils.TEST_CFG, DriverCompletionInfo.EMPTY, null, null);
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

    private static List<String> names(Result r) {
        return r.schema().stream().map(Attribute::name).toList();
    }

    private static List<DataType> dataTypes(Result r) {
        return r.schema().stream().map(Attribute::dataType).toList();
    }

    private static List<List<Object>> rows(Result r) {
        List<List<Object>> rows = new ArrayList<>();
        for (Page page : r.pages()) {
            for (int row = 0; row < page.getPositionCount(); row++) {
                Object[] cells = new Object[page.getBlockCount()];
                for (int col = 0; col < cells.length; col++) {
                    Object value = BlockUtils.toJavaObject(page.getBlock(col), row);
                    cells[col] = value instanceof BytesRef bytesRef ? bytesRef.utf8ToString() : value;
                }
                rows.add(row(cells));
            }
        }
        return rows;
    }
}
