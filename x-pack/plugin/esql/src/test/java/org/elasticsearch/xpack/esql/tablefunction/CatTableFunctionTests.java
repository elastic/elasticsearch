/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.action.cat.RestShardsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.nullValue;

/**
 * The parts of {@code CAT} that do not need a cluster: what it refuses, what columns it declares, and what it
 * makes of a {@link Table}'s cells. The integration test covers the two requests and the query; everything here
 * is reachable from a table built in the test, which is why the conversion takes one.
 */
public class CatTableFunctionTests extends ESTestCase {

    private static final CatTableFunction FUNCTION = new CatTableFunction(null);

    public void testAnUnknownTableIsRefusedAtItsOwnArgument() {
        ParsingException e = expectThrows(ParsingException.class, () -> FUNCTION.validate(call("nosuchtable", "logs-*")));
        assertThat(e.getMessage(), containsString("[cat] has no table named [nosuchtable]"));
    }

    /** The table is a name a user types, and {@code TableFunctionRegistry} resolves those without regard to case. */
    public void testTheTableNameIsCaseInsensitive() {
        for (String table : List.of("shards", "SHARDS", "Shards")) {
            FUNCTION.validate(call(table, "logs-*"));
        }
    }

    /**
     * A pattern that splits to nothing. {@code ","} is not blank, and an empty indices array on the two requests
     * means every index in the cluster — so the guard reads the split rather than the string.
     */
    public void testAPatternThatNamesNoIndexIsRefused() {
        for (String pattern : List.of("", "   ", ",", ",,")) {
            ParsingException e = expectThrows(ParsingException.class, () -> FUNCTION.validate(call("shards", pattern)));
            assertThat(e.getMessage(), containsString("requires an index pattern"));
        }
    }

    public void testTheSignatureTakesATableAndAPattern() {
        assertThat(FUNCTION.signature().arguments().size(), equalTo(2));
    }

    /** The columns are the cat action's own, and every one is a keyword because a cat cell is text. */
    public void testTheColumnsComeFromTheCatActionAndAreAllKeywords() {
        Table header = RestShardsAction.getTableWithHeader(RestApiVersion.current());
        List<Attribute> columns = written(header).columns();

        List<String> expected = header.getHeaders().stream().map(cell -> String.valueOf(cell.value)).toList();
        assertThat(columns.stream().map(Attribute::name).toList(), equalTo(expected));
        assertThat(columns.stream().map(Attribute::dataType).toList(), everyItem(equalTo(DataType.KEYWORD)));
    }

    /**
     * A cell renders the way it renders itself, which for a size is text and not a number — the reason the
     * columns are keywords and {@code SUM(store)} does not work without a cast.
     */
    public void testACellRendersAsItsOwnText() {
        Table table = table(List.of("0", ByteSizeValue.ofBytes(227), 42));

        assertThat(row(table, 0), contains("0", "227b", "42"));
    }

    /** A null cell is an absent value. Rendering it would put the four letters in the column. */
    public void testANullCellStaysNull() {
        Table table = table(List.of("0", "227b", "x"));
        table.startRow();
        table.addCell("1");
        table.addCell(null);
        table.addCell(null);
        table.endRow();

        assertThat(row(table, 1), contains(equalTo("1"), nullValue(), nullValue()));
    }

    /**
     * Past the cap the read is refused naming the pattern, as an {@code IllegalArgumentException} so it reaches
     * the caller as a 400: too many shards is their input and their fix. The base class's own cap would throw an
     * {@code IllegalStateException}, which maps to a 500.
     */
    public void testPastTheCapTheReadIsRefusedAsABadRequest() {
        Table table = new Table();
        table.startHeaders().addCell("index").endHeaders();
        for (int i = 0; i < 10_001; i++) {
            table.startRow();
            table.addCell("i" + i);
            table.endRow();
        }

        try (TableRows rows = new TableRows(CatTableFunction.NAME, TestBlockFactory.getNonBreakingInstance())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CatTableFunction.write("logs-*", table, rows));
            assertThat(e.getMessage(), containsString("matched [10001] shards for [logs-*]"));
            assertThat(e.getMessage(), containsString("narrow the index pattern"));
        }
    }

    /** The table CAT writes for {@code table}, which the caller owns and releases. */
    private static TableRows written(Table table) {
        TableRows rows = new TableRows(CatTableFunction.NAME, TestBlockFactory.getNonBreakingInstance());
        boolean success = false;
        try {
            CatTableFunction.write("logs-*", table, rows);
            success = true;
            return rows;
        } finally {
            if (success == false) {
                rows.close();
            }
        }
    }

    /** One row of what CAT wrote, as the strings a reader sees, with an absent cell as null. */
    private static List<String> row(Table table, int position) {
        TableRows rows = written(table);
        try (Page page = rows.page()) {
            List<String> cells = new ArrayList<>(page.getBlockCount());
            BytesRef scratch = new BytesRef();
            for (int c = 0; c < page.getBlockCount(); c++) {
                BytesRefBlock block = page.getBlock(c);
                cells.add(block.isNull(position) ? null : block.getBytesRef(position, scratch).utf8ToString());
            }
            return cells;
        }
    }

    private static TableFunctionInvocation call(String table, String pattern) {
        return new TableFunctionInvocation(
            Source.EMPTY,
            CatTableFunction.NAME,
            List.of(new TableFunctionArgument.Name(Source.EMPTY, table), new TableFunctionArgument.Name(Source.EMPTY, pattern))
        );
    }

    /** One header per value, one row holding them, which is all the conversion looks at. */
    private static Table table(List<Object> values) {
        Table table = new Table();
        table.startHeaders();
        for (int i = 0; i < values.size(); i++) {
            table.addCell("c" + i);
        }
        table.endHeaders();
        table.startRow();
        for (Object value : values) {
            table.addCell(value);
        }
        table.endRow();
        return table;
    }
}
