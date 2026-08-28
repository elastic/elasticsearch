/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * What this renderer gets wrong is invisible downstream, which is why the refusals are tested as hard
 * as the renderings. A cell silently re-quoted in PLAIN, or a comma left unescaped in ESCAPED, corrupts
 * the values of one row while leaving the row count and every aggregate intact -- so a probe reading
 * five rows and a SUM passes over the damage.
 */
public class TextRowRendererTests extends ESTestCase {

    private static CsvFixtureParser.CsvFixtureResult fixture(Object[]... rows) {
        return new CsvFixtureParser.CsvFixtureResult(
            List.of(new CsvFixtureParser.ColumnSpec("name", "keyword"), new CsvFixtureParser.ColumnSpec("age", "integer")),
            List.of(rows)
        );
    }

    public void testQuotedWrapsOnlyWhatNeedsIt() {
        String out = new TextRowRenderer(',', TextRowRenderer.Dialect.QUOTED, true).render(fixture(new Object[] { "plain", 30 }));
        assertThat(out, equalTo("name:keyword,age:integer\nplain,30\n"));
    }

    public void testQuotedDoublesAnInternalQuote() {
        String out = new TextRowRenderer(',', TextRowRenderer.Dialect.QUOTED, false).render(fixture(new Object[] { "sa\"id", 1 }));
        assertThat(out, equalTo("\"sa\"\"id\",1\n"));
    }

    public void testQuotedWrapsAValueHoldingTheDelimiter() {
        String out = new TextRowRenderer(',', TextRowRenderer.Dialect.QUOTED, false).render(fixture(new Object[] { "a,b", 1 }));
        assertThat(out, equalTo("\"a,b\",1\n"));
    }

    /**
     * The load-bearing one. Leaving the delimiter out of the escape set misaligns every row that
     * contains one and leaves the rest correct -- damage no row count or aggregate can see.
     */
    public void testEscapedEscapesTheDelimiter() {
        String out = new TextRowRenderer(',', TextRowRenderer.Dialect.ESCAPED, false).render(fixture(new Object[] { "a,b", 1 }));
        assertThat(out, equalTo("a\\,b,1\n"));
    }

    public void testEscapedEscapesBackslashAndNewline() {
        String out = new TextRowRenderer(',', TextRowRenderer.Dialect.ESCAPED, false).render(fixture(new Object[] { "a\\b\nc", 1 }));
        assertThat(out, equalTo("a\\\\b\\nc,1\n"));
    }

    /** An escaped empty field cannot be told from an empty string, so null needs its own spelling. */
    public void testEscapedWritesNullDistinctlyFromEmpty() {
        String out = new TextRowRenderer(',', TextRowRenderer.Dialect.ESCAPED, false).render(fixture(new Object[] { null, 1 }));
        assertThat(out, equalTo("\\N,1\n"));
    }

    public void testPlainWritesWhatNeedsNothing() {
        String out = new TextRowRenderer(',', TextRowRenderer.Dialect.PLAIN, false).render(fixture(new Object[] { "plain", 30 }));
        assertThat(out, equalTo("plain,30\n"));
    }

    /** PLAIN has no mechanism for this. Re-quoting would be read back as literal quote characters. */
    public void testPlainRefusesAValueHoldingTheDelimiter() {
        TextRowRenderer renderer = new TextRowRenderer(',', TextRowRenderer.Dialect.PLAIN, false);
        Exception e = expectThrows(IllegalArgumentException.class, () -> renderer.render(fixture(new Object[] { "a,b", 1 })));
        assertThat(e.getMessage(), containsString("unrepresentable in PLAIN"));
    }

    public void testPlainRefusesALeadingQuote() {
        TextRowRenderer renderer = new TextRowRenderer(',', TextRowRenderer.Dialect.PLAIN, false);
        Exception e = expectThrows(IllegalArgumentException.class, () -> renderer.render(fixture(new Object[] { "\"x", 1 })));
        assertThat(e.getMessage(), containsString("unrepresentable in PLAIN"));
    }

    /** Brackets need quote-aware tokenisation, which is exactly what the other two dialects lack. */
    public void testOnlyQuotedCarriesAMultiValueCell() {
        Object[] row = new Object[] { List.of("a", "b"), 1 };
        assertThat(new TextRowRenderer(',', TextRowRenderer.Dialect.QUOTED, false).render(fixture(row)), equalTo("\"[a,b]\",1\n"));
        for (TextRowRenderer.Dialect dialect : List.of(TextRowRenderer.Dialect.ESCAPED, TextRowRenderer.Dialect.PLAIN)) {
            TextRowRenderer renderer = new TextRowRenderer(',', dialect, false);
            Exception e = expectThrows(IllegalArgumentException.class, () -> renderer.render(fixture(row)));
            assertThat(e.getMessage(), containsString("multi-value cell"));
        }
    }

    public void testHeaderRowFalseOmitsOnlyTheHeader() {
        String out = new TextRowRenderer(',', TextRowRenderer.Dialect.QUOTED, false).render(fixture(new Object[] { "x", 1 }));
        assertThat(out, equalTo("x,1\n"));
    }

    /** Tab-delimited output must escape tabs and leave commas alone -- the delimiter is a parameter. */
    public void testTheDelimiterIsWhateverItWasConstructedWith() {
        String out = new TextRowRenderer('\t', TextRowRenderer.Dialect.ESCAPED, false).render(fixture(new Object[] { "a,b\tc", 1 }));
        assertThat(out, equalTo("a,b\\tc\t1\n"));
    }
}
