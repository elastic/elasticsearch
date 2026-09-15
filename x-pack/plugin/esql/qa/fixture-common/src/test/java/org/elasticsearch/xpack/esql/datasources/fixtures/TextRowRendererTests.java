/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
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

    /**
     * A non-default quote character is actually used.
     *
     * <p>Untested until now, and that gap let a real defect ship: both generators called the 3-arg
     * constructor, so quote and escape stayed at their defaults while the suite announced the pinned
     * values to the reader. Bytes written with one grammar and read as another parse cleanly and mean
     * something else -- the failure with no symptom.
     */
    public void testQuoteCharacterIsHonoured() {
        String out = new TextRowRenderer(',', '\'', '\\', TextRowRenderer.Dialect.QUOTED, false).render(fixture(new Object[] { "a,b", 1 }));
        assertThat(out, equalTo("'a,b',1\n"));
        assertThat("the default quote must not appear", out.contains("\""), equalTo(false));
    }

    /** An internal occurrence of the configured quote is doubled, like the default one is. */
    public void testConfiguredQuoteIsDoubledWhenItAppearsInAValue() {
        String out = new TextRowRenderer(',', '\'', '\\', TextRowRenderer.Dialect.QUOTED, false).render(
            fixture(new Object[] { "it's", 1 })
        );
        assertThat(out, equalTo("'it''s',1\n"));
    }

    /** A non-default escape character is used for the delimiter, the quote, and itself. */
    public void testEscapeCharacterIsHonoured() {
        String out = new TextRowRenderer(',', '"', '~', TextRowRenderer.Dialect.ESCAPED, false).render(
            fixture(new Object[] { "a,b~c", 1 })
        );
        assertThat(out, equalTo("a~,b~~c,1\n"));
        assertThat("the default escape must not appear", out.contains("\\"), equalTo(false));
    }

    /** The null spelling rides the escape character rather than a hard-coded backslash. */
    public void testNullSpellingUsesTheConfiguredEscape() {
        String out = new TextRowRenderer(',', '"', '~', TextRowRenderer.Dialect.ESCAPED, false).render(fixture(new Object[] { null, 1 }));
        assertThat(out, equalTo("~N,1\n"));
    }

    /**
     * A blank source cell must stay a blank in every dialect, and a literal {@code null} must stay a null.
     *
     * <p>The parser maps both to Java {@code null} so a columnar twin of an UNDECLARED dataset answers like
     * the CSV read of the same file. A text rendering cannot inherit that: ESCAPED spells a null {@code \N}
     * and a blank as an empty field, so an authored blank came out as a null token and a declared keyword
     * column then read {@code null} where the contract owes {@code ""}. QUOTED and PLAIN hid it, because a
     * null renders empty there and reads back as the blank it started as -- which is why this asserts on
     * both, not only on the dialect that failed.
     */
    public void testBlankStaysBlankAndLiteralNullStaysNullInEveryDialect() throws Exception {
        Path src = createTempFile("blank", ".csv");
        Files.writeString(src, "name:keyword,age:integer\nalice,30\n,31\nnull,32\n");
        CsvFixtureParser.CsvFixtureResult parsed = CsvFixtureParser.parseCsvFile(src);

        String escaped = new TextRowRenderer(',', TextRowRenderer.Dialect.ESCAPED, true).render(parsed);
        assertThat("the blank row keeps a blank field", escaped, containsString("\n,31\n"));
        assertThat("the literal null row keeps the null token", escaped, containsString("\\N,32"));

        for (TextRowRenderer.Dialect d : List.of(TextRowRenderer.Dialect.QUOTED, TextRowRenderer.Dialect.PLAIN)) {
            String out = new TextRowRenderer(',', d, true).render(parsed);
            assertThat(d + " keeps the blank a blank", out, containsString("\n,31\n"));
        }
    }

}
