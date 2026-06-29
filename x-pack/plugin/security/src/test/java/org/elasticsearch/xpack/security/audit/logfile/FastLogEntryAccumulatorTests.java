/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.audit.logfile.FastLogEntryAccumulator.FieldType;
import org.elasticsearch.xpack.security.audit.logfile.FastLogEntryAccumulator.LogField;
import org.elasticsearch.xpack.security.audit.logfile.FastLogEntryAccumulator.LogFormat;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests for {@link FastLogEntryAccumulator} as a general-purpose fixed-schema key/value store with deferred JSON rendering.
 * Uses a test-local minimal {@link LogFormat} — does not depend on {@link FastLogEntryAccumulator#AUDIT_FORMAT}.
 */
public class FastLogEntryAccumulatorTests extends ESTestCase {

    private static final LogFormat FORMAT = LogFormat.of(
        new LogField("alpha", FieldType.STRING),
        new LogField("beta", FieldType.STRING_ARRAY),
        new LogField("gamma", FieldType.RAW),
        new LogField("delta", FieldType.STRING)
    );

    private FastLogEntryAccumulator entry() {
        return new FastLogEntryAccumulator(FORMAT, Map.of());
    }

    private String render(FastLogEntryAccumulator acc) {
        final StringBuilder sb = new StringBuilder();
        acc.formatTo(sb);
        return sb.toString();
    }

    public void testLogFormatIndexOf() {
        assertThat(FORMAT.indexOf("alpha"), equalTo(0));
        assertThat(FORMAT.indexOf("beta"), equalTo(1));
        assertThat(FORMAT.indexOf("gamma"), equalTo(2));
        assertThat(FORMAT.indexOf("delta"), equalTo(3));
        assertThat(FORMAT.indexOf("nonexistent"), equalTo(-1));
    }

    public void testLogFieldPrefixIsPrecomputed() {
        assertThat(new LogField("my_field", FieldType.STRING).prefix(), equalTo(", \"my_field\":"));
    }

    public void testSetAndGet() {
        assertThat(entry().with("alpha", "hello").get("alpha"), equalTo("hello"));
    }

    public void testGetUnsetFieldReturnsNull() {
        assertThat(entry().get("alpha"), nullValue());
    }

    public void testGetUnknownFieldReturnsNull() {
        assertThat(entry().get("nonexistent"), nullValue());
    }

    public void testWithNullValueIsNoOp() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "set").with("alpha", null);
        assertThat(acc.get("alpha"), equalTo("set"));
    }

    public void testWithUnknownFieldIsIgnored() {
        final FastLogEntryAccumulator acc = entry().with("nonexistent", "value");
        assertThat(acc.get("nonexistent"), nullValue());
        assertThat(render(acc), equalTo(""));
    }

    public void testWithOverwritesPreviousValue() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "first").with("alpha", "second");
        assertThat(acc.get("alpha"), equalTo("second"));
        assertThat(render(acc), equalTo(", \"alpha\":\"second\""));
    }

    public void testRemoveClearsField() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "hello");
        acc.remove("alpha");
        assertThat(acc.get("alpha"), nullValue());
        assertThat(render(acc), equalTo(""));
    }

    public void testRemoveUnknownFieldDoesNotThrow() {
        entry().remove("nonexistent");
    }

    public void testEmptyEntryProducesEmptyOutput() {
        assertThat(render(entry()), equalTo(""));
    }

    public void testStringField() {
        assertThat(render(entry().with("alpha", "hello")), equalTo(", \"alpha\":\"hello\""));
    }

    public void testStringFieldSkipsEmpty() {
        assertThat(render(entry().with("alpha", "")), equalTo(""));
    }

    public void testRawFieldEmittedVerbatim() {
        assertThat(render(entry().with("gamma", "{\"k\":1}")), equalTo(", \"gamma\":{\"k\":1}"));
    }

    public void testStringArrayField() {
        assertThat(render(entry().with("beta", new Object[] { "x", "y", "z" })), equalTo(", \"beta\":[\"x\",\"y\",\"z\"]"));
    }

    public void testStringArraySkipsNullElements() {
        assertThat(render(entry().with("beta", new Object[] { "a", null, "c" })), equalTo(", \"beta\":[\"a\",\"c\"]"));
    }

    public void testStringArrayAllNullsProducesEmptyArray() {
        assertThat(render(entry().with("beta", new Object[] { null, null })), equalTo(", \"beta\":[]"));
    }

    public void testFieldsAppearInFormatOrder() {
        // with() calls are intentionally out of FORMAT slot order; output must follow FORMAT order
        final String output = render(entry().with("delta", "d").with("alpha", "a").with("gamma", "{}"));
        assertThat(output, equalTo(", \"alpha\":\"a\", \"gamma\":{}, \"delta\":\"d\""));
    }

    public void testMultipleStringFieldsCommaSeparated() {
        assertThat(render(entry().with("alpha", "a1").with("delta", "d1")), equalTo(", \"alpha\":\"a1\", \"delta\":\"d1\""));
    }

    public void testPrintableAsciiIsPassedThroughVerbatim() {
        // All chars in [0x20, 0x7E] except '"' and '\' take the fast path and appear unmodified
        final String safe = "hello world 0-9 A-Z a-z !#$%&'()*+,-./:;<=>?@[]^_`{|}~";
        assertThat(render(entry().with("alpha", safe)), equalTo(", \"alpha\":\"" + safe + "\""));
    }

    public void testSpaceIsFirstSafeChar() {
        // 0x20 is the lowest char that isAsciiSafe accepts
        assertThat(render(entry().with("alpha", " ")), equalTo(", \"alpha\":\" \""));
    }

    public void testTildeIsLastSafeChar() {
        // 0x7E (~) is the highest char that isAsciiSafe accepts
        assertThat(render(entry().with("alpha", "~")), equalTo(", \"alpha\":\"~\""));
    }

    public void testDoubleQuoteIsEscaped() {
        assertThat(render(entry().with("alpha", "say \"hi\"")), equalTo(", \"alpha\":\"say \\\"hi\\\"\""));
    }

    public void testBackslashIsEscaped() {
        assertThat(render(entry().with("alpha", "C:\\Users\\foo")), equalTo(", \"alpha\":\"C:\\\\Users\\\\foo\""));
    }

    public void testTabIsEscaped() {
        assertThat(render(entry().with("alpha", "col1\tcol2")), equalTo(", \"alpha\":\"col1\\tcol2\""));
    }

    public void testNewlineIsEscaped() {
        assertThat(render(entry().with("alpha", "line1\nline2")), equalTo(", \"alpha\":\"line1\\nline2\""));
    }

    public void testCarriageReturnIsEscaped() {
        assertThat(render(entry().with("alpha", "a\rb")), equalTo(", \"alpha\":\"a\\rb\""));
    }

    public void testFormFeedIsEscaped() {
        assertThat(render(entry().with("alpha", "a\fb")), equalTo(", \"alpha\":\"a\\fb\""));
    }

    public void testNullCharIsEscaped() {
        assertThat(render(entry().with("alpha", "a\u0000b")), equalTo(", \"alpha\":\"a\\u0000b\""));
    }

    public void testUnitSeparatorOneBelowSafeFloorIsEscaped() {
        // 0x1F: the last control char, one below isAsciiSafe's 0x20 floor
        final String output = render(entry().with("alpha", "a\u001Fb"));
        assertFalse("U+001F must not appear raw in JSON output", output.contains("\u001F"));
    }

    public void testDelCharOnceAboveSafeCeilingIsHandledBySlowPath() {
        // 0x7F (DEL) > 0x7E, so isAsciiSafe returns false and quoteAsString is called.
        // JsonStringEncoder does not escape 0x7F by default, so it passes through as-is.
        assertThat(render(entry().with("alpha", "a\u007Fb")), equalTo(", \"alpha\":\"a\u007Fb\""));
    }

    public void testLatinAccentedCharsAreHandledBySlowPath() {
        // U+00E9 (é) > 0x7E → quoteAsString; Jackson emits non-ASCII UTF-8 chars unescaped
        assertThat(render(entry().with("alpha", "café")), equalTo(", \"alpha\":\"café\""));
    }

    public void testCjkCharsAreHandledBySlowPath() {
        assertThat(render(entry().with("alpha", "用户")), equalTo(", \"alpha\":\"用户\""));
    }

    public void testEmojiIsHandledBySlowPath() {
        // U+1F389 (🎉) is a supplementary char; both Java surrogate halves are > 0x7E
        assertThat(render(entry().with("alpha", "party 🎉")), equalTo(", \"alpha\":\"party 🎉\""));
    }

    public void testSingleUnsafeCharInStringTriggersSlowPathForWholeString() {
        // One unsafe char anywhere causes the entire string to go through quoteAsString
        assertThat(render(entry().with("alpha", "safe\nmiddle")), equalTo(", \"alpha\":\"safe\\nmiddle\""));
    }

    public void testStringArrayWithSpecialChars() {
        assertThat(render(entry().with("beta", new Object[] { "a\"b", "c\\d" })), equalTo(", \"beta\":[\"a\\\"b\",\"c\\\\d\"]"));
    }

    public void testStringArrayWithUnicodeElements() {
        assertThat(render(entry().with("beta", new Object[] { "ñoño", "用户" })), equalTo(", \"beta\":[\"ñoño\",\"用户\"]"));
    }

    public void testStringArrayWithEmojiElement() {
        assertThat(render(entry().with("beta", new Object[] { "flag 🏴" })), equalTo(", \"beta\":[\"flag 🏴\"]"));
    }

    public void testFormatToProducesSameResultOnRepeatedCalls() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "x").with("delta", "y");
        final StringBuilder sb1 = new StringBuilder();
        acc.formatTo(sb1);
        final StringBuilder sb2 = new StringBuilder();
        acc.formatTo(sb2);
        assertThat(sb1.toString(), equalTo(sb2.toString()));
    }

    public void testGetFormattedMessageReturnsCachedInstanceAfterFormatTo() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "cached");
        // prime the memoized rendered field
        acc.formatTo(new StringBuilder());
        // both calls hit the fast path and must return the same String instance
        assertThat(acc.getFormattedMessage(), sameInstance(acc.getFormattedMessage()));
    }

    public void testGetFormattedMessageConsistentWithFormatTo() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "msg").with("delta", "d");
        final StringBuilder sb = new StringBuilder();
        acc.formatTo(sb);
        assertThat(acc.getFormattedMessage(), equalTo(sb.toString()));
    }

    public void testGetFormatReturnsEmptyString() {
        assertThat(entry().getFormat(), equalTo(""));
    }

    public void testGetParametersReturnsEmptyArray() {
        assertThat(entry().getParameters().length, equalTo(0));
    }

    public void testGetThrowableReturnsNull() {
        assertThat(entry().getThrowable(), nullValue());
    }

    public void testGetDataIsEmptyWhenNoFieldsSet() {
        assertThat(entry().getData(), anEmptyMap());
    }

    public void testGetDataKeyOrderIsAlphabetical() {
        // getData uses a TreeMap, so keys come back sorted regardless of FORMAT slot order
        final FastLogEntryAccumulator acc = entry().with("delta", "d").with("alpha", "a").with("gamma", "{}");
        assertThat(List.copyOf(acc.getData().keySet()), contains("alpha", "delta", "gamma"));
    }

    public void testGetDataContainsSetValues() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "a").with("gamma", "{}");
        final SortedMap<String, Object> data = acc.getData();
        assertThat(data.get("alpha"), equalTo("a"));
        assertThat(data.get("gamma"), equalTo("{}"));
    }

    public void testGetDataExcludesUnsetFields() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "a");
        assertThat(acc.getData(), aMapWithSize(1));
    }

    public void testGetDataExcludesRemovedField() {
        final FastLogEntryAccumulator acc = entry().with("alpha", "a").with("delta", "d");
        acc.remove("alpha");
        final SortedMap<String, Object> data = acc.getData();
        assertThat(data, aMapWithSize(1));
        assertThat(data.containsKey("alpha"), is(false));
        assertThat(data.get("delta"), equalTo("d"));
    }

    public void testCommonFieldsArePrePopulated() {
        final FastLogEntryAccumulator acc = new FastLogEntryAccumulator(FORMAT, Map.of("alpha", "common"));
        assertThat(acc.get("alpha"), equalTo("common"));
        assertThat(render(acc), equalTo(", \"alpha\":\"common\""));
    }

    public void testCommonFieldCanBeOverridden() {
        final FastLogEntryAccumulator acc = new FastLogEntryAccumulator(FORMAT, Map.of("alpha", "common"));
        acc.with("alpha", "override");
        assertThat(acc.get("alpha"), equalTo("override"));
        assertThat(render(acc), equalTo(", \"alpha\":\"override\""));
    }

    public void testUnknownCommonFieldsAreIgnored() {
        final FastLogEntryAccumulator acc = new FastLogEntryAccumulator(FORMAT, Map.of("nonexistent", "value"));
        assertThat(render(acc), equalTo(""));
    }
}
