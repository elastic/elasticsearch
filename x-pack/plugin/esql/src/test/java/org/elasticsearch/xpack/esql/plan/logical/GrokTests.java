/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.anonymizer.AnonymizationContext;
import org.elasticsearch.xpack.esql.core.tree.Source;

import static org.hamcrest.Matchers.containsString;

public class GrokTests extends ESTestCase {

    public void testWatchdogInterruptsBacktracking() {
        // With timeout 0 the watchdog fires immediately after the first check.
        Grok.Parser parser = Grok.pattern(Source.EMPTY, "(?<a>a+)+b", MatcherWatchdog.newInstance(0));
        RuntimeException ex = expectThrows(RuntimeException.class, () -> parser.grok().match("aaaaaaaaX"));
        assertThat(ex.getMessage(), containsString("interrupted"));
    }

    public void testNoopWatchdogDoesNotInterruptSimplePattern() {
        Grok.Parser parser = Grok.pattern(Source.EMPTY, "%{IP:client_ip}", MatcherWatchdog.noop());
        assertNotNull(parser.grok().captures("192.168.1.1"));
    }

    public void testRewriteGrokPatternKeepsLibraryIdentifierTokenizesCaptureName() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        // The Grok library identifier (IP) is not customer data — keep verbatim. The capture name
        // (client_ip) is user-defined — tokenize via the column map.
        StringBuilder sb = new StringBuilder();
        Grok.rewriteGrokPattern(sb, "%{IP:client_ip}", ctx.mapper());
        String out = sb.toString();
        assertTrue("expected library id preserved + capture tokenized: " + out, out.matches("%\\{IP:col_[0-9a-f]+\\}"));
    }

    public void testRewriteGrokPatternKeepsTypeCoercionSuffix() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        // `:int` is a type coercion suffix, not customer data; survives the rewrite.
        StringBuilder sb = new StringBuilder();
        Grok.rewriteGrokPattern(sb, "%{NUMBER:bytes:int}", ctx.mapper());
        String out = sb.toString();
        assertTrue("expected library id + tokenized capture + type suffix: " + out, out.matches("%\\{NUMBER:col_[0-9a-f]+:int\\}"));
    }

    public void testRewriteGrokPatternHandlesMultipleCapturesAndSeparators() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        StringBuilder sb = new StringBuilder();
        Grok.rewriteGrokPattern(sb, "%{IP:client_ip} - %{NUMBER:bytes} OK", ctx.mapper());
        String out = sb.toString();
        assertTrue("expected mixed structure preserved: " + out, out.matches("%\\{IP:col_[0-9a-f]+\\} - %\\{NUMBER:col_[0-9a-f]+\\} OK"));
    }

    public void testRewriteGrokPatternNullAndEmptyAreNoop() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        StringBuilder sb = new StringBuilder();
        Grok.rewriteGrokPattern(sb, null, ctx.mapper());
        assertEquals("", sb.toString());
        Grok.rewriteGrokPattern(sb, "", ctx.mapper());
        assertEquals("", sb.toString());
    }
}
