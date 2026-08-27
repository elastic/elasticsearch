/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.anonymizer.AnonymizationContext;

public class DissectTests extends ESTestCase {

    public void testRewriteDissectPatternTokenizesCaptureNames() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        // The %{...} braces survive; the capture names inside route through the column map; the
        // separator characters between captures pass through unchanged.
        StringBuilder sb = new StringBuilder();
        Dissect.rewriteDissectPattern(sb, "%{ip} - %{date} %{message}", ctx.mapper());
        String out = sb.toString();
        assertTrue(
            "expected braces preserved + capture names tokenized + separators kept: " + out,
            out.matches("%\\{col_[0-9a-f]+\\} - %\\{col_[0-9a-f]+\\} %\\{col_[0-9a-f]+\\}")
        );
    }

    public void testRewriteDissectPatternKeepsAppendAndSkipModifiers() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        // Dissect supports `+` (append) and `?` (skip) modifiers right after the open brace. These
        // are structural, not data — preserve the modifier; tokenize the rest of the name.
        StringBuilder sb = new StringBuilder();
        Dissect.rewriteDissectPattern(sb, "%{?skip} %{+append}", ctx.mapper());
        String out = sb.toString();
        assertTrue("expected ? and + modifiers preserved: " + out, out.matches("%\\{\\?col_[0-9a-f]+\\} %\\{\\+col_[0-9a-f]+\\}"));
    }

    public void testRewriteDissectPatternNullAndEmptyAreNoop() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        StringBuilder sb = new StringBuilder();
        Dissect.rewriteDissectPattern(sb, null, ctx.mapper());
        assertEquals("", sb.toString());
        Dissect.rewriteDissectPattern(sb, "", ctx.mapper());
        assertEquals("", sb.toString());
    }
}
