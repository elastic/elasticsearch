/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.anonymizer.AnonymizationContext;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

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

    /**
     * A wide input followed by a long chain of DISSECTs used to recompute every node's output on each {@code output()} call, which made
     * planning O(width * depth^2) per pass.
     */
    public void testWideDeepChainOutputIsCached() {
        int width = 2_000;
        int depth = 450;
        List<Alias> fields = new ArrayList<>(width + 1);
        fields.add(new Alias(EMPTY, "s", Literal.keyword(EMPTY, "a")));
        for (int i = 0; i < width; i++) {
            fields.add(new Alias(EMPTY, "c" + i, Literal.integer(EMPTY, 1)));
        }
        Row row = new Row(EMPTY, fields);
        Attribute input = row.output().get(0);

        List<LogicalPlan> chain = new ArrayList<>(depth + 1);
        LogicalPlan plan = row;
        chain.add(plan);
        for (int i = 0; i < depth; i++) {
            String pattern = "%{k" + i + "}";
            Dissect.Parser parser = new Dissect.Parser(pattern, "", new DissectParser(pattern, ""));
            plan = new Dissect(EMPTY, plan, input, parser, List.of(new ReferenceAttribute(EMPTY, "k" + i, KEYWORD)));
            chain.add(plan);
        }

        assertEquals(width + 1 + depth, plan.output().size());
        for (LogicalPlan node : chain) {
            assertSame(node.output(), node.output());
        }
    }
}
