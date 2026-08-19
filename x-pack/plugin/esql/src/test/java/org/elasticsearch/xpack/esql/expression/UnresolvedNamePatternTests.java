/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.anonymizer.AnonymizationContext;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class UnresolvedNamePatternTests extends AbstractNamedExpressionSerializationTests<UnresolvedNamePattern> {
    @Override
    protected UnresolvedNamePattern createTestInstance() {
        // No automaton, this is normally injected during parsing and is derived from the pattern.
        return new UnresolvedNamePattern(Source.EMPTY, null, randomAlphaOfLength(3), randomAlphaOfLength(3));
    }

    @Override
    protected UnresolvedNamePattern mutateInstance(UnresolvedNamePattern instance) {
        Source source = instance.source();
        String name = instance.name();
        String pattern = instance.pattern();
        switch (between(0, 1)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLength(4));
            case 1 -> pattern = randomValueOtherThan(pattern, () -> randomAlphaOfLength(4));
        }
        return new UnresolvedNamePattern(source, null, pattern, name);
    }

    @Override
    protected UnresolvedNamePattern mutateNameId(UnresolvedNamePattern instance) {
        // Creating a new instance is enough as the NameId is generated automatically.
        return new UnresolvedNamePattern(instance.source(), null, instance.pattern(), instance.name());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return true;
    }

    @Override
    protected UnresolvedNamePattern copyInstance(UnresolvedNamePattern instance, TransportVersion version) throws IOException {
        // Doesn't escape the node
        return new UnresolvedNamePattern(instance.source(), null, instance.pattern(), instance.name());
    }

    public void testRewriteWildcardPatternKeepsStarsTokenizesRuns() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        // The two literal runs ("foo" and "bar") tokenize through the column map; the metacharacters
        // survive verbatim. Same name → same token within a context.
        StringBuilder sb = new StringBuilder();
        UnresolvedNamePattern.rewriteWildcardPattern(sb, "*foo*bar*", ctx.mapper());
        String out = sb.toString();
        assertTrue("wildcard stars dropped: " + out, out.matches("\\*col_[0-9a-f]+\\*col_[0-9a-f]+\\*"));
        // foo and bar tokenize to different col_ tokens
        assertNotEquals(ctx.mapper().column("foo"), ctx.mapper().column("bar"));
    }

    public void testRewriteWildcardPatternHandlesAllMetacharacters() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        // ?, %, _, * all survive as structural metacharacters
        StringBuilder sb = new StringBuilder();
        UnresolvedNamePattern.rewriteWildcardPattern(sb, "a?b%c_d*e", ctx.mapper());
        String out = sb.toString();
        assertTrue(
            "expected ? % _ * preserved: " + out,
            out.matches("col_[0-9a-f]+\\?col_[0-9a-f]+%col_[0-9a-f]+_col_[0-9a-f]+\\*col_[0-9a-f]+")
        );
    }

    public void testRewriteWildcardPatternBackslashEscape() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        // Escaped `\*` is treated as a literal char and folds into the literal run that wraps it,
        // not as a wildcard metacharacter.
        StringBuilder sb = new StringBuilder();
        UnresolvedNamePattern.rewriteWildcardPattern(sb, "foo\\*bar", ctx.mapper());
        String out = sb.toString();
        assertTrue("escaped backslash should not surface as wildcard: " + out, out.matches("col_[0-9a-f]+"));
    }

    public void testRewriteWildcardPatternNullAndEmptyAreNoop() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        StringBuilder sb = new StringBuilder();
        UnresolvedNamePattern.rewriteWildcardPattern(sb, null, ctx.mapper());
        assertEquals("", sb.toString());
        UnresolvedNamePattern.rewriteWildcardPattern(sb, "", ctx.mapper());
        assertEquals("", sb.toString());
    }
}
