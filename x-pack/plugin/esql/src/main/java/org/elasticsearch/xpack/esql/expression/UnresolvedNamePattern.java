/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedNamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * Unresolved expression for encapsulating a pattern:
 * KEEP `a*`, b*, `c*`*`d*`
 * a* is an actual name (UnresolvedAttribute)
 * b* is a name pattern (this class)
 * `c*`*`d*` is a name pattern
 */
public class UnresolvedNamePattern extends UnresolvedNamedExpression {

    private final CharacterRunAutomaton automaton;
    private final String pattern;
    // string representation without backquotes
    // Cannot rely on NamedExpression.name: the UnresolvedNamedExpression superclass throws on name()
    // and stores "<unresolved>" as the internal name field.
    private final String actualName;

    public UnresolvedNamePattern(Source source, CharacterRunAutomaton automaton, String patternString, String name) {
        super(source, emptyList());
        this.automaton = automaton;
        this.pattern = patternString;
        this.actualName = name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    public boolean match(String string) {
        return automaton.run(string);
    }

    // override because the super class throws
    @Override
    public String name() {
        return actualName;
    }

    public String pattern() {
        return pattern;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    protected NodeInfo<UnresolvedNamePattern> info() {
        return NodeInfo.create(this, UnresolvedNamePattern::new, automaton, pattern, actualName);
    }

    @Override
    public String unresolvedMessage() {
        return "Unresolved pattern [" + pattern + "]";
    }

    public static String errorMessage(String pattern, List<String> potentialMatches) {
        String msg = "No matches found for pattern [" + pattern + "]";
        if (CollectionUtils.isEmpty(potentialMatches) == false) {
            msg += ", did you mean to match "
                + (potentialMatches.size() == 1 ? "[" + potentialMatches.get(0) + "]" : "any of " + potentialMatches)
                + "?";
        }
        return msg;
    }

    @Override
    public Nullability nullable() {
        throw new UnresolvedException("nullable", this);
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(true), pattern, actualName);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (UnresolvedNamePattern) o;
        return super.innerEquals(other, true) && Objects.equals(pattern, other.pattern) && Objects.equals(actualName, other.actualName);
    }

    /**
     * Renders a wildcard-style pattern (SQL {@code LIKE} / shell {@code KEEP *foo*}) preserving
     * the metacharacters {@code *}, {@code ?}, {@code %}, {@code _} verbatim; each literal run
     * between metacharacters routes through {@code mapper.column}. Backslash escapes the next
     * character (treated as literal).
     */
    public static void rewriteWildcardPattern(StringBuilder sb, String pattern, NodeStringMapper mapper) {
        if (pattern == null || pattern.isEmpty()) {
            return;
        }
        StringBuilder run = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < pattern.length()) {
                run.append(pattern.charAt(++i));
                continue;
            }
            if (c == '*' || c == '?' || c == '%' || c == '_') {
                if (run.length() > 0) {
                    sb.append(mapper.column(run.toString()));
                    run.setLength(0);
                }
                sb.append(c);
            } else {
                run.append(c);
            }
        }
        if (run.length() > 0) {
            sb.append(mapper.column(run.toString()));
        }
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + pattern;
    }

}
