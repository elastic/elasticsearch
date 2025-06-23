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
    private final String name;

    public UnresolvedNamePattern(Source source, CharacterRunAutomaton automaton, String patternString, String name) {
        super(source, emptyList());
        this.automaton = automaton;
        this.pattern = patternString;
        this.name = name;
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

    @Override
    public String name() {
        return name;
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
        return NodeInfo.create(this, UnresolvedNamePattern::new, automaton, pattern, name);
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
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), pattern);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (UnresolvedNamePattern) o;
        return super.innerEquals(other) && Objects.equals(pattern, other.pattern);
    }

    @Override
    public String nodeString() {
        return toString();
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + pattern;
    }
}
