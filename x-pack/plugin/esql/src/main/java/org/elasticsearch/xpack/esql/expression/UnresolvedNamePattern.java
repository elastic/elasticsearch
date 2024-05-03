/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.UnresolvedNamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

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
    public int hashCode() {
        return Objects.hash(super.hashCode(), pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            UnresolvedNamePattern ua = (UnresolvedNamePattern) obj;
            return Objects.equals(pattern, ua.pattern);
        }
        return false;
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
