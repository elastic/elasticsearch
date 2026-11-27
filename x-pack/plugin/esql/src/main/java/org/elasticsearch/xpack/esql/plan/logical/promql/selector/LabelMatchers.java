/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeStringRenderable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;

/**
 * Immutable collection of label matchers for a PromQL selector.
 */
public class LabelMatchers implements NodeStringRenderable {
    /**
     * Empty label matchers for literal selectors and other cases with no label constraints.
     */
    public static final LabelMatchers EMPTY = new LabelMatchers(emptyList());

    private final List<LabelMatcher> labelMatchers;
    private final Map<String, LabelMatcher> nameToMatcher;

    public LabelMatchers(List<LabelMatcher> labelMatchers) {
        Objects.requireNonNull(labelMatchers, "label matchers cannot be null");
        this.labelMatchers = labelMatchers;
        int size = labelMatchers.size();
        if (size == 0) {
            nameToMatcher = emptyMap();
        } else {
            nameToMatcher = Maps.newLinkedHashMapWithExpectedSize(size);
            for (LabelMatcher lm : labelMatchers) {
                nameToMatcher.put(lm.name(), lm);
            }
        }
    }

    public List<LabelMatcher> matchers() {
        return labelMatchers;
    }

    public LabelMatcher nameLabel() {
        return nameToMatcher.get(LabelMatcher.NAME);
    }

    public boolean isEmpty() {
        return labelMatchers.isEmpty();
    }

    /**
     * Lowers these matchers into an AND of per-label ES|QL predicates over {@code fields} - the label fields in matcher
     * order, where a metric name written as an identifier has none. Uses {@link AutomatonUtils} to lower a
     * pattern to a predicate cheaper than a regex where possible: exact values become equality/IN, prefix/suffix
     * alternations become STARTS_WITH/ENDS_WITH disjunctions, everything else falls back to an RE2 regex match. Null when there is
     * nothing to filter on.
     */
    public Expression predicate(Source source, List<Expression> fields, Configuration configuration) {
        // Prometheus resolves these to `cpu_util{host="..."}`:
        // `{__name__="cpu_util", host="..."}`
        // `{"cpu_util", host="..."}`
        // Their name matcher has a label field like every other matcher; in `cpu_util{...}` the name matcher has none.
        int fieldOffset = labelMatchers.size() - fields.size();
        assert fieldOffset == 0 || (fieldOffset == 1 && LabelMatcher.NAME.equals(labelMatchers.getFirst().name()))
            : "invariant: label fields " + fields + " must align with matchers " + labelMatchers;
        List<Expression> conditions = new ArrayList<>(labelMatchers.size());
        for (int i = 0, s = labelMatchers.size(); i < s; i++) {
            LabelMatcher matcher = labelMatchers.get(i);
            // the metric name matcher selects the series; it has no label field to filter on
            if (LabelMatcher.NAME.equals(matcher.name())) {
                continue;
            }
            Expression field = fields.get(i - fieldOffset);
            if (field.resolved() && DataType.isString(field.dataType()) == false) {
                field = new ToString(field.source(), field, configuration);
            }
            conditions.add(condition(source, field, matcher));
        }
        return conditions.isEmpty() ? null : combineAnd(conditions);
    }

    /** Lowers a single matcher to an ES|QL predicate over {@code field}; also used by the prometheus REST layer. */
    public static Expression condition(Source source, Expression field, LabelMatcher matcher) {
        if (matcher.matchesAll()) {
            return Literal.fromBoolean(source, true);
        }
        if (matcher.matchesNone()) {
            return Literal.fromBoolean(source, false);
        }
        Expression condition;
        if (matcher.isMultiValue()) {
            // each value is a regex, combine with OR; plain literals match exact with an IN clause
            condition = matcher.matcher().isRegex()
                ? Predicates.combineOr(
                    matcher.values().stream().<Expression>map(v -> new Re2Like(source, field, new Re2Pattern(v))).toList()
                )
                : new In(source, field, matcher.values().stream().<Expression>map(v -> Literal.keyword(source, v)).toList());
            if (matcher.isNegation()) {
                condition = new Not(source, condition);
            }
        } else {
            var exact = AutomatonUtils.matchesExact(matcher.automaton());
            if (exact != null) {
                condition = new Equals(source, field, Literal.keyword(source, exact));
            } else {
                // the value of = and != is a literal, not a regex; a regex of literal alternatives, as in a|b, matches as a set
                var values = matcher.matcher().isRegex()
                    ? AutomatonUtils.literalAlternatives(matcher.getFirstValue())
                    : List.of(matcher.getFirstValue());
                condition = values != null
                    ? new In(source, field, values.stream().<Expression>map(v -> Literal.keyword(source, v)).toList())
                    // any other regex is an RE2 match over the full pattern, anchored per PromQL semantics
                    : new Re2Like(source, field, new Re2Pattern(matcher.getFirstValue()));
                if (matcher.isNegation()) {
                    condition = new Not(source, condition);
                }
            }
        }
        // absent labels are treated as having value "" because if the matcher accepts the empty string
        // (e.g. {label=""} or {label!="foo"}), series where the label field is NULL (absent) must also match.
        if (matcher.matchesEmpty()) {
            condition = Predicates.combineOr(List.of(new IsNull(source, field), condition));
        }
        return condition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(labelMatchers);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LabelMatchers other = (LabelMatchers) obj;
        return Objects.equals(labelMatchers, other.labelMatchers);
    }

    @Override
    public String toString() {
        return labelMatchers.toString();
    }

    /**
     * Renders the matcher list shape ({@code [m1, m2]}, matching {@code List.toString()}) with each
     * matcher routed through the mapper, so label names + match values tokenize under anonymization
     * while identity rendering stays byte-identical.
     */
    @Override
    public void nodeString(StringBuilder sb, Node.NodeStringFormat format, NodeStringMapper mapper) {
        sb.append('[');
        boolean first = true;
        for (LabelMatcher m : labelMatchers) {
            if (first == false) {
                sb.append(", ");
            }
            first = false;
            m.nodeString(sb, format, mapper);
        }
        sb.append(']');
    }
}
