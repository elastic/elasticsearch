/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvRegexMatch;

/**
 * The range a prefix-bearing {@code mv_like} / {@code mv_rlike} implies, handed to a dataset reader beside the pattern
 * so the reader can skip the row groups or stripes whose statistics cannot hold a match.
 * <p>
 * Every value matching {@code abc*xyz} (or {@code abc.*xyz}) starts with {@code abc}, so it lies in {@code [abc, abd)}.
 * A pattern has no statistics form, a range does, and {@code mv_in_range} is the range with the pattern functions'
 * semantics: any-value and two-valued. So on every row, multivalued or not, the pattern implies the bound, and the
 * bound is only ever a pruning hint beside the pattern, never in its place.
 * <p>
 * This is derived when pushing to an external source rather than rewritten into the logical plan, where the scalar
 * {@code LIKE} gets its {@code STARTS_WITH}: on an Elasticsearch keyword field with a normalizer, the pushed range has
 * its bounds normalized, which can turn {@code [prefix, next(prefix))} into an empty range and drop documents the
 * normalized wildcard query matches.
 */
public final class PatternPrefixBound {

    private PatternPrefixBound() {}

    /**
     * The half-open {@code mv_in_range(field, prefix, next(prefix))} that {@code expr} implies, or {@code null} when
     * {@code expr} is not an {@code mv_like} / {@code mv_rlike} over a string attribute, its pattern has no literal
     * prefix, or the prefix has no upper bound (every code point in it is the maximal one).
     */
    @Nullable
    public static MvInRange of(Expression expr) {
        if (expr instanceof MvRegexMatch == false) {
            return null;
        }
        MvRegexMatch match = (MvRegexMatch) expr;
        if (match.left() instanceof Attribute == false || DataType.isString(match.left().dataType()) == false) {
            return null;
        }
        String prefix = match.patternPrefix();
        if (prefix == null) {
            return null;
        }
        BytesRef lower = new BytesRef(prefix);
        BytesRef upper = StringPrefixUtils.nextPrefixUpperBound(lower);
        if (upper == null) {
            return null;
        }
        Source source = match.source();
        return MvInRange.halfOpen(
            source,
            match.left(),
            new Literal(source, lower, DataType.KEYWORD),
            new Literal(source, upper, DataType.KEYWORD)
        );
    }
}
