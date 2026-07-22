/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string.regex;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isStringAndExact;

/**
 * A temporary placeholder for a {@code LIKE} or {@code RLIKE} predicate whose pattern is a
 * non-literal constant expression (e.g. {@code WHERE field LIKE CONCAT("prefix", "*")}).
 * <p>
 * Unlike {@link WildcardLike}/{@link RLike}, this node has <em>two children</em>: the field (LHS)
 * and the pattern expression (RHS). Having both as children ensures the standard analyzer tree
 * walk resolves the pattern expression through the normal reference/function resolution rules.
 * <p>
 * After analysis the optimizer's {@code PropagateEvalFoldables} and {@code ConstantFolding} rules
 * run on the pattern child. A subsequent optimizer rule ({@code ReplaceUnresolvedRegex}) then
 * replaces this node with a concrete {@link WildcardLike} or {@link RLike} once the pattern is
 * foldable to a string. Any node that survives to post-optimization verification is reported as
 * an error via {@link #postOptimizationVerification}.
 */
public class UnresolvedRegexExpression extends Expression implements PostOptimizationVerificationAware {

    /** Which regex operator this placeholder represents. */
    public enum Variant {
        LIKE,
        RLIKE
    }

    private final Expression field;
    private final Expression patternExpression;
    private final Variant variant;

    public UnresolvedRegexExpression(Source source, Expression field, Expression patternExpression, Variant variant) {
        super(source, List.of(field, patternExpression));
        this.field = field;
        this.patternExpression = patternExpression;
        this.variant = variant;
    }

    public Expression field() {
        return field;
    }

    public Expression patternExpression() {
        return patternExpression;
    }

    public Variant variant() {
        return variant;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    /**
     * Validates the field (LHS) type at analysis time, mirroring {@link WildcardLike}/{@link RLike}
     * (see {@code RegexMatch#resolveType}), so that a non-string field is rejected the same way for a
     * constant-expression pattern as for a literal one. The pattern (RHS) type/foldability is not known
     * until after optimizer folding and is checked in {@link #postOptimizationVerification}.
     */
    @Override
    protected TypeResolution resolveType() {
        return isStringAndExact(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    /**
     * Validates the pattern after the optimizer has run constant folding and eval propagation.
     * Any {@code UnresolvedRegexExpression} that survives to this point means the optimizer's
     * {@code ReplaceUnresolvedRegex} rule could not convert it: the pattern folds to a non-string
     * type, is not foldable (e.g. a field reference), or folds to null.
     */
    @Override
    public void postOptimizationVerification(Failures failures) {
        String opName = variant.name();
        if (DataType.isString(patternExpression.dataType()) == false) {
            failures.add(
                Failure.fail(
                    patternExpression,
                    "[{}] pattern must be a string, found [{}]",
                    opName,
                    patternExpression.dataType().typeName()
                )
            );
            return;
        }
        if (patternExpression.foldable() == false) {
            failures.add(
                Failure.fail(
                    patternExpression,
                    "[{}] pattern must be a constant, received [{}]",
                    opName,
                    Expressions.name(patternExpression)
                )
            );
            return;
        }
        // Pattern is foldable and string-typed but evaluates to null
        if (patternExpression.fold(FoldContext.small()) == null) {
            failures.add(Failure.fail(patternExpression, "[{}] pattern must not be null", opName));
        }
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public Object fold(FoldContext ctx) {
        throw new UnsupportedOperationException("UnresolvedRegexExpression cannot be folded directly");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("UnresolvedRegexExpression should not be serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("UnresolvedRegexExpression should not be serialized");
    }

    @Override
    protected NodeInfo<UnresolvedRegexExpression> info() {
        return NodeInfo.create(this, UnresolvedRegexExpression::new, field, patternExpression, variant);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new UnresolvedRegexExpression(source(), newChildren.get(0), newChildren.get(1), variant);
    }
}
