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
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * A temporary placeholder for a {@code LIKE} or {@code RLIKE} predicate whose pattern is a
 * non-literal constant expression (e.g. {@code WHERE field LIKE CONCAT("prefix", "*")}).
 * <p>
 * Unlike {@link WildcardLike}/{@link RLike}, this node has <em>two children</em>: the field (LHS)
 * and the pattern expression (RHS). Having both as children ensures the standard analyzer tree
 * walk resolves the pattern expression through the normal reference/function resolution rules.
 * <p>
 * After analysis the optimizer's {@code PropagateEvalFoldables} and {@code ConstantFolding} rules
 * run on the pattern child. A subsequent optimizer rule ({@code ReplaceDeferredRegex}) then
 * replaces this node with a concrete {@link WildcardLike} or {@link RLike} once the pattern is
 * foldable to a string. Any node that survives to post-optimization verification is reported as
 * an error via {@link #postOptimizationVerification}.
 */
public class DeferredRegexExpression extends Expression implements PostOptimizationVerificationAware {

    /** Which regex operator this placeholder represents. */
    public enum Variant {
        LIKE,
        RLIKE
    }

    private final Expression field;
    private final Expression patternExpression;
    private final Variant variant;

    public DeferredRegexExpression(Source source, Expression field, Expression patternExpression, Variant variant) {
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
     * (see {@code RegexMatch#resolveType}), which uses {@code isString} (not {@code isStringAndExact}).
     * Using {@code isStringAndExact} would reject text fields without a keyword sub-field, which
     * would be accepted by the literal-pattern path. The pattern (RHS) type/foldability is not known
     * until after optimizer folding and is checked in {@link #postOptimizationVerification}.
     */
    @Override
    protected TypeResolution resolveType() {
        return isString(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    /**
     * Validates the pattern after the optimizer has run constant folding and eval propagation.
     * Any {@code DeferredRegexExpression} that survives to this point means the optimizer's
     * {@code ReplaceDeferredRegex} rule could not convert it: the pattern has a non-string type,
     * is not foldable (e.g. a field reference), or the optimizer reduced it to a null literal.
     * <p>
     * The null check uses {@link Expressions#isGuaranteedNull} rather than folding again: after
     * {@code ConstantFolding} has run, any expression that evaluated to null is already represented
     * as a {@code Literal(null)} in the tree, so no new fold is needed here.
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
        if (Expressions.isGuaranteedNull(patternExpression)) {
            failures.add(Failure.fail(patternExpression, "[{}] pattern must not be null", opName));
        } else {
            // Foldable, string-typed, non-null: ReplaceDeferredRegex must have replaced this node.
            // Reaching here indicates an optimizer bug.
            assert false : "BUG: DeferredRegexExpression survived optimization with a foldable non-null string pattern";
            failures.add(Failure.fail(this, "internal error: [{}] pattern was not resolved during optimization", opName));
        }
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public Object fold(FoldContext ctx) {
        throw new UnsupportedOperationException("DeferredRegexExpression cannot be folded directly");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("DeferredRegexExpression should not be serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("DeferredRegexExpression should not be serialized");
    }

    @Override
    protected NodeInfo<DeferredRegexExpression> info() {
        return NodeInfo.create(this, DeferredRegexExpression::new, field, patternExpression, variant);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new DeferredRegexExpression(source(), newChildren.get(0), newChildren.get(1), variant);
    }

    /**
     * Builds a concrete {@link WildcardLike} or {@link RLike} from a folded pattern string.
     * Shared by the parse-time literal fast path and {@code ReplaceDeferredRegex}.
     */
    public static RegexMatch<?> buildRegexMatch(Source source, Expression field, Variant variant, String patternStr) {
        try {
            return switch (variant) {
                case LIKE -> new WildcardLike(source, field, new WildcardPattern(patternStr));
                case RLIKE -> new RLike(source, field, new RLikePattern(patternStr));
            };
        } catch (InvalidArgumentException e) {
            throw new ParsingException(source, "Invalid pattern for {} [{}]: [{}]", variant.name(), patternStr, e.getMessage());
        }
    }
}
