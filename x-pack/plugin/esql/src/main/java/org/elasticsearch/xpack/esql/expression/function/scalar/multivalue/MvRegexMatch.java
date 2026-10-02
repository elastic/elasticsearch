/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;

/**
 * Shared base for the any-value pattern-matching functions {@link MvLike} (wildcard) and {@link MvRLike} (regex).
 * Both answer "does <em>any</em> value of a (possibly multivalued) field match this constant pattern", both are
 * two-valued ({@link Nullability#FALSE} — a null or empty field yields {@code false}, never {@code null}, so the
 * predicate composes through {@code AND}/{@code OR}/{@code NOT}), and both push to a bare Lucene multi-term query
 * that is existential over the field's terms.
 * <p>
 * This mirrors how the scalar operators {@code WildcardLike}/{@code RLike} share {@code RegexMatch}: the two
 * grammars compile to different Lucene queries and different evaluators, but the type-resolution, two-valued
 * folding, and pushdown gating are identical, so they live here once. Subclasses supply the pattern grammar
 * ({@link #validatePattern}), the evaluator ({@link #buildEvaluator}), and the pushed query ({@link #asQuery}).
 */
public abstract class MvRegexMatch extends BinaryScalarFunction
    implements
        EvaluatorMapper,
        TranslationAware,
        PostOptimizationVerificationAware {

    protected MvRegexMatch(Source source, Expression field, Expression pattern) {
        super(source, field, pattern);
    }

    protected MvRegexMatch(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        // Types only. A null-typed field is a valid signature (the whole predicate folds to false, per the two-valued
        // contract — a null field has no value to match). The pattern must be a string type. A null literal pattern is
        // type-compatible and passes here; whether the pattern is a *constant*, and whether it is a single, non-null,
        // well-formed pattern, is checked in postOptimizationVerification — after constant folding and propagation — so
        // expressions that fold to a constant (CONCAT("a","*"), a ROW var) are accepted. (A constant field folds the
        // whole predicate before that verifier; patternString() re-checks on that path so it fails loudly too.)
        if (left().dataType() != DataType.NULL) {
            TypeResolution resolution = isString(left(), sourceText(), FIRST);
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return isString(right(), sourceText(), SECOND);
    }

    /**
     * The pattern must be a single, non-null, well-formed constant string. This runs after the logical optimizer has
     * folded and propagated constants, so {@code CONCAT("a","*")} or a constant {@code ROW} variable are accepted here
     * even though they are not literals at analysis time — a normal expression should allow the full expressiveness of
     * the language. A pattern that is not foldable, folds to null, folds to a multivalue, or is malformed is a mistake
     * in the query (the pattern is author-supplied, not data-derived) and fails loudly rather than silently matching
     * nothing.
     */
    @Override
    public final void postOptimizationVerification(Failures failures) {
        Failure notConstant = isFoldable(right(), sourceText(), SECOND);
        if (notConstant != null) {
            failures.add(notConstant);
            return;
        }
        String pattern;
        try {
            pattern = patternString(); // throws on a null or multivalue constant
        } catch (InvalidArgumentException e) {
            failures.add(Failure.fail(right(), e.getMessage()));
            return;
        }
        // Build the pattern so a malformed or over-complex one is caught here rather than as a planner crash.
        // AbstractStringPattern.createAutomaton converts a determinize blow-up (TooComplexToDeterminize) into
        // IllegalArgumentException, so the two exception types cover every validatePattern failure.
        try {
            validatePattern(pattern);
        } catch (InvalidArgumentException | IllegalArgumentException e) {
            failures.add(Failure.fail(right(), "invalid pattern [{}] for [{}]: {}", pattern, sourceText(), e.getMessage()));
        }
    }

    /**
     * The folded pattern as a single, non-null string, or throws {@link InvalidArgumentException} if it folds to null
     * or a multivalue. It validates rather than assuming a prior check: a constant field folds the whole predicate
     * (through {@link #toEvaluator}) <em>before</em> {@link #postOptimizationVerification} runs, so without the checks
     * here a null pattern would NPE and a multivalue pattern would silently match nothing. {@code postOptimization-}
     * {@code Verification} turns the throw into a clean {@link Failures} entry; the constant-fold path lets it surface
     * as a query error.
     */
    protected final String patternString() {
        Object folded = right().fold(FoldContext.small());
        if (folded == null) {
            throw new InvalidArgumentException("second argument of [{}] must not be null", sourceText());
        }
        if (folded instanceof BytesRef == false && folded instanceof String == false) {
            throw new InvalidArgumentException(
                "second argument of [{}] must be a single pattern string, found value [{}]",
                sourceText(),
                folded
            );
        }
        return BytesRefs.toString(folded);
    }

    @Override
    public final DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public final Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public final Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public final ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // Validate the pattern here, and before the null-field short-circuit below. A constant field folds the whole
        // predicate through here before postOptimizationVerification runs, so this path cannot assume the pattern was
        // already checked — and a null-typed field would otherwise fold to constant false before the pattern is even
        // looked at, silently accepting an author error. patternString() throws the same shape error the verifier
        // surfaces on a null or multivalue pattern; validatePattern's malformed-pattern error is reframed to match the
        // verifier's. Either way an author-error pattern fails loudly on every path, regardless of the field.
        String pattern = patternString();
        try {
            validatePattern(pattern);
        } catch (InvalidArgumentException | IllegalArgumentException e) {
            throw new InvalidArgumentException(e, "invalid pattern [{}] for [{}]: {}", pattern, sourceText(), e.getMessage());
        }
        // A null-typed field has no values to match, so the predicate is constant false.
        if (left().dataType() == DataType.NULL) {
            return ConstantEvaluators.CONSTANT_FALSE_FACTORY;
        }
        return buildEvaluator(toEvaluator, pattern);
    }

    /**
     * The exactness argument, shared by both functions: a Lucene multi-term query on a keyword field is inherently
     * existential — it matches a document iff <em>some</em> indexed term of the field matches. That is this predicate's
     * definition verbatim (any value matches, and a missing field has no terms and so does not match, agreeing with the
     * two-valued contract), so the bare query <em>is</em> the predicate: {@link Translatable#YES}, the filter is
     * dropped, and {@code must_not} is an exact negation.
     * <p>
     * Nothing wraps the query in {@code SingleValueQuery}. That wrap gives single-value scalars their null-on-multivalue
     * semantics and would restrict the match to single-valued documents — the bug these functions exist to avoid. The
     * avoidance is structural: this class implements plain {@link TranslationAware}, never
     * {@link TranslationAware.SingleValueTranslationAware}, so {@code TranslatorHandler.asQuery} can never apply the wrap.
     * <p>
     * One caveat inherited from the scalar operators {@code WildcardLike}/{@code RLike}: ES|QL reports a keyword field
     * with a {@code normalizer} as pushable (the index resolver hard-codes {@code normalized = false}). On such a field
     * the pushed query can normalize the pattern server-side while the evaluator matches the raw pattern against the
     * normalized doc value, so the two disagree — most clearly for wildcard matching, where the keyword field type
     * normalizes the pushed pattern. This is not corrected here; it is the same shared upstream limitation the scalar
     * operators carry.
     */
    @Override
    public final Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // text is excluded outright, even where an exact `.keyword` subfield exists. The analyzed field's terms are
        // tokens rather than whole values, and routing to the subfield inherits that subfield's ignore_above hole:
        // values the subfield ignored are matched by the evaluator but invisible to the pushed query — an under-match
        // that RECHECK cannot repair, since a recheck can only drop surfaced rows, never restore missing ones.
        if (left().dataType() == DataType.TEXT) {
            return Translatable.NO;
        }
        // The pattern is a valid non-null constant string here (postOptimizationVerification guarantees it); a subclass
        // may still decline to push a particular pattern (MvLike refuses the empty wildcard).
        if (patternPushable(patternString()) == false) {
            return Translatable.NO;
        }
        return pushdownPredicates.isPushableFieldAttribute(left()) ? Translatable.YES : Translatable.NO;
    }

    /**
     * The pushed field name. TEXT is already excluded by {@link #translatable}, so the field is always an exact keyword
     * and no {@code exactAttribute()} redirection is needed.
     */
    protected final String pushdownFieldName(TranslatorHandler handler) {
        LucenePushdownPredicates.checkIsPushableAttribute(left());
        return handler.nameOf(left());
    }

    /**
     * Validate the folded pattern, throwing {@link InvalidArgumentException} or {@link IllegalArgumentException} if it
     * is malformed or too complex to determinize. Called only with a non-null scalar pattern string.
     */
    protected abstract void validatePattern(String pattern);

    /** Whether a given (present, non-null) pattern can be pushed to Lucene. Defaults to true; {@link MvLike} refuses the empty pattern. */
    protected boolean patternPushable(String pattern) {
        return true;
    }

    /** Build the per-row evaluator for a present, non-null pattern. */
    protected abstract ExpressionEvaluator.Factory buildEvaluator(ToEvaluator toEvaluator, String pattern);
}
