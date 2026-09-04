/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

/**
 * Any-value wildcard matching over a multivalued field: returns {@code true} when <em>any</em> value of
 * {@code field} matches {@code pattern}.
 * <p>
 * {@code LIKE} is a single-value scalar — applied to a multivalued field it yields {@code null} rather than reducing
 * to any-value semantics. This is the reduction, and it is the semantics a Lucene {@code wildcard} query already has
 * over a multivalued field: an index matches a document when any value of the field matches. The shared base
 * {@link MvRegexMatch} carries the two-valued contract, type resolution, and pushdown; this class supplies the
 * wildcard grammar, the affix fast paths, and the {@code wildcard} query.
 */
public class MvLike extends MvRegexMatch {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvLike", MvLike::new);

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvLike.class).binary(MvLike::new).name("mv_like");

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks if any value of a multivalue field matches a wildcard pattern.",
        description = """
            Returns `true` when any value yielded by `field` matches `pattern`, using the same wildcard syntax as `LIKE`.
            `LIKE` is a single-value scalar: applied to a multivalue field it emits a warning and returns `null`, so this
            is how you match a wildcard pattern against a multivalue field. A null or empty `field` returns `false`, and
            because the result is never null the predicate composes under `AND`/`OR`/`NOT`.""",
        examples = {
            @Example(file = "string", tag = "mv_like"),
            @Example(
                description = "A prefix pattern matches when any value starts with the literal:",
                file = "string",
                tag = "mv_like_prefix"
            ),
            @Example(description = "Because the result is never null, it composes under `NOT`:", file = "string", tag = "mv_like_not") },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") }
    )
    public MvLike(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text" },
            description = "Multivalue expression to test. If null or empty, the function returns `false`."
        ) Expression field,
        @Param(
            name = "pattern",
            type = { "keyword", "text" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Wildcard pattern. Must be a constant. `*` matches any run of characters, `?` matches a single "
                + "character; escape either with `\\`."
        ) Expression pattern
    ) {
        super(source, field, pattern);
    }

    private MvLike(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected MvLike replaceChildren(Expression newLeft, Expression newRight) {
        return new MvLike(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvLike::new, left(), right());
    }

    @Override
    protected void validatePattern(String pattern) {
        // The constructor force-validates the escape syntax; building the automaton additionally surfaces an
        // over-complex pattern (determinize work-limit) as an analysis-time error rather than a late data-node crash,
        // matching mv_rlike. createAutomaton converts TooComplexToDeterminizeException to IllegalArgumentException.
        new WildcardPattern(pattern).createAutomaton(false);
    }

    /** The empty pattern matches only the empty string; it cannot be pushed (see {@link #buildEvaluator}), so it is not pushable. */
    @Override
    protected boolean patternPushable(String pattern) {
        return pattern.isEmpty() == false;
    }

    @Override
    protected ExpressionEvaluator.Factory buildEvaluator(ToEvaluator toEvaluator, String patternString) {
        ExpressionEvaluator.Factory field = toEvaluator.apply(left());
        if (patternString.isEmpty()) {
            // The empty pattern accepts the empty string — same special case RegexMatch.toEvaluator makes, so a value
            // matches mv_like exactly when it would match LIKE. A Lucene wildcard query built from an empty pattern
            // matches no term, which is why patternPushable() refuses it and this evaluator answers instead.
            return MvAutomataMatch.toEvaluator(source(), field, Automata.makeEmptyString());
        }
        /*
         * Affix-shaped patterns skip the automaton entirely, mirroring WildcardLike.toEvaluator. The saving is larger
         * here than for LIKE: the matcher runs once per *value*, not once per row, so replacing a per-byte
         * RunAutomaton.step walk with a byte compare (or a SIMD substring scan) is paid for on every value of every
         * multivalued position. `prefix*` is also the shape the DSL `prefix` clause compiles to, so the fast path lands
         * on the consumer's hottest case.
         *
         * WildcardLike delegates these to StartsWith/EndsWith; mv_like cannot, because those are single-value scalars
         * that null out on a multivalued field — the exact behaviour this function exists to avoid. So the reduction is
         * open-coded here over the same ByteMatchers primitives.
         */
        WildcardPattern pattern = new WildcardPattern(patternString);
        return switch (pattern.shape()) {
            case WildcardPattern.Shape.Prefix(String prefix) -> MvLikeAffixMatch.toEvaluator(
                source(),
                field,
                MvLikeAffixMatch.Shape.PREFIX,
                new BytesRef(prefix)
            );
            case WildcardPattern.Shape.Suffix(String suffix) -> MvLikeAffixMatch.toEvaluator(
                source(),
                field,
                MvLikeAffixMatch.Shape.SUFFIX,
                new BytesRef(suffix)
            );
            case WildcardPattern.Shape.Contains(String literal) -> MvLikeAffixMatch.toEvaluator(
                source(),
                field,
                MvLikeAffixMatch.Shape.CONTAINS,
                new BytesRef(literal)
            );
            case WildcardPattern.Shape.General ignored -> MvAutomataMatch.toEvaluator(source(), field, pattern.createAutomaton(false));
        };
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return new WildcardQuery(
            source(),
            pushdownFieldName(handler),
            new WildcardPattern(patternString()).asLuceneWildcard(),
            false,
            pushdownPredicates.flags().stringLikeOnIndex()
        );
    }
}
