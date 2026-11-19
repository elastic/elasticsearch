/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RegexQuery;
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
 * Any-value regular-expression matching over a multivalued field: returns {@code true} when <em>any</em> value of
 * {@code field} matches {@code pattern}.
 * <p>
 * The regex counterpart of {@link MvLike}, and the second half of what the out-of-band Query DSL filter needs: a
 * {@code regexp} clause is a different grammar compiling to a different Lucene query than {@code wildcard}, so it gets
 * its own function rather than a mode flag. The shared base {@link MvRegexMatch} carries everything the two functions
 * have in common; this class supplies the regex grammar, the automaton evaluator, and the {@code regexp} query.
 * <p>
 * There are no affix fast paths: unlike a wildcard pattern, a regular expression has no prefix/suffix/contains shape to
 * peel off, so every pattern runs the automaton. There is likewise no empty-pattern special case — {@code RegExp("")}
 * already accepts exactly the empty string, matching what a pushed {@code regexp} query does, so the two agree.
 */
public class MvRLike extends MvRegexMatch {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvRLike", MvRLike::new);

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvRLike.class).binary(MvRLike::new).name("mv_rlike");

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks if any value of a multivalue field matches a regular expression.",
        description = """
            Returns `true` when any value yielded by `field` matches `pattern`, using the same regular-expression syntax
            as `RLIKE`. The pattern must match a value in full, as with `RLIKE`. `RLIKE` is a single-value scalar:
            applied to a multivalue field it emits a warning and returns `null`, so this is how you match a regular
            expression against a multivalue field. A null or empty `field` returns `false`, and because the result is
            never null the predicate composes under `AND`/`OR`/`NOT`.""",
        examples = {
            @Example(file = "string", tag = "mv_rlike"),
            @Example(description = "Character classes work as they do in `RLIKE`:", file = "string", tag = "mv_rlike_class") },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") }
    )
    public MvRLike(
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
            description = "Regular expression. Must be a constant. The pattern must match a value in full, as with `RLIKE`."
        ) Expression pattern
    ) {
        super(source, field, pattern);
    }

    private MvRLike(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected MvRLike replaceChildren(Expression newLeft, Expression newRight) {
        return new MvRLike(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvRLike::new, left(), right());
    }

    @Override
    protected void validatePattern(String pattern) {
        // RLikePattern's constructor does not parse the regex — RegExp is only built inside doCreateAutomaton — so the
        // automaton is built here to turn an unparseable or over-complex pattern into an analysis-time error.
        new RLikePattern(pattern).createAutomaton(false);
    }

    @Override
    protected ExpressionEvaluator.Factory buildEvaluator(ToEvaluator toEvaluator, String patternString) {
        return MvAutomataMatch.toEvaluator(source(), toEvaluator.apply(left()), new RLikePattern(patternString).createAutomaton(false));
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        // RLikePattern is Lucene RegExp syntax already, and asJavaRegex returns it unchanged, so the pushed query
        // compiles the same pattern language the evaluator's automaton does.
        return new RegexQuery(source(), pushdownFieldName(handler), new RLikePattern(patternString()).asJavaRegex(), false);
    }
}
