/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string.regex;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePatternList;
import org.elasticsearch.xpack.esql.core.querydsl.query.EsqlAutomatonQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.stream.Collectors;

public class RLikeList extends RegexMatch<RLikePatternList> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RLikeList",
        RLikeList::new
    );

    /**
     * The documentation for this function is in RLike, and shown to the users `RLIKE` in the docs.
     */
    public RLikeList(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "A literal value.") Expression value,
        @Param(name = "patterns", type = { "keyword", "text" }, description = "A list of regular expressions.") RLikePatternList patterns
    ) {
        this(source, value, patterns, false);
    }

    public RLikeList(Source source, Expression field, RLikePatternList rLikePattern, boolean caseInsensitive) {
        super(source, field, rLikePattern, caseInsensitive);
    }

    private RLikeList(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            new RLikePatternList(in),
            deserializeCaseInsensitivity(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        pattern().writeTo(out);
        serializeCaseInsensitivity(out);
    }

    @Override
    public String name() {
        return ENTRY.name;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected RLikeList replaceChild(Expression newChild) {
        return new RLikeList(source(), newChild, pattern(), caseInsensitive());
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableAttribute(field()) ? Translatable.YES : Translatable.NO;
    }


    /**
     * Returns a {@link Query} that matches the field against the provided patterns.
     * For now, we only support a single pattern in the list for pushdown.
     */
    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        //throw new RuntimeException("As query called");
        var field = field();
        LucenePushdownPredicates.checkIsPushableAttribute(field);
        return translateField(handler.nameOf(field instanceof FieldAttribute fa ? fa.exactAttribute() : field));
    }

    private Query translateField(String targetFieldName) {
        return new EsqlAutomatonQuery(source(), targetFieldName, pattern().createAutomaton(caseInsensitive()), getAutomatonDescription());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RLikeList::new, field(), pattern(), caseInsensitive());
    }

    private String getAutomatonDescription() {
        // we use the information used to create the automaton to describe the query here
        String patternDesc = pattern().patternList().stream().map(RLikePattern::pattern).collect(Collectors.joining("\", \""));
        return "RLIKE(\"" + patternDesc + "\"), caseInsensitive=" + caseInsensitive();
    }
}
