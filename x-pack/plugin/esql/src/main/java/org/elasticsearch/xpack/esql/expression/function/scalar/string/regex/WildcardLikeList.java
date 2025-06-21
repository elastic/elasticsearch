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
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPatternList;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

public class WildcardLikeList extends RegexMatch<WildcardPatternList> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WildcardLikeList",
        WildcardLikeList::new
    );

    /**
     * The documentation for this function is in WildcardLike, and shown to the users `LIKE` in the docs.
     */
    public WildcardLikeList(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "A literal expression.") Expression left,
        @Param(name = "pattern", type = { "keyword", "text" }, description = "Pattern.") WildcardPatternList patterns
    ) {
        this(source, left, patterns, false);
    }

    public WildcardLikeList(Source source, Expression left, WildcardPatternList patterns, boolean caseInsensitive) {
        super(source, left, patterns, caseInsensitive);
    }

    public WildcardLikeList(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            new WildcardPatternList(in),
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
    protected NodeInfo<WildcardLikeList> info() {
        return NodeInfo.create(this, WildcardLikeList::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected WildcardLikeList replaceChild(Expression newLeft) {
        return new WildcardLikeList(source(), newLeft, pattern(), caseInsensitive());
    }

    /**
     * Returns {@link Translatable#YES} if the field is pushable, otherwise {@link Translatable#NO}.
     * For now, we only support a single pattern in the list for pushdown.
     */
    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        if (pattern().patternList().size() != 1) {
            // we only support a single pattern in the list for pushdown for now
            return Translatable.NO;
        }
        return pushdownPredicates.isPushableAttribute(field()) ? Translatable.YES : Translatable.NO;

    }

    /**
     * Returns a {@link Query} that matches the field against the provided patterns.
     * For now, we only support a single pattern in the list for pushdown.
     */
    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var field = field();
        LucenePushdownPredicates.checkIsPushableAttribute(field);
        return translateField(handler.nameOf(field instanceof FieldAttribute fa ? fa.exactAttribute() : field));
    }

    /**
     * Translates the field to a {@link WildcardQuery} using the first pattern in the list.
     * Throws an {@link IllegalArgumentException} if the pattern list contains more than one pattern.
     */
    private Query translateField(String targetFieldName) {
        if (pattern().patternList().size() != 1) {
            throw new IllegalArgumentException("WildcardLikeList can only be translated when it has a single pattern");
        }
        return new WildcardQuery(source(), targetFieldName, pattern().patternList().get(0).asLuceneWildcard(), caseInsensitive());
    }
}
