/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string.regex;

import org.apache.lucene.search.MultiTermQuery.RewriteMethod;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPatternList;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.ExpressionQuery;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WildcardLikeList extends RegexMatch<WildcardPatternList> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WildcardLikeList",
        WildcardLikeList::new
    );

    Supplier<Automaton> automatonSupplier = new Supplier<>() {
        Automaton cached;

        @Override
        public Automaton get() {
            if (cached == null) {
                cached = pattern().createAutomaton(caseInsensitive());
            }
            return cached;
        }
    };

    Supplier<CharacterRunAutomaton> characterRunAutomatonSupplier = new Supplier<>() {
        CharacterRunAutomaton cached;

        @Override
        public CharacterRunAutomaton get() {
            if (cached == null) {
                cached = new CharacterRunAutomaton(automatonSupplier.get());
            }
            return cached;
        }
    };

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
        if (supportsPushdown(pushdownPredicates.minTransportVersion())) {
            return pushdownPredicates.isPushableAttribute(field()) ? Translatable.YES : Translatable.NO;
        } else {
            // The ExpressionQuery we use isn't serializable to all nodes in the cluster.
            return Translatable.NO;
        }
    }

    /**
     * Returns a {@link Query} that matches the field against the provided patterns.
     * For now, we only support a single pattern in the list for pushdown.
     */
    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var field = field();
        LucenePushdownPredicates.checkIsPushableAttribute(field);
        String targetFieldName = handler.nameOf(field instanceof FieldAttribute fa ? fa.exactAttribute() : field);
        return translateField(targetFieldName);
    }

    private boolean supportsPushdown(TransportVersion version) {
        return version == null || version.onOrAfter(TransportVersions.ESQL_FIXED_INDEX_LIKE);
    }

    @Override
    public org.apache.lucene.search.Query asLuceneQuery(
        MappedFieldType fieldType,
        RewriteMethod constantScoreRewrite,
        SearchExecutionContext context
    ) {
        return fieldType.automatonQuery(
            automatonSupplier,
            characterRunAutomatonSupplier,
            constantScoreRewrite,
            context,
            getLuceneQueryDescription()
        );
    }

    private String getLuceneQueryDescription() {
        // we use the information used to create the automaton to describe the query here
        String patternDesc = pattern().patternList().stream().map(WildcardPattern::pattern).collect(Collectors.joining("\", \""));
        return "LIKE(\"" + patternDesc + "\"), caseInsensitive=" + caseInsensitive();
    }

    /**
     * Translates the field to a {@link WildcardQuery} using the first pattern in the list.
     * Throws an {@link IllegalArgumentException} if the pattern list contains more than one pattern.
     */
    private Query translateField(String targetFieldName) {
        return new ExpressionQuery(source(), targetFieldName, this);
    }
}
