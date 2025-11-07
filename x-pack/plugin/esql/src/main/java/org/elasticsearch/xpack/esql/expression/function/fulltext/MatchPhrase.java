/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MatchPhraseQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.index.query.MatchPhraseQueryBuilder.SLOP_FIELD;
import static org.elasticsearch.index.query.MatchPhraseQueryBuilder.ZERO_TERMS_QUERY_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Full text function that performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MatchPhraseQuery} .
 */
public class MatchPhrase extends SingleFieldFullTextFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MatchPhrase",
        MatchPhrase::readFrom
    );
    public static final Set<DataType> FIELD_DATA_TYPES = Set.of(KEYWORD, TEXT, NULL);
    public static final Set<DataType> QUERY_DATA_TYPES = Set.of(KEYWORD, TEXT);

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(ANALYZER_FIELD.getPreferredName(), KEYWORD),
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(SLOP_FIELD.getPreferredName(), INTEGER),
        entry(ZERO_TERMS_QUERY_FIELD.getPreferredName(), KEYWORD)
    );

    @FunctionInfo(
        returnType = "boolean",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.1.0") },
        description = """
            Use `MATCH_PHRASE` to perform a [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) on the
            specified field.
            Using `MATCH_PHRASE` is equivalent to using the `match_phrase` query in the Elasticsearch Query DSL.""",
        detailedDescription = """
            MatchPhrase can be used on <<text, text>> fields, as well as other field types like keyword, boolean, or date types.
            MatchPhrase is not supported for <<semantic-text, semantic_text>> or numeric types.

            MatchPhrase can use <<esql-function-named-params,function named parameters>> to specify additional options for the
            match_phrase query.
            All [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query parameters are supported.

            `MATCH_PHRASE` returns true if the provided query matches the row.""",
        examples = { @Example(file = "match-phrase-function", tag = "match-phrase-with-field", applies_to = "stack: ga 9.1.0") }
    )
    public MatchPhrase(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "Field that the query will target.") Expression field,
        @Param(name = "query", type = { "keyword" }, description = "Value to find in the provided field.") Expression matchPhraseQuery,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "analyzer",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Analyzer used to convert the text in the query value into token. Defaults to the index-time analyzer"
                        + " mapped for the field. If no analyzer is mapped, the index’s default analyzer is used."
                ),
                @MapParam.MapParamEntry(
                    name = "slop",
                    type = "integer",
                    valueHint = { "1" },
                    description = "Maximum number of positions allowed between matching tokens. Defaults to 0."
                        + " Transposed terms have a slop of 2."
                ),
                @MapParam.MapParamEntry(
                    name = "zero_terms_query",
                    type = "keyword",
                    valueHint = { "none", "all" },
                    description = "Indicates whether all documents or none are returned if the analyzer removes all tokens, such as "
                        + "when using a stop filter. Defaults to none."
                ),
                @MapParam.MapParamEntry(
                    name = "boost",
                    type = "float",
                    valueHint = { "2.5" },
                    description = "Floating point number used to decrease or increase the relevance scores of the query. Defaults to 1.0."
                ) },
            description = "(Optional) MatchPhrase additional options as <<esql-function-named-params,function named parameters>>."
                + " See [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) for more information.",
            optional = true
        ) Expression options
    ) {
        this(source, field, matchPhraseQuery, options, null);
    }

    public MatchPhrase(Source source, Expression field, Expression matchPhraseQuery, Expression options, QueryBuilder queryBuilder) {
        super(
            source,
            field,
            matchPhraseQuery,
            options,
            options == null ? List.of(field, matchPhraseQuery) : List.of(field, matchPhraseQuery, options),
            queryBuilder
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String functionName() {
        return ENTRY.name;
    }

    private static MatchPhrase readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        return new MatchPhrase(source, field, query, null, queryBuilder);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeOptionalNamedWriteable(queryBuilder());
    }

    @Override
    protected Set<DataType> getFieldDataTypes() {
        return FIELD_DATA_TYPES;
    }

    @Override
    protected Set<DataType> getQueryDataTypes() {
        return QUERY_DATA_TYPES;
    }

    @Override
    protected Map<String, DataType> getAllowedOptions() {
        return ALLOWED_OPTIONS;
    }

    private Map<String, Object> matchPhraseQueryOptions() throws InvalidArgumentException {
        if (options() == null) {
            return Map.of();
        }

        Map<String, Object> matchPhraseOptions = new HashMap<>();
        Options.populateMap((MapExpression) options(), matchPhraseOptions, source(), SECOND, ALLOWED_OPTIONS);
        return matchPhraseOptions;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MatchPhrase::new, field(), query(), options(), queryBuilder());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MatchPhrase(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.size() > 2 ? newChildren.get(2) : null,
            queryBuilder()
        );
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new MatchPhrase(source(), field, query(), options(), queryBuilder);
    }

    @Override
    protected Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fieldAttribute = fieldAsFieldAttribute();
        Check.notNull(fieldAttribute, "MatchPhrase must have a field attribute as the first argument");
        String fieldName = getNameFromFieldAttribute(fieldAttribute);
        return new MatchPhraseQuery(source(), fieldName, queryAsObject(), matchPhraseQueryOptions());
    }

}
