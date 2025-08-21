/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.lucene.HighlighterExpressionEvaluator;
import org.elasticsearch.compute.lucene.LuceneQueryEvaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightSnippetUtils;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.esql.capabilities.RewriteableAware;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.querydsl.query.TranslationAwareExpressionQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction.fieldAsFieldAttribute;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction.getNameFromFieldAttribute;

/**
 * Extract snippets function, that extracts the most relevant snippets from a given input string
 */
public class ExtractSnippets extends EsqlScalarFunction
    implements
        TwoOptionalArguments,
        RewriteableAware,
        TranslationAware,
        EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ExtractSnippets",
        ExtractSnippets::new
    );

    private static final int DEFAULT_NUM_SNIPPETS = 1;
    // TODO: Determine good default, set artificially low for POC purposes
    private static final int DEFAULT_SNIPPET_LENGTH = 10;

    private final Expression field, str, numSnippets, snippetLength;
    private final QueryBuilder queryBuilder;

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        description = """
            Extracts the most relevant snippets to return from a given input string""",
        examples = @Example(file = "keyword", tag = "extract_snippets")
    )
    public ExtractSnippets(
        Source source,
        @Param(name = "field", type = { "keyword" }, description = "The input string") Expression field,
        @Param(name = "str", type = { "keyword", "text" }, description = "The input string") Expression str,
        @Param(
            optional = true,
            name = "num_snippets",
            type = { "integer" },
            description = "The number of snippets to return. Defaults to " + DEFAULT_NUM_SNIPPETS
        ) Expression numSnippets,
        @Param(
            optional = true,
            name = "snippet_length",
            type = { "integer" },
            description = "The length of snippets to return. Defaults to " + DEFAULT_SNIPPET_LENGTH
        ) Expression snippetLength
    ) {
        this(source, field, str, numSnippets, snippetLength, new MatchQueryBuilder(field.sourceText(), str.sourceText()));
    }

    public ExtractSnippets(
        Source source,
        Expression field,
        Expression str,
        Expression numSnippets,
        Expression snippetLength,
        QueryBuilder queryBuilder
    ) {
        super(source, fields(field, str, numSnippets, snippetLength));
        this.field = field;
        this.str = str;
        this.numSnippets = numSnippets;
        this.snippetLength = snippetLength;
        this.queryBuilder = queryBuilder;
    };

    public ExtractSnippets(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(QueryBuilder.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(str);
        out.writeOptionalNamedWriteable(numSnippets);
        out.writeOptionalNamedWriteable(snippetLength);
        out.writeOptionalNamedWriteable(queryBuilder);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return field.dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(field(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isString(str(), sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = numSnippets() == null
            ? TypeResolution.TYPE_RESOLVED
            : isType(numSnippets(), dt -> dt == DataType.INTEGER, sourceText(), THIRD, "integer");
        if (resolution.unresolved()) {
            return resolution;
        }

        return snippetLength() == null
            ? TypeResolution.TYPE_RESOLVED
            : isType(snippetLength(), dt -> dt == DataType.INTEGER, sourceText(), FOURTH, "integer");
    }

    @Override
    public boolean foldable() {
        return field().foldable()
            && str().foldable()
            && (numSnippets() == null || numSnippets().foldable())
            && (snippetLength() == null || snippetLength().foldable());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ExtractSnippets(
            source(),
            newChildren.get(0), // field
            newChildren.get(1), // str
            numSnippets == null ? null : newChildren.get(2),
            snippetLength == null ? null : newChildren.get(3),
            queryBuilder
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ExtractSnippets::new, field, str, numSnippets, snippetLength, queryBuilder);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<EsPhysicalOperationProviders.ShardContext> shardContexts = toEvaluator.shardContexts();
        LuceneQueryEvaluator.ShardConfig[] shardConfigs = new LuceneQueryEvaluator.ShardConfig[shardContexts.size()];

        int numSnippets = this.numSnippets == null ? DEFAULT_NUM_SNIPPETS : (Integer) this.numSnippets.fold(FoldContext.small());
        int snippetSize = this.snippetLength == null ? DEFAULT_SNIPPET_LENGTH : (Integer) this.snippetLength.fold(FoldContext.small());

        int i = 0;
        for (EsPhysicalOperationProviders.ShardContext shardContext : shardContexts) {
            SearchExecutionContext searchExecutionContext = shardContext.searchExecutionContext();
            SearchContext searchContext = shardContext.searchContext();
            if (searchContext == null) {
                throw new IllegalStateException("Missing search context, cannot extract snippets");
            }

            try {
                // We need to call rewrite here, to ensure we rewrite on both coordinator and data nodes.
                assert queryBuilder != null : "ExtractSnippets missing required state";
                QueryBuilder rewritten = Rewriteable.rewrite(queryBuilder, searchExecutionContext);
                SearchHighlightContext highlightContext = HighlightSnippetUtils.buildSearchHighlightContextForSnippets(
                    searchExecutionContext,
                    fieldName(),
                    numSnippets,
                    snippetSize,
                    rewritten
                );
                searchContext.highlight(highlightContext);

            } catch (IOException e) {
                throw new RuntimeException(
                    "Failed to create highlight context for field ["
                        + fieldName()
                        + "], str ["
                        + searchString()
                        + "], numSnippets: ["
                        + numSnippets
                        + "], snippetLength: ["
                        + snippetLength
                        + "]",
                    e
                );
            }

            shardConfigs[i++] = new LuceneQueryEvaluator.ShardConfig(shardContext.toQuery(queryBuilder), shardContext.searcher());
        }
        // Get field name and search context from the first shard context
        SearchContext firstSearchContext = shardContexts.isEmpty() ? null : shardContexts.getFirst().searchContext();
        Map<String, Highlighter> highlighters = firstSearchContext == null ? Map.of() : firstSearchContext.highlighters();
        return new HighlighterExpressionEvaluator.Factory(
            shardConfigs,
            fieldName(),
            numSnippets,
            snippetSize,
            firstSearchContext,
            highlighters
        );
    }

    @Override
    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new ExtractSnippets(source(), field, str, numSnippets, snippetLength, queryBuilder);
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // We don't want pushdown for this function, as it is not a filter query
        return Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return queryBuilder != null
            ? new TranslationAwareExpressionQuery(source(), queryBuilder())
            : translate(pushdownPredicates, handler);
    }

    private Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        Object query = str().fold(FoldContext.small());
        // Make query lenient so mixed field types can be queried when a field type is incompatible with the value provided
        return new MatchQuery(source(), fieldName(), query, Map.of(MatchQueryBuilder.LENIENT_FIELD.getPreferredName(), true));
    }

    Expression field() {
        return field;
    }

    private String fieldName() {
        var fieldAttribute = fieldAsFieldAttribute(field());
        Check.notNull(fieldAttribute, "Highlight must have a field attribute as the first argument");
        return getNameFromFieldAttribute(fieldAttribute);
    }

    Expression str() {
        return str;
    }

    private String searchString() {
        var strAttribute = fieldAsFieldAttribute(str());
        Check.notNull(strAttribute, "Highlight must have a str attribute as the second argument");
        return getNameFromFieldAttribute(strAttribute);
    }

    Expression numSnippets() {
        return numSnippets;
    }

    Expression snippetLength() {
        return snippetLength;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ExtractSnippets extractSnippets = (ExtractSnippets) o;
        return Objects.equals(field(), extractSnippets.field())
            && Objects.equals(str(), extractSnippets.str())
            && Objects.equals(numSnippets(), extractSnippets.numSnippets())
            && Objects.equals(snippetLength(), extractSnippets.snippetLength())
            && Objects.equals(queryBuilder(), extractSnippets.queryBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), str(), numSnippets(), snippetLength(), queryBuilder());
    }

    private static List<Expression> fields(Expression field, Expression str, Expression numSnippets, Expression snippetLength) {
        List<Expression> list = new ArrayList<>(4);
        list.add(field);
        list.add(str);
        if (numSnippets != null) {
            list.add(numSnippets);
            if (snippetLength != null) {
                list.add(snippetLength);
            }
        }
        return list;
    }
}
