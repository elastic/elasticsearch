/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.expression.function.Options.resolve;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.emitChunks;

public class TopSnippets extends EsqlScalarFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TopSnippets",
        TopSnippets::new
    );

    static final int DEFAULT_NUM_SNIPPETS = 5;
    static final int DEFAULT_WORD_SIZE = 300;

    private final Expression field, query, options;

    private static final String NUM_SNIPPETS = "num_snippets";
    private static final String NUM_WORDS = "num_words";

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(NUM_SNIPPETS, DataType.INTEGER),
        entry(NUM_WORDS, DataType.INTEGER)
    );

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        returnType = "keyword",
        preview = true,
        description = "Use `TOP_SNIPPETS` to extract the best snippets for a given query string from a text field.",
        detailedDescription = """
                TopSnippets can be used on fields from the text famiy like <<text, text>> and <<semantic-text, semantic_text>>.
                TopSnippets will extract the best snippets for a given query string.
            """,
        examples = {
            @Example(file = "top-snippets", tag = "top-snippets", applies_to = "stack: preview 9.3.0"),
            @Example(file = "top-snippets", tag = "top-snippets-with-options", applies_to = "stack: preview 9.3.0") }
    )
    public TopSnippets(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "The input to chunk.") Expression field,
        @Param(name = "query", type = { "keyword" }, description = """
            The input text containing only query terms for snippet extraction.
            Lucene query syntax, operators, and wildcards are not allowed.
            """) Expression query,
        @MapParam(
            name = "options",
            description = "Options to customize snippet extraction behavior.",
            optional = true,
            params = {
                @MapParam.MapParamEntry(
                    name = "num_snippets",
                    type = { "integer" },
                    description = "The maximum number of matching snippets to return.",
                    valueHint = { "3" }
                ),
                @MapParam.MapParamEntry(name = "num_words", type = { "integer" }, description = """
                    The maximum number of words to return in each snippet.
                    This allows better control of inference costs by limiting the size of tokens per snippet.
                    """, valueHint = { "300" }) }
        ) Expression options
    ) {
        super(source, options == null ? List.of(field, query) : List.of(field, query, options));
        this.field = field;
        this.query = query;
        this.options = options;
    }

    public TopSnippets(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(query);
        out.writeOptionalNamedWriteable(options);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isString(field(), sourceText(), FIRST).and(() -> isString(query(), sourceText(), SECOND))
            .and(() -> resolve(options(), source(), THIRD, ALLOWED_OPTIONS))
            .and(this::validateOptions);
    }

    private TypeResolution validateOptions() {
        if (options() == null) {
            return TypeResolution.TYPE_RESOLVED;
        }
        MapExpression optionsMap = (MapExpression) options();
        return validateOptionValueIsPositiveInteger(optionsMap, NUM_SNIPPETS).and(
            validateOptionValueIsPositiveInteger(optionsMap, NUM_WORDS)
        );
    }

    private TypeResolution validateOptionValueIsPositiveInteger(MapExpression optionsMap, String paramName) {
        Expression expr = optionsMap.keyFoldedMap().get(paramName);
        if (expr != null) {
            Object value = expr.fold(FoldContext.small());
            if (value != null && ((Number) value).intValue() <= 0) {
                return new TypeResolution(
                    "'" + paramName + "' option must be a positive integer, found [" + ((Number) value).intValue() + "]"
                );
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return field().foldable() && query().foldable() && (options() == null || options().foldable());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TopSnippets(
            source(),
            newChildren.get(0), // field
            newChildren.get(1), // query
            newChildren.size() > 2 ? newChildren.get(2) : null // options
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TopSnippets::new, field, query, options);
    }

    Expression field() {
        return field;
    }

    Expression query() {
        return query;
    }

    Expression options() {
        return options;
    }

    private int numSnippets() {
        return extractIntegerOption(NUM_SNIPPETS, DEFAULT_NUM_SNIPPETS);
    }

    private int numWords() {
        return extractIntegerOption(NUM_WORDS, DEFAULT_WORD_SIZE);
    }

    private int extractIntegerOption(String option, int defaultValue) {
        if (options == null) {
            return defaultValue;
        }

        MapExpression optionsMap = (MapExpression) options;
        Expression expr = optionsMap.keyFoldedMap().get(option);
        if (expr == null) {
            return defaultValue;
        }
        Object value = expr.fold(FoldContext.small());
        return value != null ? ((Number) value).intValue() : defaultValue;
    }

    @Evaluator(extraName = "BytesRef")
    static void process(
        BytesRefBlock.Builder builder,
        BytesRef str,
        BytesRef query,
        @Fixed ChunkingSettings chunkingSettings,
        @Fixed MemoryIndexChunkScorer scorer,
        @Fixed int numSnippets
    ) {
        String content = str.utf8ToString();
        String queryString = query.utf8ToString();

        List<String> chunks = chunkText(content, chunkingSettings);
        List<MemoryIndexChunkScorer.ScoredChunk> scoredChunks = scorer.scoreChunks(chunks, queryString, numSnippets, false);
        List<String> snippets = scoredChunks.stream().map(MemoryIndexChunkScorer.ScoredChunk::content).limit(numSnippets).toList();
        emitChunks(builder, snippets);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        TopSnippets chunk = (TopSnippets) o;
        return Objects.equals(field, chunk.field) && Objects.equals(query, chunk.query) && Objects.equals(options, chunk.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, query, options);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {

        int numSnippets = numSnippets();
        int numWords = numWords();
        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(numWords, 0);
        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();

        return new TopSnippetsBytesRefEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            toEvaluator.apply(query),
            chunkingSettings,
            scorer,
            numSnippets
        );
    }
}
