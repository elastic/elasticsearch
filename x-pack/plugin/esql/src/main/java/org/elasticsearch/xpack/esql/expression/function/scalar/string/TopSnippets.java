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
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
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
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                `TOP_SNIPPETS` can be used on fields from the text famiy like <<text, text>> and <<semantic-text, semantic_text>>.
                `TOP_SNIPPETS` will extract the best snippets for a given query string.
            """,
        examples = {
            @Example(file = "top-snippets", tag = "top-snippets-with-field", applies_to = "stack: preview 9.3.0"),
            @Example(file = "top-snippets", tag = "top-snippets-with-options", applies_to = "stack: preview 9.3.0") }
    )
    public TopSnippets(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text" },
            description = "The field to extract snippets from. The input can be a single-valued"
                + " or multi-valued field. In the case of a multi-valued argument,"
                + " snippets are extracted from each value separately."
        ) Expression field,
        @Param(
            name = "query",
            type = { "keyword" },
            description = "The input text containing only query terms for snippet extraction."
                + " Lucene query syntax, operators, and wildcards are not allowed."
        ) Expression query,
        @MapParam(
            name = "options",
            description = "(Optional) `TOP_SNIPPETS` additional options as "
                + "[function named parameters](/reference/query-languages/esql/esql-syntax.md#esql-function-named-params).",
            optional = true,
            params = {
                @MapParam.MapParamEntry(
                    name = "num_snippets",
                    type = "integer",
                    description = "The maximum number of matching snippets to return.",
                    valueHint = { "3" }
                ),
                @MapParam.MapParamEntry(name = "num_words", type = "integer", description = """
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
            .and(() -> resolve(options(), source(), THIRD, ALLOWED_OPTIONS, TopSnippets::validateOptions));
    }

    private static void validateOptions(Map<String, Object> options) {
        validateOptionValueIsPositiveInteger(options, NUM_SNIPPETS);
        validateOptionValueIsPositiveInteger(options, NUM_WORDS);
    }

    private static void validateOptionValueIsPositiveInteger(Map<String, Object> options, String paramName) {
        Object value = options.get(paramName);
        if (value != null && ((Number) value).intValue() <= 0) {
            throw new InvalidArgumentException("'{}' option must be a positive integer, found [{}]", paramName, value);
        }
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

    private int numSnippets(Map<String, Object> options) {
        return extractIntegerOption(options, NUM_SNIPPETS, DEFAULT_NUM_SNIPPETS);
    }

    private int numWords(Map<String, Object> options) {
        return extractIntegerOption(options, NUM_WORDS, DEFAULT_WORD_SIZE);
    }

    private int extractIntegerOption(Map<String, Object> options, String option, int defaultValue) {
        Object value = options.get(option);
        return value != null ? ((Number) value).intValue() : defaultValue;
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static void process(
        BytesRefBlock.Builder builder,
        @Position int position,
        BytesRefBlock field,
        BytesRefBlock query,
        @Fixed ChunkingSettings chunkingSettings,
        @Fixed MemoryIndexChunkScorer scorer,
        @Fixed int numSnippets
    ) {
        int valueCount = field.getValueCount(position);
        if (valueCount == 0) {
            builder.appendNull();
            return;
        }

        // Get query value (should be single-valued)
        int queryValueCount = query.getValueCount(position);
        if (queryValueCount == 0) {
            builder.appendNull();
            return;
        }
        if (queryValueCount > 1) {
            throw new IllegalArgumentException("single-value function encountered multi-value");
        }

        BytesRef scratch = new BytesRef();
        BytesRef queryValue = query.getBytesRef(query.getFirstValueIndex(position), scratch);
        String queryString = queryValue.utf8ToString();

        int firstValueIndex = field.getFirstValueIndex(position);

        // Collect scored chunks from all values and return the top N overall
        ArrayList<ScoredChunk> allScoredChunks = new ArrayList<>();

        for (int i = 0; i < valueCount; i++) {
            BytesRef value = field.getBytesRef(firstValueIndex + i, scratch);
            String content = value.utf8ToString();

            List<String> chunks = chunkText(content, chunkingSettings);
            allScoredChunks.addAll(scorer.scoreChunks(chunks, queryString, numSnippets, false));
        }

        List<String> snippets = allScoredChunks.stream()
            .sorted(Comparator.comparing(ScoredChunk::score).reversed())
            .map(ScoredChunk::content)
            .limit(numSnippets)
            .toList();

        if (snippets.isEmpty()) {
            builder.appendNull();
            return;
        }

        emitChunks(builder, snippets);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        int numSnippets;
        int numWords;
        if (options != null) {
            Map<String, Object> opts = new HashMap<>();
            Options.populateMap((MapExpression) options, opts, source(), THIRD, ALLOWED_OPTIONS);
            numSnippets = numSnippets(opts);
            numWords = numWords(opts);
        } else {
            numSnippets = DEFAULT_NUM_SNIPPETS;
            numWords = DEFAULT_WORD_SIZE;
        }

        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(numWords, 0);
        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();

        return new TopSnippetsEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            toEvaluator.apply(query),
            chunkingSettings,
            scorer,
            numSnippets
        );
    }
}
