/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.lucene.search.uhighlight.CustomPassageFormatter;
import org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.elasticsearch.lucene.search.uhighlight.QueryMaxAnalyzedOffset;
import org.elasticsearch.lucene.search.uhighlight.Snippet;
import org.elasticsearch.xpack.core.common.chunks.MemoryIndexChunkScorer;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
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
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.resolveTypeQuery;
import static org.elasticsearch.xpack.esql.expression.function.Options.resolve;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.emitChunks;

public class TopSnippets extends EsqlScalarFunction implements OptionalArgument, PostOptimizationVerificationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TopSnippets",
        TopSnippets::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TopSnippets.class)
        .ternary(TopSnippets::new)
        .name("top_snippets");

    static final int DEFAULT_NUM_SNIPPETS = 5;
    static final int DEFAULT_WORD_SIZE = 300;

    /**
     * Each chunk is highlighted as a single passage — no further splitting.
     */
    private static final int MAX_PASSAGES_PER_CHUNK = 1;

    private final Expression field, query, options;

    private static final String NUM_SNIPPETS = "num_snippets";
    private static final String NUM_WORDS = "num_words";
    private static final String HIGHLIGHT = "highlight";
    private static final String PRE_TAG = "pre_tag";
    private static final String POST_TAG = "post_tag";
    private static final String ENCODER = "encoder";
    private static final String HTML_ENCODER = "html";
    private static final String ORDER = "order";
    private static final String DOC_ORDER = "none";
    private static final String SCORE_ORDER = "score";

    static final String DEFAULT_PRE_TAG = "<em>";
    static final String DEFAULT_POST_TAG = "</em>";
    static final String DEFAULT_ENCODER = "default";

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(NUM_SNIPPETS, DataType.INTEGER),
        entry(NUM_WORDS, DataType.INTEGER),
        entry(HIGHLIGHT, DataType.BOOLEAN),
        entry(PRE_TAG, DataType.KEYWORD),
        entry(POST_TAG, DataType.KEYWORD),
        entry(ENCODER, DataType.KEYWORD),
        entry(ORDER, DataType.KEYWORD)
    );

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        returnType = "keyword",
        preview = true,
        description = "Use `TOP_SNIPPETS` to extract the best snippets for a given query string from a text field.",
        detailedDescription = """
                `TOP_SNIPPETS` can be used on fields from the text family like <<text, text>> and <<semantic-text, semantic_text>>.
                `TOP_SNIPPETS` will extract the best snippets for a given query string.
            """,
        examples = {
            @Example(file = "top-snippets", tag = "top-snippets-with-field", applies_to = "stack: preview 9.3.0"),
            @Example(file = "top-snippets", tag = "top-snippets-with-options", applies_to = "stack: preview 9.3.0"),
            @Example(file = "rerank", tag = "rerank-top-snippets", applies_to = "stack: preview 9.3.0", explanation = """
                This examples demonstrates how to use `TOP_SNIPPETS` with `RERANK`. By returning a fixed number of snippets with a limited
                size, we have more control over the number of tokens that are used for semantic reranking.
                """),
            @Example(file = "top-snippets", tag = "top-snippets-with-highlighting", applies_to = "stack: preview 9.5.0", explanation = """
                Enable highlighting by setting `highlight` to `true` in the options. This wraps matched query terms in the
                returned snippets with `<em>` tags by default. To use different tags, set the `pre_tag` and `post_tag` options
                to the desired opening and closing tags respectively.
                """),
            @Example(
                file = "top-snippets",
                tag = "top-snippets-num-words-zero-highlight",
                applies_to = "stack: preview 9.5.0",
                explanation = """
                    Set `num_words` to 0 to disable chunking entirely. This keeps the input field values as-is,
                    which is useful when the text has already been chunked. Combine this with `highlight` set to
                    `true` to highlight matched terms within each full value.
                    """
            ),
            @Example(
                file = "top-snippets",
                tag = "top-snippets-num-words-zero-highlight-preceding-chunk",
                applies_to = "stack: preview 9.5.0",
                explanation = """
                        This is another example of setting `num_words` to 0, this time applied to an input that has
                        already been chunked with the `CHUNK` command. The markdown text is chunked by sections first, and
                        because the text is pre-chunked, no further splitting is needed. Setting `num_words` to 0 disables
                        chunking so that each chunk is scored and highlighted individually.
                    """
            ), }
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
                    The maximum number of words to return in each snippet.\n\t
                    {applies_to}`stack: preview 9.4`When set to 0, disables chunking entirely,
                     the input field values are used as-is, which is
                    useful when the text has already been chunked.
                    """, valueHint = { "300" }),
                @MapParam.MapParamEntry(name = "highlight", type = "boolean", description = """
                    When true, wraps matched query terms in the returned snippets with markup tags.
                    Defaults to false.
                    """, valueHint = { "true" }, applies_to = "stack: preview 9.5.0"),
                @MapParam.MapParamEntry(name = "pre_tag", type = "keyword", description = """
                    Opening tag for highlighted terms. Only applies when highlight is true.
                    Defaults to `<em>`.
                    """, valueHint = { "<em>" }, applies_to = "stack: preview 9.5.0"),
                @MapParam.MapParamEntry(name = "post_tag", type = "keyword", description = """
                    Closing tag for highlighted terms. Only applies when highlight is true.
                    Defaults to `</em>`.
                    """, valueHint = { "</em>" }, applies_to = "stack: preview 9.5.0"),
                @MapParam.MapParamEntry(name = "encoder", type = "keyword", description = """
                    Controls HTML encoding of snippet text before tagging: `default` (no encoding) or `html`.
                    Only applies when highlight is true. Defaults to `default`.
                    """, valueHint = { "default" }, applies_to = "stack: preview 9.5.0"),
                @MapParam.MapParamEntry(name = "order", type = "keyword", description = """
                    Order of returned snippets: `score` (default, by relevance) or `none` (original text order).
                    """, valueHint = { "score", "none" }, applies_to = "stack: preview 9.5.0") }
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

        return resolveParams();
    }

    /**
     * Resolves the type for the function parameters, as part of the type resolution for the function
     *
     */
    private TypeResolution resolveParams() {
        return isString(field(), sourceText(), FIRST).and(() -> resolveQuery())
            .and(() -> resolve(options(), source(), THIRD, ALLOWED_OPTIONS, TopSnippets::validateOptions));
    }

    /**
     * Resolves the type for the query parameter, as part of the type resolution for the function
     *
     * @return type resolution for the query parameter
     */
    private TypeResolution resolveQuery() {
        return isString(query(), sourceText(), SECOND).and(
            () -> resolveTypeQuery(query(), sourceText(), forPreOptimizationValidation(query()))
        );
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        if (query() != null && query() instanceof Literal == false) {
            failures.add(
                fail(query(), "second argument of [{}] must be a constant, received [{}]", sourceText(), Expressions.name(query()))
            );
        }
    }

    private static void validateOptions(Map<String, Object> options) {
        validateOptionValueIsPositiveInteger(options, NUM_SNIPPETS);
        validateOptionValueIsNonNegativeInteger(options, NUM_WORDS);
        validateEncoder(options);
        validateOrder(options);
        validateHighlightOnlyOptions(options);
    }

    private static void validateOrder(Map<String, Object> options) {
        Object value = options.get(ORDER);
        if (value != null && SCORE_ORDER.equals(value) == false && DOC_ORDER.equals(value) == false) {
            throw new InvalidArgumentException("'{}' option must be '{}' or '{}', found [{}]", ORDER, SCORE_ORDER, DOC_ORDER, value);
        }
    }

    private static void validateOptionValueIsPositiveInteger(Map<String, Object> options, String paramName) {
        Object value = options.get(paramName);
        if (value != null && ((Number) value).intValue() <= 0) {
            throw new InvalidArgumentException("'{}' option must be a positive integer, found [{}]", paramName, value);
        }
    }

    private static void validateOptionValueIsNonNegativeInteger(Map<String, Object> options, String paramName) {
        Object value = options.get(paramName);
        if (value != null && ((Number) value).intValue() < 0) {
            throw new InvalidArgumentException("'{}' option must be a non-negative integer, found [{}]", paramName, value);
        }
    }

    private static void validateEncoder(Map<String, Object> options) {
        Object value = options.get(ENCODER);
        if (value != null && DEFAULT_ENCODER.equals(value) == false && HTML_ENCODER.equals(value) == false) {
            throw new InvalidArgumentException(
                "'{}' option must be '{}' or '{}', found [{}]",
                ENCODER,
                DEFAULT_ENCODER,
                HTML_ENCODER,
                value
            );
        }
    }

    private static void validateHighlightOnlyOptions(Map<String, Object> options) {
        boolean highlight = Boolean.TRUE.equals(options.get(HIGHLIGHT));
        if (highlight == false) {
            if (options.containsKey(PRE_TAG) || options.containsKey(POST_TAG) || options.containsKey(ENCODER)) {
                throw new InvalidArgumentException(
                    "'{}', '{}', and '{}' options require '{}' to be true",
                    PRE_TAG,
                    POST_TAG,
                    ENCODER,
                    HIGHLIGHT
                );
            }
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
        @Fixed String queryString,
        @Fixed ChunkingSettings chunkingSettings,
        @Fixed MemoryIndexChunkScorer scorer,
        @Fixed int numSnippets,
        @Fixed boolean docOrder,
        @Fixed(includeInToString = false) PassageFormatter highlightFormatter
    ) {
        if (queryString == null) {
            throw new IllegalArgumentException("single-value function encountered multi-value");
        }
        int valueCount = field.getValueCount(position);
        if (valueCount == 0) {
            builder.appendNull();
            return;
        }
        BytesRef scratch = new BytesRef();
        int firstValueIndex = field.getFirstValueIndex(position);

        // Collect all chunks from all field values upfront so we build one index.
        // When chunkingSettings is null (num_words=0), each field value is used as-is.
        ArrayList<String> allChunks = new ArrayList<>();
        for (int i = 0; i < valueCount; i++) {
            BytesRef value = field.getBytesRef(firstValueIndex + i, scratch);
            String text = value.utf8ToString();
            if (chunkingSettings != null) {
                allChunks.addAll(chunkText(text, chunkingSettings));
            } else {
                allChunks.add(text);
            }
        }

        Query luceneQuery = scorer.buildQuery(queryString);
        List<ScoredChunk> topChunks = scorer.scoreChunks(allChunks, luceneQuery, numSnippets, false);
        if (docOrder) {
            topChunks = topChunks.stream().sorted(Comparator.comparingInt(ScoredChunk::originalIndex)).toList();
        }
        if (topChunks.isEmpty()) {
            builder.appendNull();
            return;
        }

        try {
            List<String> snippets;
            if (highlightFormatter != null) {
                snippets = highlightChunks(topChunks, luceneQuery, scorer.analyzer(), highlightFormatter);
            } else {
                snippets = topChunks.stream().map(ScoredChunk::content).toList();
            }
            emitChunks(builder, snippets);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to highlight snippets", e);
        }
    }

    /**
     * Highlights each top chunk independently using a Lucene {@link MemoryIndex}. Each chunk
     * is indexed as a single document with positions and offsets, highlighted via
     * {@code OffsetSource.POSTINGS}, then discarded. This avoids indexing offsets for all
     * chunks upfront when only a few need highlighting.
     */
    private static List<String> highlightChunks(List<ScoredChunk> topChunks, Query query, Analyzer analyzer, PassageFormatter formatter)
        throws IOException {
        List<String> result = new ArrayList<>(topChunks.size());
        for (ScoredChunk chunk : topChunks) {
            result.add(highlightOneChunk(chunk.content(), query, analyzer, formatter));
        }
        return result;
    }

    private static String highlightOneChunk(String text, Query query, Analyzer analyzer, PassageFormatter formatter) throws IOException {
        MemoryIndex mi = new MemoryIndex(true);
        mi.addField(MemoryIndexChunkScorer.CONTENT_FIELD, text, analyzer);
        IndexSearcher searcher = mi.createSearcher();
        CustomUnifiedHighlighter highlighter = buildHighlighter(searcher, analyzer, query, formatter);
        LeafReaderContext leaf = searcher.getIndexReader().leaves().getFirst();
        Snippet[] hits = highlighter.highlightField(leaf.reader(), 0, () -> text);
        return (hits != null && hits.length > 0) ? hits[0].getText() : text;
    }

    private static CustomUnifiedHighlighter buildHighlighter(
        IndexSearcher searcher,
        Analyzer analyzer,
        Query query,
        PassageFormatter formatter
    ) {
        UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(searcher, analyzer);
        builder.withFormatter(formatter);
        builder.withBreakIterator(() -> new CustomSeparatorBreakIterator(CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR));
        int maxAnalyzedOffset = IndexSettings.MAX_ANALYZED_OFFSET_SETTING.get(Settings.EMPTY);
        return new CustomUnifiedHighlighter(
            builder,
            UnifiedHighlighter.OffsetSource.POSTINGS,
            null,
            "",
            MemoryIndexChunkScorer.CONTENT_FIELD,
            query,
            0,
            MAX_PASSAGES_PER_CHUNK,
            maxAnalyzedOffset,
            QueryMaxAnalyzedOffset.create(-1, maxAnalyzedOffset),
            false,
            false
        );
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        int numSnippets;
        int numWords;
        boolean docOrder;
        PassageFormatter highlightFormatter = null;
        if (options != null) {
            Map<String, Object> opts = new HashMap<>();
            Options.populateMap((MapExpression) options, opts, source(), THIRD, ALLOWED_OPTIONS);
            numSnippets = numSnippets(opts);
            numWords = numWords(opts);
            docOrder = DOC_ORDER.equals(opts.get(ORDER));
            if (Boolean.TRUE.equals(opts.get(HIGHLIGHT))) {
                String preTag = (String) opts.getOrDefault(PRE_TAG, DEFAULT_PRE_TAG);
                String postTag = (String) opts.getOrDefault(POST_TAG, DEFAULT_POST_TAG);
                String encoderType = (String) opts.getOrDefault(ENCODER, DEFAULT_ENCODER);
                Encoder encoder = HTML_ENCODER.equals(encoderType) ? new SimpleHTMLEncoder() : new DefaultEncoder();
                highlightFormatter = new CustomPassageFormatter(preTag, postTag, encoder, 0);
            }
        } else {
            numSnippets = DEFAULT_NUM_SNIPPETS;
            numWords = DEFAULT_WORD_SIZE;
            docOrder = false;
        }

        ChunkingSettings chunkingSettings = numWords > 0 ? new SentenceBoundaryChunkingSettings(numWords, 0) : null;

        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();

        Object foldedQuery = query.fold(toEvaluator.foldCtx());
        // at this point this should only return null if we have List<BytesRef> which we handle in process
        String queryString = foldedQuery instanceof BytesRef bytes ? bytes.utf8ToString() : null;

        return new TopSnippetsEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            queryString,
            chunkingSettings,
            scorer,
            numSnippets,
            docOrder,
            highlightFormatter
        );
    }
}
