/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
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
    private static final String PRE_TAGS = "pre_tags";
    private static final String POST_TAGS = "post_tags";
    private static final String ENCODER = "encoder";

    static final String DEFAULT_PRE_TAGS = "<em>";
    static final String DEFAULT_POST_TAGS = "</em>";
    static final String DEFAULT_ENCODER = "default";

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(NUM_SNIPPETS, DataType.INTEGER),
        entry(NUM_WORDS, DataType.INTEGER),
        entry(HIGHLIGHT, DataType.BOOLEAN),
        entry(PRE_TAGS, DataType.KEYWORD),
        entry(POST_TAGS, DataType.KEYWORD),
        entry(ENCODER, DataType.KEYWORD)
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
                """) }
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
                    The maximum number of words to return in each snippet. When set to 0,
                    disables chunking entirely, the input field values are used as-is, which is
                    useful when the text has already been chunked externally.
                    """, valueHint = { "300" }),
                @MapParam.MapParamEntry(name = "highlight", type = "boolean", description = """
                    When true, wraps matched query terms in the returned snippets with markup tags.
                    Defaults to false.
                    """, valueHint = { "true" }),
                @MapParam.MapParamEntry(name = "pre_tags", type = "keyword", description = """
                    Opening tag for highlighted terms. Only applies when highlight is true.
                    Defaults to `<em>`.
                    """, valueHint = { "<em>" }),
                @MapParam.MapParamEntry(name = "post_tags", type = "keyword", description = """
                    Closing tag for highlighted terms. Only applies when highlight is true.
                    Defaults to `</em>`.
                    """, valueHint = { "</em>" }),
                @MapParam.MapParamEntry(name = "encoder", type = "keyword", description = """
                    Controls HTML encoding of snippet text before tagging: `default` (no encoding) or `html`.
                    Only applies when highlight is true. Defaults to `default`.
                    """, valueHint = { "default" }) }
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
        validateOptionValueIsPositiveInteger(options, NUM_WORDS);
        validateEncoder(options);
        validateHighlightOnlyOptions(options);
    }


    private static void validateOptionValueIsPositiveInteger(Map<String, Object> options, String paramName) {
        Object value = options.get(paramName);
        if (value != null && ((Number) value).intValue() <= 0) {
            throw new InvalidArgumentException("'{}' option must be a positive integer, found [{}]", paramName, value);
        }
    }

    private static void validateEncoder(Map<String, Object> options) {
        Object value = options.get(ENCODER);
        if (value != null && "default".equals(value) == false && "html".equals(value) == false) {
            throw new InvalidArgumentException("'{}' option must be 'default' or 'html', found [{}]", ENCODER, value);
        }
    }

    private static void validateHighlightOnlyOptions(Map<String, Object> options) {
        boolean highlight = Boolean.TRUE.equals(options.get(HIGHLIGHT));
        if (highlight == false) {
            if (options.containsKey(PRE_TAGS) || options.containsKey(POST_TAGS) || options.containsKey(ENCODER)) {
                throw new InvalidArgumentException(
                    "'{}', '{}', and '{}' options require '{}' to be true",
                    PRE_TAGS,
                    POST_TAGS,
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

    /**
     * Normalises an option value produced by {@link Options#populateMap} into a {@code List<String>}.
     * After the populateMap change, a KEYWORD option may arrive as either a plain {@code String}
     * (scalar literal) or a {@code List<String>} (array literal such as {@code ["<b>","<em>"]}).
     */
    @SuppressWarnings("unchecked")
    private static List<String> toStringList(Object value, String defaultValue) {
        if (value == null) {
            return List.of(defaultValue);
        }
        if (value instanceof List<?>) {
            return (List<String>) value;
        }
        return List.of((String) value);
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static void process(
        BytesRefBlock.Builder builder,
        @Position int position,
        BytesRefBlock field,
        BytesRefBlock query,
        @Fixed ChunkingSettings chunkingSettings,
        @Fixed MemoryIndexChunkScorer scorer,
        @Fixed int numSnippets,
        @Fixed(includeInToString = false) PassageFormatter highlightFormatter
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

        // Collect all chunks from all field values upfront so we build one index
        ArrayList<String> allChunks = new ArrayList<>();
        for (int i = 0; i < valueCount; i++) {
            BytesRef value = field.getBytesRef(firstValueIndex + i, scratch);
            allChunks.addAll(chunkText(value.utf8ToString(), chunkingSettings));
        }

        try (var session = scorer.openSession(allChunks, highlightFormatter != null)) {
            List<ScoredChunk> scored = session.score(queryString, allChunks.size(), false);

            List<ScoredChunk> topChunks = scored.stream()
                .sorted(Comparator.comparing(ScoredChunk::score).reversed())
                .limit(numSnippets)
                .toList();

            if (topChunks.isEmpty()) {
                builder.appendNull();
                return;
            }

            List<String> snippets;
            if (highlightFormatter != null) {
                snippets = highlightSnippets(session, topChunks, queryString, highlightFormatter);
            } else {
                snippets = topChunks.stream().map(ScoredChunk::content).toList();
            }

            emitChunks(builder, snippets);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to process snippets", e);
        }
    }

    /**
     * Adds highlight markup around matched query terms within each snippet. Reuses the session's
     * in-memory index: chunks are indexed with postings offsets, so we use {@code OffsetSource.POSTINGS}
     * and the correct Lucene doc id per chunk instead of re-analyzing the full text ({@code ANALYSIS}).
     */
    private static List<String> highlightSnippets(
        MemoryIndexChunkScorer.Session session,
        List<ScoredChunk> topChunks,
        String queryText,
        PassageFormatter formatter
    ) throws IOException {
        QueryBuilder qb = new QueryBuilder(session.analyzer());
        Query query = qb.createBooleanQuery(MemoryIndexChunkScorer.CONTENT_FIELD, queryText, BooleanClause.Occur.SHOULD);
        if (query == null) {
            return topChunks.stream().map(ScoredChunk::content).toList();
        }

        UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(session.searcher(), session.analyzer());
        builder.withFormatter(formatter);
        builder.withBreakIterator(() -> new CustomSeparatorBreakIterator(CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR));
        int maxAnalyzedOffset = IndexSettings.MAX_ANALYZED_OFFSET_SETTING.get(Settings.EMPTY);
        CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(
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

        return highlightScoredChunks(highlighter, session.reader(), topChunks);
    }

    /**
     * Highlights each chunk using the global doc id from {@link ScoredChunk#docId()}. Lucene's highlighter API is
     * leaf-scoped; {@link ReaderUtil#subIndex} is the same pattern as {@code FetchPhaseDocsIterator} and
     * {@code FollowingEngine} for resolving a top-level doc id to a segment.
     */
    private static List<String> highlightScoredChunks(
        CustomUnifiedHighlighter highlighter,
        IndexReader reader,
        List<ScoredChunk> topChunks
    ) throws IOException {
        List<LeafReaderContext> leaves = reader.leaves();
        if (leaves.isEmpty()) {
            return topChunks.stream().map(ScoredChunk::content).toList();
        }
        int maxDoc = reader.maxDoc();
        List<String> result = new ArrayList<>(topChunks.size());
        for (ScoredChunk chunk : topChunks) {
            result.add(highlightOneScoredChunk(highlighter, leaves, maxDoc, chunk));
        }
        return result;
    }

    private static String highlightOneScoredChunk(
        CustomUnifiedHighlighter highlighter,
        List<LeafReaderContext> leaves,
        int maxDoc,
        ScoredChunk chunk
    ) throws IOException {
        String text = chunk.content();
        int globalDoc = chunk.docId();
        if (globalDoc < 0 || globalDoc >= maxDoc) {
            return text;
        }
        LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(globalDoc, leaves));
        Snippet[] hits = highlighter.highlightField(leaf.reader(), globalDoc - leaf.docBase, () -> text);
        return (hits != null && hits.length > 0) ? hits[0].getText() : text;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        int numSnippets;
        int numWords;
        PassageFormatter highlightFormatter = null;
        if (options != null) {
            Map<String, Object> opts = new HashMap<>();
            Options.populateMap((MapExpression) options, opts, source(), THIRD, ALLOWED_OPTIONS);
            numSnippets = numSnippets(opts);
            numWords = numWords(opts);
            if (Boolean.TRUE.equals(opts.get(HIGHLIGHT))) {
                List<String> preTags = toStringList(opts.get(PRE_TAGS), DEFAULT_PRE_TAGS);
                List<String> postTags = toStringList(opts.get(POST_TAGS), DEFAULT_POST_TAGS);
                //TODO(mromaios): the array complicates things, AFAIU the current DefaultHighlighter also just gets the first tag to use
                //do we really need multiple tags support here? Maybe not, but only have this in the HIGHLIGHT cmd
                String encoderType = opts.containsKey(ENCODER) ? (String) opts.get(ENCODER) : DEFAULT_ENCODER;
                Encoder encoder = "html".equals(encoderType) ? new SimpleHTMLEncoder() : new DefaultEncoder();
                //TODO(mromaios): We could use HighlightUtils.Encoders.HTML, but it's under search/phase/subphase which feels weird.
                highlightFormatter = new CustomPassageFormatter(preTags.getFirst(), postTags.getFirst(), encoder, 0);
            }
        } else {
            numSnippets = DEFAULT_NUM_SNIPPETS;
            numWords = DEFAULT_WORD_SIZE;
        }

        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(numWords, 0);
        //TODO(mromaios): add Analyzer support
        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();

        return new TopSnippetsEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            toEvaluator.apply(query),
            chunkingSettings,
            scorer,
            numSnippets,
            highlightFormatter
        );
    }
}
