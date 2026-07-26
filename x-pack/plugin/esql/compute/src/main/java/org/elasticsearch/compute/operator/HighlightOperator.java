/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.search.uhighlight.CharArrayMatcher;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.SplittingBreakIterator;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.lucene.search.uhighlight.BoundedBreakIteratorScanner;
import org.elasticsearch.lucene.search.uhighlight.CustomPassageFormatter;
import org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.elasticsearch.lucene.search.uhighlight.QueryMaxAnalyzedOffset;
import org.elasticsearch.lucene.search.uhighlight.Snippet;
import org.elasticsearch.search.fetch.subphase.highlight.LimitTokenOffsetAnalyzer;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Appends one highlighted keyword column per ON field to the input page.
 * <p>
 * Each row is highlighted via a Lucene {@link MemoryIndex} and {@link CustomUnifiedHighlighter}: ON-field values are
 * analyzed into an in-memory document, the configured {@link Query} is run against it, and matched terms are wrapped
 * with the configured tags. Non-matching rows yield {@code null} (or leading text when {@code no_match_size > 0}).
 * <p>
 * The {@link MemoryIndex} is built with offsets ({@link UnifiedHighlighter.OffsetSource#POSTINGS}), matching the
 * coordinator-side path used by {@code TOP_SNIPPETS}. Unlike Query DSL highlighting, analyzed tokens are truncated at
 * the configured/default offset instead of throwing a "field too long" error.
 * <p>
 * TODO: use real index offsets and per-field analyzers when highlighting can run against shard data.
 */
public class HighlightOperator extends AbstractPageMappingOperator {

    public record Factory(HighlightConfig config, List<ExpressionEvaluator.Factory> fieldEvaluatorFactories) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            ExpressionEvaluator[] fieldEvaluators = fieldEvaluatorFactories.stream()
                .map(factory -> factory.get(driverContext))
                .toArray(ExpressionEvaluator[]::new);
            return new HighlightOperator(driverContext.blockFactory(), config, fieldEvaluators);
        }

        @Override
        public String describe() {
            return "HighlightOperator[" + config.describe() + ", fields=" + fieldEvaluatorFactories.size() + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final HighlightConfig config;
    private final Query query;
    private final List<String> fieldNames;
    private final Analyzer analyzer;
    private final Analyzer memoryIndexAnalyzer;
    private final PassageFormatter formatter;
    private final int indexMaxAnalyzedOffset;
    private final QueryMaxAnalyzedOffset queryMaxAnalyzedOffset;
    private final int highlighterNumberOfFragments;
    private final Supplier<BreakIterator> breakIteratorSupplier;
    private final ExpressionEvaluator[] fieldEvaluators;
    private final MemoryIndex memoryIndex;
    private final LeafReader memoryIndexReader;
    private final CustomUnifiedHighlighter[] highlighters;
    private final CharArrayMatcher keepWordMatcher;

    public HighlightOperator(BlockFactory blockFactory, HighlightConfig config, ExpressionEvaluator[] fieldEvaluators) {
        this.blockFactory = blockFactory;
        this.config = config;
        this.fieldEvaluators = fieldEvaluators;
        this.analyzer = config.requiredAnalyzer();
        this.query = config.requiredQuery();
        this.fieldNames = config.fieldNames();
        assert fieldNames.size() == fieldEvaluators.length
            : "HIGHLIGHT ON field count [" + fieldNames.size() + "] does not match ON expression count [" + fieldEvaluators.length + "]";
        Encoder encoder = HighlightConfig.HTML_ENCODER.equals(config.encoder()) ? new SimpleHTMLEncoder() : new DefaultEncoder();
        this.formatter = new CustomPassageFormatter(config.preTag(), config.postTag(), encoder, config.numberOfFragments());
        // Coordinator-side highlighting has no IndexSettings yet, so the index cap is just the default. Clamping the
        // user's max_analyzed_offset to it (rather than overwriting the index cap) prevents raising the default.
        this.indexMaxAnalyzedOffset = IndexSettings.MAX_ANALYZED_OFFSET_SETTING.get(Settings.EMPTY);
        int configuredOffset = config.maxAnalyzedOffset();
        int queryOffset = configuredOffset < 0 ? indexMaxAnalyzedOffset : Math.min(configuredOffset, indexMaxAnalyzedOffset);
        this.queryMaxAnalyzedOffset = QueryMaxAnalyzedOffset.create(queryOffset, indexMaxAnalyzedOffset);
        Analyzer indexingAnalyzer = analyzer instanceof NamedAnalyzer named ? named.analyzer() : analyzer;
        this.memoryIndexAnalyzer = new LimitTokenOffsetAnalyzer(indexingAnalyzer, queryMaxAnalyzedOffset.getNotNull());
        // number_of_fragments=0 means whole value; CustomUnifiedHighlighter uses MAX_VALUE-1 for that.
        this.highlighterNumberOfFragments = config.numberOfFragments() > 0 ? config.numberOfFragments() : Integer.MAX_VALUE - 1;
        this.breakIteratorSupplier = breakIterator(
            config.numberOfFragments(),
            config.fragmentSize(),
            config.wordBoundary(),
            config.locale()
        );
        this.memoryIndex = new MemoryIndex(true); // true == store offsets, required by OffsetSource.POSTINGS
        IndexSearcher searcher = memoryIndex.createSearcher();
        this.memoryIndexReader = (LeafReader) searcher.getIndexReader();
        this.highlighters = new CustomUnifiedHighlighter[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(searcher, analyzer);
            builder.withFormatter(formatter);
            builder.withBreakIterator(breakIteratorSupplier);
            highlighters[i] = new CustomUnifiedHighlighter(
                builder,
                UnifiedHighlighter.OffsetSource.POSTINGS,
                null,
                "",
                fieldNames.get(i),
                query,
                config.noMatchSize(),
                highlighterNumberOfFragments,
                indexMaxAnalyzedOffset,
                queryMaxAnalyzedOffset,
                true,
                true
            );
        }
        this.keepWordMatcher = buildKeepWordMatcher(query);
    }

    /**
     * Builds a matcher that keeps only tokens the query actually needs, either to highlight or to decide if a row
     * matches. Returns {@code null} when the query contains clauses whose terms cannot be enumerated (multi-term or
     * automaton queries, unknown leaves); in that case we fall back to indexing every token.
     * <p>
     * We build one matcher from all query terms across every ON field. Keeping a token in field B's postings that only
     * field A's query mentions is harmless: the highlighter looks up {@code field:term} postings, so an over-kept token
     * can never produce a false highlight. Splitting the matcher per field is a possible future tweak, but it is not
     * worth it here.
     * <p>
     * {@code MUST_NOT} terms are deliberately kept, even though the highlighter never wraps them in tags.
     * <p>
     * Any other non-enumerable leaf disables filtering entirely. Under-keeping a token would break highlighting, while
     * over-keeping (not filtering at all) only costs performance.
     */
    private static CharArrayMatcher buildKeepWordMatcher(Query query) {
        List<BytesRef> terms = new ArrayList<>();
        boolean[] unfilterable = new boolean[1];
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query q, Term... queryTerms) {
                for (Term term : queryTerms) {
                    terms.add(term.bytes());
                }
            }

            @Override
            public void consumeTermsMatching(Query q, String field, Supplier<ByteRunAutomaton> automaton) {
                unfilterable[0] = true; // wildcard/prefix/regexp etc., cannot enumerate safely
            }

            @Override
            public void visitLeaf(Query q) {
                unfilterable[0] = true; // leaf that reported no terms, we don't know what it matches
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                // QueryVisitor's default returns EMPTY_VISITOR for MUST_NOT, which would skip its terms.
                return this;
            }
        });
        if (unfilterable[0] || terms.isEmpty()) {
            return null;
        }
        for (BytesRef term : terms) {
            if (term.length >= Automata.MAX_STRING_UNION_TERM_LENGTH) {
                // CharArrayMatcher.fromTerms cannot build an automaton over huge terms
                return null;
            }
        }
        // The underlying string-union automaton builder requires strictly sorted, de-duplicated input. Query terms
        // often repeat across clauses (e.g. the same term MUST'd and used in a phrase), so dedupe after sorting.
        Collections.sort(terms);
        return CharArrayMatcher.fromTerms(terms.stream().distinct().toList());
    }

    // Mirrors DefaultHighlighter#getBreakIterator: the word scanner ignores fragment_size, the sentence scanner honours it.
    private static Supplier<BreakIterator> breakIterator(int numberOfFragments, int fragmentSize, boolean wordBoundary, Locale locale) {
        if (numberOfFragments == 0) {
            // One passage per (multi-)value: only break on the multi-value separator.
            return () -> new CustomSeparatorBreakIterator(CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR);
        }
        return () -> {
            BreakIterator passageIterator = wordBoundary
                ? BreakIterator.getWordInstance(locale)
                : sentenceBreakIterator(fragmentSize, locale);
            return new SplittingBreakIterator(passageIterator, CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR);
        };
    }

    // Break on sentences, capped to fragment_size chars when it's positive (long sentences get split, short ones may
    // share a fragment). A non-positive fragment_size drops the cap and just breaks on sentences.
    private static BreakIterator sentenceBreakIterator(int fragmentSize, Locale locale) {
        return fragmentSize > 0 ? BoundedBreakIteratorScanner.getSentence(locale, fragmentSize) : BreakIterator.getSentenceInstance(locale);
    }

    @Override
    protected Page process(Page page) {
        int rowCount = page.getPositionCount();
        int fieldCount = fieldEvaluators.length;
        HighlightField[] fields = new HighlightField[fieldCount];
        Block[] highlightedBlocks = new Block[fieldCount];
        BytesRef scratch = new BytesRef();
        boolean success = false;
        try {
            initFields(page, rowCount, fields);
            for (int row = 0; row < rowCount; row++) {
                highlightRow(row, fields, scratch);
            }
            buildHighlightedBlocks(fields, highlightedBlocks);
            Page result = page.appendBlocks(highlightedBlocks);
            success = true;
            return result;
        } finally {
            Releasables.closeExpectNoException(fields);
            if (success == false) {
                Releasables.closeExpectNoException(highlightedBlocks);
            }
        }
    }

    private void initFields(Page page, int rowCount, HighlightField[] fields) {
        for (int field = 0; field < fieldEvaluators.length; field++) {
            Block block = fieldEvaluators[field].eval(page);
            if (block instanceof BytesRefBlock b) {
                BytesRefBlock.Builder builder = null;
                try {
                    builder = blockFactory.newBytesRefBlockBuilder(rowCount);
                    fields[field] = new HighlightField(fieldNames.get(field), b, builder);
                    continue;
                } catch (RuntimeException e) {
                    Releasables.closeExpectNoException(b, builder);
                    throw e;
                }
            }
            block.close();
            throw new IllegalArgumentException(
                "HIGHLIGHT ON fields must be [text] or [keyword], found [" + block.getClass().getSimpleName() + "]"
            );
        }
    }

    private static void buildHighlightedBlocks(HighlightField[] fields, Block[] highlightedBlocks) {
        for (int field = 0; field < highlightedBlocks.length; field++) {
            highlightedBlocks[field] = fields[field].builder.build();
        }
    }

    private void highlightRow(int row, HighlightField[] fields, BytesRef scratch) {
        boolean hasRowValues = false;
        for (HighlightField field : fields) {
            field.loadRowText(row, scratch);
            hasRowValues |= field.rowText != null;
        }
        if (hasRowValues == false) {
            appendNulls(fields);
            return;
        }
        if (fillRowIndex(fields)) {
            // The query can't match anything kept from this row and no_match_size == 0, so every field's output is
            // null by definition. Skip building highlighters' passages entirely.
            appendNulls(fields);
            return;
        }
        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            HighlightField field = fields[fieldIndex];
            if (field.rowText == null) {
                field.builder.appendNull();
                continue;
            }
            try {
                appendSnippets(field.builder, highlight(fieldIndex, field.rowText));
            } catch (IOException e) {
                throw new IllegalStateException("HIGHLIGHT failed for ON field [" + field.name + "]", e);
            }
        }
    }

    private boolean fillRowIndex(HighlightField[] fields) {
        memoryIndex.reset();
        int keptTokens = 0;
        for (HighlightField field : fields) {
            if (field.rowText == null) {
                continue;
            }
            if (keepWordMatcher == null) {
                memoryIndex.addField(field.name, field.rowText, memoryIndexAnalyzer);
            } else {
                // Only tokens the query could match are indexed: the rest are dropped before they ever reach
                // MemoryIndex.addField, so hashing and sorting only ever see query terms.
                TokenStream tokenStream = memoryIndexAnalyzer.tokenStream(field.name, field.rowText);
                KeepQueryTermsFilter filtered = new KeepQueryTermsFilter(tokenStream, keepWordMatcher);
                memoryIndex.addField(field.name, filtered); // addField resets and closes the stream
                keptTokens += filtered.kept;
            }
        }
        // Without filtering we can't prove the row is a miss (keptTokens stays 0), so we never short-circuit.
        return keepWordMatcher != null && keptTokens == 0 && config.noMatchSize() == 0;
    }

    /**
     * Drops tokens the query cannot match before they reach the memory index, so hashing and sorting only see query
     * terms. {@link FilteringTokenFilter} accumulates position increments for the dropped tokens, so kept tokens keep
     * their original positions and phrase queries still match exactly as they would against the unfiltered index. This
     * mirrors what Lucene's {@code MemoryIndexOffsetStrategy} does for Query DSL highlighting.
     */
    private static final class KeepQueryTermsFilter extends FilteringTokenFilter {
        private final CharArrayMatcher matcher;
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private int kept = 0;

        KeepQueryTermsFilter(TokenStream in, CharArrayMatcher matcher) {
            super(in);
            this.matcher = matcher;
        }

        @Override
        protected boolean accept() {
            boolean keep = matcher.match(termAtt.buffer(), 0, termAtt.length());
            if (keep) {
                kept++;
            }
            return keep;
        }
    }

    private static void appendNulls(HighlightField[] fields) {
        for (HighlightField field : fields) {
            field.builder.appendNull();
        }
    }

    private static final class HighlightField implements Releasable {
        private final String name;
        private final BytesRefBlock values;
        private final BytesRefBlock.Builder builder;
        private String rowText;

        private HighlightField(String name, BytesRefBlock values, BytesRefBlock.Builder builder) {
            this.name = name;
            this.values = values;
            this.builder = builder;
        }

        private void loadRowText(int row, BytesRef scratch) {
            int valueCount = values.getValueCount(row);
            rowText = valueCount == 0 ? null : joinValues(values, row, valueCount, scratch);
        }

        @Override
        public void close() {
            Releasables.close(values, builder);
        }
    }

    /**
     * Joins all values of a multi-valued field into a single string separated by the highlighter's multi-value
     * separator, so fragment scanning never crosses a value boundary. This is an intentional divergence from Query
     * DSL: HIGHLIGHT always keeps fragments aligned to multi-value boundaries (via the {@link SplittingBreakIterator}
     * on the separator applied in {@link #breakIterator}), whereas the unified highlighter can merge terminator-less
     * short values that fit within {@code fragment_size} into a single fragment.
     */
    private static String joinValues(BytesRefBlock fieldValues, int row, int valueCount, BytesRef scratch) {
        int firstValueIndex = fieldValues.getFirstValueIndex(row);
        if (valueCount == 1) {
            return fieldValues.getBytesRef(firstValueIndex, scratch).utf8ToString();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < valueCount; i++) {
            if (i > 0) {
                sb.append(CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR);
            }
            sb.append(fieldValues.getBytesRef(firstValueIndex + i, scratch).utf8ToString());
        }
        return sb.toString();
    }

    // Reuses the per-field highlighter built in the constructor against the shared, just-refilled memory index
    // reader. The highlighter's internal FieldHighlighter is derived from the query at construction time and doesn't
    // cache anything from the reader, so reusing it across rows and pages is safe.
    private Snippet[] highlight(int fieldIndex, String text) throws IOException {
        return highlighters[fieldIndex].highlightField(memoryIndexReader, 0, () -> text);
    }

    /**
     * Appends the highlighter output for one row: {@code null} when there is no snippet (no match and no
     * {@code no_match_size}), a single value, or a multi-value entry when several fragments are returned. Snippets
     * arrive in document order; when {@code order} is {@code score} they are re-sorted by descending score first. When
     * {@code number_of_fragments > 0} they are then capped to that many fragments.
     */
    private void appendSnippets(BytesRefBlock.Builder builder, Snippet[] snippets) {
        if (snippets == null || snippets.length == 0) {
            builder.appendNull();
            return;
        }
        if (config.orderByScore() && snippets.length > 1) {
            Arrays.sort(snippets, SCORE_DESCENDING);
        }
        int count = snippets.length;
        if (config.numberOfFragments() > 0) {
            count = Math.min(count, config.numberOfFragments());
        }
        if (count == 1) {
            builder.appendBytesRef(new BytesRef(snippets[0].getText()));
            return;
        }
        builder.beginPositionEntry();
        for (int i = 0; i < count; i++) {
            builder.appendBytesRef(new BytesRef(snippets[i].getText()));
        }
        builder.endPositionEntry();
    }

    // Highest score first. NaN counts as the lowest score so the no-match fallback passage (which carries a NaN score)
    // never sorts ahead of a real fragment. Arrays.sort is stable, so equal scores keep document order.
    static final Comparator<Snippet> SCORE_DESCENDING = Comparator.comparingDouble((Snippet s) -> {
        float score = s.getScore();
        return Float.isNaN(score) ? Double.NEGATIVE_INFINITY : score;
    }).reversed();

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[query="
            + query
            + ", "
            + config.describe()
            + ", fields="
            + Arrays.toString(fieldEvaluators)
            + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> Releasables.close(fieldEvaluators), super::close);
    }
}
