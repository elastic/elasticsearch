/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.SplittingBreakIterator;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.lucene.search.uhighlight.BoundedBreakIteratorScanner;
import org.elasticsearch.lucene.search.uhighlight.CustomPassageFormatter;
import org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.elasticsearch.lucene.search.uhighlight.QueryMaxAnalyzedOffset;
import org.elasticsearch.lucene.search.uhighlight.Snippet;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Appends one highlighted keyword column per ON field to the input page.
 * <p>
 * Each ON field is evaluated to its {@link BytesRefBlock} value and highlighted per row using a Lucene
 * {@link MemoryIndex}: the row's (possibly multi-valued) text is analyzed into a single in-memory document and the
 * configured {@link Query} is run against it through a {@link CustomUnifiedHighlighter}. Matched terms are wrapped with
 * the configured tags by the {@link PassageFormatter}. A row that the query does not match yields {@code null} (or, when
 * {@code no_match_size > 0}, the leading text). Multiple fragments and multi-valued inputs become a multi-value keyword
 * block.
 * <p>
 * The {@link MemoryIndex} is built with offsets, so we read them via {@link UnifiedHighlighter.OffsetSource#POSTINGS}.
 * This is the same coordinator-side path that {@code TOP_SNIPPETS} already uses.
 * <p>
 * TODO: use real index offsets and per-field analyzers when highlighting can run directly against shard data.
 */
public class HighlightOperator extends AbstractPageMappingOperator {

    /**
     * Synthetic field name under which each row's text is indexed in the per-row {@link MemoryIndex}. The configured
     * {@link Query} passed to the factory must be built against this same field name.
     */
    public static final String CONTENT_FIELD = "content";

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
            return "HighlightOperator[query="
                + config.queryText()
                + ", fields="
                + fieldEvaluatorFactories.size()
                + ", number_of_fragments="
                + config.numberOfFragments()
                + ", fragment_size="
                + config.fragmentSize()
                + ", no_match_size="
                + config.noMatchSize()
                + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final HighlightConfig config;
    private final Query query;
    private final Analyzer analyzer;
    private final PassageFormatter formatter;
    private final int maxAnalyzedOffset;
    private final int highlighterNumberOfFragments;
    private final Supplier<BreakIterator> breakIteratorSupplier;
    private final ExpressionEvaluator[] fieldEvaluators;

    public HighlightOperator(BlockFactory blockFactory, HighlightConfig config, ExpressionEvaluator[] fieldEvaluators) {
        this.blockFactory = blockFactory;
        this.config = config;
        this.fieldEvaluators = fieldEvaluators;
        // TODO: resolve a named analyzer from the AnalysisRegistry once the "analyzer" option is supported.
        this.analyzer = new StandardAnalyzer();
        // TODO: support more query shapes here (phrase, fuzzy, wildcard, QSTR, KQL, MATCH, MATCH_PHRASE) instead of
        // treating the query text as a bag of words.
        Query parsedQuery = new QueryBuilder(analyzer).createBooleanQuery(CONTENT_FIELD, config.queryText(), BooleanClause.Occur.SHOULD);
        this.query = parsedQuery != null ? parsedQuery : new MatchNoDocsQuery("HIGHLIGHT query produced no terms");
        Encoder encoder = HighlightConfig.HTML_ENCODER.equals(config.encoder()) ? new SimpleHTMLEncoder() : new DefaultEncoder();
        this.formatter = new CustomPassageFormatter(config.preTag(), config.postTag(), encoder, config.numberOfFragments());
        this.maxAnalyzedOffset = IndexSettings.MAX_ANALYZED_OFFSET_SETTING.get(Settings.EMPTY);
        // Ask Lucene for every passage and trim to number_of_fragments ourselves. Lucene would otherwise keep the
        // top passages by score, which loses document order when several sentences tie. We want document order.
        this.highlighterNumberOfFragments = Integer.MAX_VALUE - 1;
        // TODO: honour boundary_scanner, boundary_scanner_locale, boundary_chars, boundary_max_scan, and order.
        if (config.numberOfFragments() == 0) {
            // One passage per (multi-)value: only break on the multi-value separator.
            this.breakIteratorSupplier = () -> new CustomSeparatorBreakIterator(CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR);
        } else {
            // Fragment by sentence, bounded to fragment_size characters when a positive size is requested.
            this.breakIteratorSupplier = () -> new SplittingBreakIterator(
                sentenceBreakIterator(config.fragmentSize()),
                CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR
            );
        }
    }

    // Break on sentences, capped to fragment_size chars when it's positive (long sentences get split, short ones may
    // share a fragment). A non-positive fragment_size drops the cap and just breaks on sentences.
    private static BreakIterator sentenceBreakIterator(int fragmentSize) {
        return fragmentSize > 0
            ? BoundedBreakIteratorScanner.getSentence(Locale.ROOT, fragmentSize)
            : BreakIterator.getSentenceInstance(Locale.ROOT);
    }

    @Override
    protected Page process(Page page) {
        int rowCount = page.getPositionCount();
        Block[] highlightedBlocks = new Block[fieldEvaluators.length];
        // One scratch BytesRef is reused across every field and every row of this page; the operator is single-threaded
        // per driver and the scratch is never retained past process().
        BytesRef scratch = new BytesRef();
        try {
            for (int f = 0; f < fieldEvaluators.length; f++) {
                try (Block block = fieldEvaluators[f].eval(page)) {
                    if (block instanceof BytesRefBlock fieldValues) {
                        highlightedBlocks[f] = highlightField(fieldValues, rowCount, scratch);
                    } else {
                        throw new IllegalArgumentException(
                            "HIGHLIGHT ON fields must evaluate to keyword/text values but got [" + block.getClass().getSimpleName() + "]"
                        );
                    }
                }
            }
            return page.appendBlocks(highlightedBlocks);
        } catch (Exception e) {
            // If we highlighted some fields but failed before appending them, we need to release them.
            Releasables.closeExpectNoException(highlightedBlocks);
            throw e;
        }
    }

    private Block highlightField(BytesRefBlock fieldValues, int rowCount, BytesRef scratch) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
            for (int row = 0; row < rowCount; row++) {
                int valueCount = fieldValues.getValueCount(row);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                String text = joinValues(fieldValues, row, valueCount, scratch);
                try {
                    appendSnippets(builder, highlight(text));
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to highlight field", e);
                }
            }
            return builder.build();
        }
    }

    /**
     * Joins all values of a multi-valued field into a single string separated by the highlighter's multi-value
     * separator, so fragment scanning never crosses a value boundary.
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

    private Snippet[] highlight(String text) throws IOException {
        MemoryIndex memoryIndex = new MemoryIndex(true);
        memoryIndex.addField(CONTENT_FIELD, text, analyzer);
        IndexSearcher searcher = memoryIndex.createSearcher();
        UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(searcher, analyzer);
        builder.withFormatter(formatter);
        builder.withBreakIterator(breakIteratorSupplier);
        CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(
            builder,
            UnifiedHighlighter.OffsetSource.POSTINGS,
            null,
            "",
            CONTENT_FIELD,
            query,
            config.noMatchSize(),
            highlighterNumberOfFragments,
            maxAnalyzedOffset,
            QueryMaxAnalyzedOffset.create(-1, maxAnalyzedOffset),
            false,
            false
        );
        LeafReaderContext leaf = searcher.getIndexReader().leaves().getFirst();
        return highlighter.highlightField(leaf.reader(), 0, () -> text);
    }

    /**
     * Appends the highlighter output for one row: {@code null} when there is no snippet (no match and no
     * {@code no_match_size}), a single value, or a multi-value entry when several fragments are returned. When
     * {@code number_of_fragments > 0} the snippets, which arrive in document order, are capped to that many fragments.
     */
    private void appendSnippets(BytesRefBlock.Builder builder, Snippet[] snippets) {
        int length = snippets == null ? 0 : snippets.length;
        int numberOfFragments = config.numberOfFragments();
        if (numberOfFragments > 0) {
            length = Math.min(length, numberOfFragments);
        }
        if (length == 0) {
            builder.appendNull();
        } else if (length == 1) {
            builder.appendBytesRef(new BytesRef(snippets[0].getText()));
        } else {
            builder.beginPositionEntry();
            for (int i = 0; i < length; i++) {
                builder.appendBytesRef(new BytesRef(snippets[i].getText()));
            }
            builder.endPositionEntry();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[query="
            + query
            + ", number_of_fragments="
            + config.numberOfFragments()
            + ", fragment_size="
            + config.fragmentSize()
            + ", no_match_size="
            + config.noMatchSize()
            + ", fields="
            + Arrays.toString(fieldEvaluators)
            + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> Releasables.close(fieldEvaluators), super::close);
    }
}
