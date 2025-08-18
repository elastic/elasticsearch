/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightSnippetUtils;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.Text;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.core.RefCounted.ALWAYS_REFERENCED;

public class HighlighterExpressionEvaluator extends LuceneQueryEvaluator<BytesRefBlock.Builder>
    implements
        EvalOperator.ExpressionEvaluator {

    private final String fieldName;
    private final Integer numFragments;
    private final Integer fragmentLength;
    private final Map<String, Highlighter> highlighters;
    private final FetchContext fetchContext;
    private final MappedFieldType fieldType;

    HighlighterExpressionEvaluator(
        BlockFactory blockFactory,
        ShardConfig[] shardConfigs,
        String fieldName,
        Integer numFragments,
        Integer fragmentLength,
        SearchContext searchContext,
        Map<String, Highlighter> highlighters
    ) {
        super(blockFactory, shardConfigs);
        this.fieldName = fieldName;
        this.numFragments = numFragments;
        this.fragmentLength = fragmentLength;
        this.highlighters = highlighters;

        // Create a source loader for highlighter use
        SourceLoader sourceLoader = searchContext.newSourceLoader(null);
        fetchContext = new FetchContext(searchContext, sourceLoader);
        SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
        if (searchExecutionContext == null) {
            throw new IllegalStateException("SearchExecutionContext not found");
        }
        fieldType = searchExecutionContext.getFieldType(fieldName);
    }

    @Override
    protected ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    protected Block createNoMatchBlock(BlockFactory blockFactory, int size) {
        return blockFactory.newConstantNullBlock(size);
    }

    @Override
    protected BytesRefBlock.Builder createBlockBuilder(BlockFactory blockFactory, int size) {
        return blockFactory.newBytesRefBlockBuilder(size * numFragments);
    }

    @Override
    protected void appendMatch(BytesRefBlock.Builder builder, Scorable scorer, int docId, LeafReaderContext leafReaderContext, Query query)
        throws IOException {

        // TODO: Can we build a custom highlighter directly here, so we don't have to rely on fetch phase classes?

        SearchHit searchHit = new SearchHit(docId, null, null, ALWAYS_REFERENCED);
        Source source = Source.lazy(lazyStoredSourceLoader(leafReaderContext, docId));
        Highlighter highlighter = highlighters.getOrDefault(fieldType.getDefaultHighlighter(), new DefaultHighlighter());

        SearchHighlightContext.Field field = HighlightSnippetUtils.buildFieldHighlightContextForSnippets(
            fetchContext.getSearchExecutionContext(),
            fieldName,
            numFragments != null ? numFragments : HighlightBuilder.DEFAULT_NUMBER_OF_FRAGMENTS,
            fragmentLength != null ? fragmentLength : HighlightBuilder.DEFAULT_FRAGMENT_CHAR_SIZE,
            query
        );
        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(searchHit, leafReaderContext, docId, Map.of(), source, null);
        FieldHighlightContext highlightContext = new FieldHighlightContext(
            fieldName,
            field,
            fieldType,
            fetchContext,
            hitContext,
            query,
            new HashMap<>()
        );
        HighlightField highlight = highlighter.highlight(highlightContext);

        if (highlight != null) {
            boolean multivalued = highlight.fragments().length > 1;
            if (multivalued) {
                builder.beginPositionEntry();
            }
            for (Text highlightText : highlight.fragments()) {
                builder.appendBytesRef(new BytesRef(highlightText.bytes().bytes()));
            }
            if (multivalued) {
                builder.endPositionEntry();
            }
        }
    }

    private static Supplier<Source> lazyStoredSourceLoader(LeafReaderContext ctx, int doc) {
        return () -> {
            StoredFieldLoader rootLoader = StoredFieldLoader.create(true, Collections.emptySet());
            try {
                LeafStoredFieldLoader leafRootLoader = rootLoader.getLoader(ctx, null);
                leafRootLoader.advanceTo(doc);
                return Source.fromBytes(leafRootLoader.source());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    @Override
    protected void appendNoMatch(BytesRefBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    public Block eval(Page page) {
        return executeQuery(page);
    }

    public record Factory(
        ShardConfig[] shardConfigs,
        String fieldName,
        Integer numFragments,
        Integer fragmentSize,
        SearchContext searchContext,
        Map<String, Highlighter> highlighters
    ) implements EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new HighlighterExpressionEvaluator(
                context.blockFactory(),
                shardConfigs,
                fieldName,
                numFragments,
                fragmentSize,
                searchContext,
                highlighters
            );
        }
    }
}
