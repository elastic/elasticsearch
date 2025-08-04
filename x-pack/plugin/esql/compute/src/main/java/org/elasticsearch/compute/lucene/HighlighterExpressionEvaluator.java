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
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class HighlighterExpressionEvaluator extends LuceneQueryEvaluator<BytesRefVector.Builder>
    implements
        EvalOperator.ExpressionEvaluator {

    private final String fieldName;
    private final SearchContext searchContext;
    private final SourceLoader sourceLoader;

    HighlighterExpressionEvaluator(
        BlockFactory blockFactory,
        ShardConfig[] shardConfigs,
        String fieldName,
        SearchContext searchContext,
        SourceLoader sourceLoader
    ) {
        super(blockFactory, shardConfigs);
        this.fieldName = fieldName;
        this.searchContext = searchContext;
        this.sourceLoader = sourceLoader;
    }

    @Override
    protected ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    protected Vector createNoMatchVector(BlockFactory blockFactory, int size) {
        return blockFactory.newConstantBytesRefVector(new BytesRef(), size);
    }

    @Override
    protected BytesRefVector.Builder createVectorBuilder(BlockFactory blockFactory, int size) {
        return blockFactory.newBytesRefVectorBuilder(size);
    }

    @Override
    protected void appendMatch(BytesRefVector.Builder builder, Scorable scorer, int docId, LeafReaderContext leafReaderContext, Query query)
        throws IOException {

        // I was trying to find the way to build the highligher from the context, but probably we should just build the
        // CustomUnifiedHighligher directly so we don't need specific fetch phase classes for this
        SearchHighlightContext.FieldOptions.Builder optionsBuilder = new SearchHighlightContext.FieldOptions.Builder();
        optionsBuilder.numberOfFragments(10);
        optionsBuilder.fragmentCharSize(100);
        SearchHighlightContext.Field field = new SearchHighlightContext.Field(fieldName, optionsBuilder.build());
        FetchContext fetchContext = new FetchContext(searchContext, sourceLoader);
        MappedFieldType fieldType = searchContext.getSearchExecutionContext().getFieldType(fieldName);
        SearchHit searchHit = new SearchHit(docId);
        Source source = Source.lazy(lazyStoredSourceLoader(leafReaderContext, docId));


        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(
            searchHit,
            leafReaderContext,
            docId,
            Map.of(),
            source,
            null
        );
        FieldHighlightContext highlightContext = new FieldHighlightContext(
            fieldName,
            field,
            fieldType,
            fetchContext,
            hitContext,
            query,
            Map.of()
        );
        Highlighter highlighter = new DefaultHighlighter();
        HighlightField highlight = highlighter.highlight(highlightContext);

        // Iterate over fragments etc
        builder.appendBytesRef(new BytesRef(highlight.fragments()[0].bytes().bytes()));
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
    protected void appendNoMatch(BytesRefVector.Builder builder) {


    }

    @Override
    public Block eval(Page page) {
        return executeQuery(page);
    }

    public record Factory(ShardConfig[] shardConfigs) implements EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            // We need to get field name, search context, and source loader. We should be able to remove the source loader by getting the field value
            return new HighlighterExpressionEvaluator(context.blockFactory(), shardConfigs, fieldName, searchContext, context.sourceLoader());
        }
    }
}
