/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getOffsetsFieldName;

abstract class ChildDocIteratingValueFetcher implements ValueFetcher {
    protected final SemanticFieldMapper.SemanticFieldType fieldType;

    private final BitSetProducer bitSetProducer;
    private final IndexSearcher searcher;

    private Weight childWeight;
    private BitSet bitSet;
    private Scorer childScorer;
    protected OffsetSourceField.OffsetSourceLoader offsetsLoader;

    ChildDocIteratingValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        Function<Query, BitSetProducer> bitSetCache,
        IndexSearcher searcher
    ) {
        this.fieldType = fieldType;
        this.bitSetProducer = bitSetCache.apply(fieldType.getChunksField().parentTypeFilter());
        this.searcher = searcher;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        try {
            bitSet = bitSetProducer.getBitSet(context);
            childScorer = getChildScorer(context);
            if (childScorer != null) {
                childScorer.iterator().nextDoc();
            }

            setNextReaderHook(context);

            var terms = context.reader().terms(getOffsetsFieldName(fieldType.name()));
            offsetsLoader = terms != null ? OffsetSourceField.loader(terms) : null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Hook for subclasses to perform additional per-leaf initialization after the shared state is set up.
     */
    protected void setNextReaderHook(LeafReaderContext context) throws IOException {}

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        if (fieldType.getModelSettings() == null || childScorer == null || offsetsLoader == null || doc == 0) {
            return List.of();
        }

        int previousParent = bitSet.prevSetBit(doc - 1);
        var it = childScorer.iterator();
        if (it.docID() < previousParent) {
            it.advance(previousParent);
        }

        return doFetchValues(source, doc, it);
    }

    protected abstract List<Object> doFetchValues(Source source, int doc, DocIdSetIterator it) throws IOException;

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }

    protected void iterateChildDocs(int doc, DocIdSetIterator it, CheckedConsumer<OffsetSourceFieldMapper.OffsetSource, IOException> action)
        throws IOException {
        while (it.docID() < doc) {
            onAdvanceChildDoc(it.docID());

            var offset = offsetsLoader.advanceTo(it.docID(), fieldType.getChunksField().indexSettings().getIndexVersionCreated());
            if (offset == null) {
                throw new IllegalStateException(
                    "Cannot fetch values for field [" + fieldType.name() + "], missing offsets for doc [" + doc + "]"
                );
            }

            action.accept(offset);

            if (it.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
        }
    }

    /**
     * Hook called for each child doc before the offset is read. Subclasses may override to load per-child state.
     */
    protected void onAdvanceChildDoc(int childDocId) throws IOException {}

    private Scorer getChildScorer(LeafReaderContext context) throws IOException {
        if (childWeight == null) {
            childWeight = searcher.createWeight(fieldType.getChunksField().nestedTypeFilter(), ScoreMode.COMPLETE_NO_SCORES, 1);
        }

        return childWeight.scorer(context);
    }
}
