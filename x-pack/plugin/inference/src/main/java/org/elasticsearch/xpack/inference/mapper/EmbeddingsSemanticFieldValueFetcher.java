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
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

class EmbeddingsSemanticFieldValueFetcher extends ChildDocIteratingValueFetcher {
    protected final SourceLoader.SyntheticFieldLoader embeddingsFieldLoader;
    private SourceLoader.SyntheticFieldLoader.DocValuesLoader dvLoader;

    EmbeddingsSemanticFieldValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        Function<Query, BitSetProducer> bitSetCache,
        IndexSearcher searcher
    ) {
        super(fieldType, bitSetCache, searcher);
        this.embeddingsFieldLoader = fieldType.getEmbeddingsField() != null ? fieldType.getEmbeddingsField().syntheticFieldLoader() : null;
    }

    @Override
    protected void setNextReaderHook(LeafReaderContext context) throws IOException {
        if (embeddingsFieldLoader != null) {
            dvLoader = embeddingsFieldLoader.docValuesLoader(context.reader(), null);
        }
    }

    @Override
    protected boolean loadOffsets() {
        return false;
    }

    @Override
    protected void onAdvanceChildDoc(int childDocId) throws IOException {
        if (dvLoader == null || dvLoader.advanceToDoc(childDocId) == false) {
            throw new IllegalStateException(
                "Cannot fetch values for field [" + fieldType.name() + "], missing embeddings for doc [" + childDocId + "]"
            );
        }
    }

    @Override
    protected List<Object> doFetchValues(Source source, int doc, DocIdSetIterator it) throws IOException {
        List<Object> embeddings = new ArrayList<>();
        iterateChildDocs(doc, it, () -> embeddings.add(rawEmbeddings(embeddingsFieldLoader::write, source.sourceContentType())));
        return embeddings;
    }

    protected static BytesReference rawEmbeddings(CheckedConsumer<XContentBuilder, IOException> writer, XContentType xContentType)
        throws IOException {
        try (var result = XContentFactory.contentBuilder(xContentType)) {
            try (var builder = XContentFactory.contentBuilder(xContentType)) {
                builder.startObject();
                writer.accept(builder);
                builder.endObject();
                try (
                    XContentParser parser = XContentHelper.createParserNotCompressed(
                        XContentParserConfiguration.EMPTY,
                        BytesReference.bytes(builder),
                        xContentType
                    )
                ) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
                    parser.nextToken();
                    result.copyCurrentStructure(parser);
                }
                return BytesReference.bytes(result);
            }
        }
    }
}
