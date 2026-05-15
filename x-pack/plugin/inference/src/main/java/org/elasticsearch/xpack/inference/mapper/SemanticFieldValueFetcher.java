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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getOffsetsFieldName;

class SemanticFieldValueFetcher implements ValueFetcher {
    enum Mode {
        TEXT,
        FULL_FIELD
    }

    private final SemanticFieldMapper.SemanticFieldType fieldType;
    private final BitSetProducer bitSetProducer;
    private final SourceLoader.SyntheticFieldLoader embeddingsFieldLoader;
    private final IndexSearcher searcher;
    private final Mode mode;

    private Weight childWeight;
    private BitSet bitSet;
    private Scorer childScorer;
    private SourceLoader.SyntheticFieldLoader.DocValuesLoader dvLoader;
    private OffsetSourceField.OffsetSourceLoader offsetsLoader;

    SemanticFieldValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        Function<Query, BitSetProducer> bitSetCache,
        IndexSearcher searcher,
        Mode mode
    ) {
        this.fieldType = fieldType;
        this.bitSetProducer = bitSetCache.apply(fieldType.getChunksField().parentTypeFilter());
        this.embeddingsFieldLoader = fieldType.getEmbeddingsField() != null ? fieldType.getEmbeddingsField().syntheticFieldLoader() : null;
        this.searcher = searcher;
        this.mode = mode;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        if (embeddingsFieldLoader == null) {
            return;
        }

        try {
            bitSet = bitSetProducer.getBitSet(context);
            childScorer = getChildScorer(context);
            if (childScorer != null) {
                childScorer.iterator().nextDoc();
            }

            if (mode == Mode.FULL_FIELD) {
                dvLoader = embeddingsFieldLoader.docValuesLoader(context.reader(), null);
            }

            var terms = context.reader().terms(getOffsetsFieldName(fieldType.name()));
            offsetsLoader = terms != null ? OffsetSourceField.loader(terms) : null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        if (embeddingsFieldLoader == null || childScorer == null || offsetsLoader == null || doc == 0) {
            return List.of();
        }

        int previousParent = bitSet.prevSetBit(doc - 1);
        var it = childScorer.iterator();
        if (it.docID() < previousParent) {
            it.advance(previousParent);
        }

        return mode == Mode.TEXT ? fetchTextChunks(source, doc, it) : fetchFullField(source, doc, it);
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }

    private List<Object> fetchTextChunks(Source source, int doc, DocIdSetIterator it) throws IOException {
        Map<String, String> originalValueMap = new HashMap<>();
        List<Object> chunks = new ArrayList<>();

        iterateChildDocs(doc, it, offset -> {
            if (offset.inputIndex() != null) {
                // TODO: Implement input index handling
                throw new UnsupportedOperationException("Input index-based text extraction is not supported");
            }
            var rawValue = originalValueMap.computeIfAbsent(offset.field(), k -> {
                var valueObj = XContentMapValues.extractValue(offset.field(), source.source(), null);
                var values = SemanticTextUtils.nodeStringValues(offset.field(), valueObj);
                return Strings.collectionToDelimitedString(values, String.valueOf(MULTIVAL_SEP_CHAR));
            });

            chunks.add(rawValue.substring(offset.start(), offset.end()));
        });

        return chunks;
    }

    private List<Object> fetchFullField(Source source, int doc, DocIdSetIterator it) throws IOException {
        Map<String, List<SemanticTextField.Chunk>> chunkMap = new LinkedHashMap<>();

        iterateChildDocs(doc, it, offset -> {
            var fullChunks = chunkMap.computeIfAbsent(offset.field(), k -> new ArrayList<>());
            var rawEmbeddings = rawEmbeddings(embeddingsFieldLoader::write, source.sourceContentType());
            fullChunks.add(
                offset.inputIndex() != null
                    ? new SemanticTextField.Chunk(offset.inputIndex(), rawEmbeddings)
                    : new SemanticTextField.Chunk(offset.start(), offset.end(), rawEmbeddings)
            );
        });

        if (chunkMap.isEmpty()) {
            return List.of();
        }

        return List.of(
            new SemanticTextField(
                false,
                fieldType.name(),
                null,
                new SemanticTextField.InferenceResult(
                    fieldType.getInferenceId(),
                    fieldType.getModelSettings(),
                    fieldType.getChunkingSettings(),
                    chunkMap
                ),
                source.sourceContentType()
            )
        );
    }

    private void iterateChildDocs(int doc, DocIdSetIterator it, CheckedConsumer<OffsetSourceFieldMapper.OffsetSource, IOException> action)
        throws IOException {
        while (it.docID() < doc) {
            if (mode == Mode.FULL_FIELD) {
                if (dvLoader == null || dvLoader.advanceToDoc(it.docID()) == false) {
                    throw new IllegalStateException(
                        "Cannot fetch values for field [" + fieldType.name() + "], missing embeddings for doc [" + doc + "]"
                    );
                }
            }

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

    private static BytesReference rawEmbeddings(CheckedConsumer<XContentBuilder, IOException> writer, XContentType xContentType)
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

    private Scorer getChildScorer(LeafReaderContext context) throws IOException {
        if (childWeight == null) {
            childWeight = searcher.createWeight(fieldType.getChunksField().nestedTypeFilter(), ScoreMode.COMPLETE_NO_SCORES, 1);
        }

        return childWeight.scorer(context);
    }
}
