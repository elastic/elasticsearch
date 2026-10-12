/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.inference.EndpointClusterState;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.function.Function;

/**
 * Base class for fetchers that need the per-chunk embeddings of a {@code semantic}/{@code semantic_text} field.
 * <p>
 * It owns the machinery for recovering those embeddings: the embeddings subfield's synthetic field loader and the
 * per-leaf doc values loader that it is advanced through as the chunk child docs are iterated. Subclasses decide
 * what shape the embeddings are exposed in (parsed values versus the raw {@code _source} representation) and
 * whether the chunk offsets are needed alongside them.
 * </p>
 */
abstract class AbstractEmbeddingsLoadingValueFetcher extends ChildDocIteratingValueFetcher {
    protected final SourceLoader.SyntheticFieldLoader embeddingsFieldLoader;
    private SourceLoader.SyntheticFieldLoader.DocValuesLoader dvLoader;

    AbstractEmbeddingsLoadingValueFetcher(
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
    protected void onAdvanceChildDoc(int childDocId) throws IOException {
        if (dvLoader == null || dvLoader.advanceToDoc(childDocId) == false) {
            throw new IllegalStateException(
                "Cannot fetch values for field [" + fieldType.name() + "], missing embeddings for doc [" + childDocId + "]"
            );
        }
    }

    protected static Object readParsedEmbeddings(
        CheckedConsumer<XContentBuilder, IOException> writer,
        XContentType xContentType,
        SemanticFieldMapper.SemanticFieldType fieldType
    ) throws IOException {
        return readEmbeddings(writer, xContentType, parser -> parseEmbeddings(parser, fieldType));
    }

    /**
     * Parses the embeddings value that {@code parser} is currently positioned on into the shape expected for the field's task type:
     * a {@code float[]} for dense vectors, or a {@code Map<String, Float>} for sparse vectors.
     */
    protected static Object parseEmbeddings(XContentParser parser, SemanticFieldMapper.SemanticFieldType fieldType) throws IOException {
        // fetchValues short-circuits on null model settings, so they are set by the time we get here
        EndpointClusterState modelSettings = fieldType.getModelSettings();
        return switch (modelSettings.taskType()) {
            // Byte vectors can be represented exactly as float vectors
            case TEXT_EMBEDDING, EMBEDDING -> VectorData.parseXContent(parser).asFloatVector();
            case SPARSE_EMBEDDING -> parser.map(LinkedHashMap::new, XContentParser::floatValue);
            default -> throw new IllegalStateException(
                "Field ["
                    + fieldType.name()
                    + "] is configured to use an inference endpoint with an unsupported task type ["
                    + modelSettings.taskType()
                    + "]"
            );
        };
    }

    protected static BytesReference readRawEmbeddings(CheckedConsumer<XContentBuilder, IOException> writer, XContentType xContentType)
        throws IOException {
        return readEmbeddings(writer, xContentType, parser -> {
            try (var result = XContentFactory.contentBuilder(xContentType)) {
                result.copyCurrentStructure(parser);
                return BytesReference.bytes(result);
            }
        });
    }

    protected static <T> T readEmbeddings(
        CheckedConsumer<XContentBuilder, IOException> writer,
        XContentType xContentType,
        CheckedFunction<XContentParser, T, IOException> reader
    ) throws IOException {
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
                advanceToEmbeddingsValue(parser);
                return reader.apply(parser);
            }
        }
    }

    /**
     * Advances {@code parser} past the wrapping object and its field name, leaving it positioned on the embeddings value.
     */
    protected static void advanceToEmbeddingsValue(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        parser.nextToken();
    }
}
