/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.mapper.BytesSource;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies that the columnar batch path meters documents exactly like the sequential path: every row is streamed through
 * the request's {@link XContentMeteringParserDecorator} and the resulting normalized size travels with the
 * {@link IndexOperationBatch}, so a {@code DocumentSizeReporter} sees the same value whichever path indexed the document.
 */
public class ShardBatchMapperMeteringTests extends IndexShardTestCase {

    private static final Settings COLUMNAR_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        IndexSettings.MODE.getKey(),
        IndexMode.COLUMNAR.getName()
    ).put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true).build();

    private static final String MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "title": { "type": "keyword" },
            "count": { "type": "long" },
            "flag":  { "type": "boolean" },
            "obj":   { "properties": { "name": { "type": "keyword" } } }
          }
        }""";

    /**
     * Moack the serverless metering decorator: charges the UTF-8 length of string values, 8 bytes per number and
     * 1 byte per boolean, and — like the production decorator — only publishes the total once the parser is closed.
     */
    private static final class SizeOnCloseDecorator implements XContentMeteringParserDecorator {
        private long inFlight = 0;
        private long published = UNKNOWN_SIZE;

        @Override
        public XContentParser decorate(XContentParser xContentParser, Mapping mapping) {
            return new FilterXContentParserWrapper(xContentParser) {
                @Override
                public Token nextToken() throws IOException {
                    final Token token = super.nextToken();
                    if (token == Token.VALUE_STRING) {
                        inFlight += text().getBytes(StandardCharsets.UTF_8).length;
                    } else if (token == Token.VALUE_NUMBER) {
                        inFlight += Long.BYTES;
                    } else if (token == Token.VALUE_BOOLEAN) {
                        inFlight += 1;
                    }
                    return token;
                }

                @Override
                public void close() throws IOException {
                    published = inFlight;
                    super.close();
                }
            };
        }

        @Override
        public long meteredDocumentSize() {
            return published;
        }
    }

    private static final DocumentParsingProvider METERING_PROVIDER = new DocumentParsingProvider() {
        @Override
        public <T> XContentMeteringParserDecorator newMeteringParserDecorator(IndexRequest request) {
            return new SizeOnCloseDecorator();
        }
    };

    private IndexShard newColumnarShard() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index").putMapping(MAPPING).settings(COLUMNAR_SETTINGS).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(shard);
        return shard;
    }

    /**
     * A document mixing strings (including non-ASCII), numbers, booleans and absent fields.
     */
    private static BytesReference randomDocument(boolean dottedObjectKey) throws IOException {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            b.field("title", randomRealisticUnicodeOfLengthBetween(1, 30));
            if (randomBoolean()) {
                b.field("count", randomLong());
            }
            if (randomBoolean()) {
                b.field("flag", randomBoolean());
            }
            if (dottedObjectKey) {
                b.field("obj.name", randomAlphaOfLengthBetween(1, 10));
            } else {
                b.startObject("obj").field("name", randomAlphaOfLengthBetween(1, 10)).endObject();
            }
            b.endObject();
            return BytesReference.bytes(b);
        }
    }

    private static List<BytesReference> randomDocuments(int numDocs) throws IOException {
        final boolean dottedObjectKey = randomBoolean();
        final List<BytesReference> sources = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            sources.add(randomDocument(dottedObjectKey));
        }
        return sources;
    }

    private static BulkItemRequest[] items(List<BytesReference> sources) {
        final BulkItemRequest[] items = new BulkItemRequest[sources.size()];
        for (int i = 0; i < items.length; i++) {
            items[i] = new BulkItemRequest(i, new IndexRequest("index").id("doc-" + i).source(sources.get(i), XContentType.JSON));
        }
        return items;
    }

    /** The size the sequential path reports for {@code request}: parse it through {@code DocumentParser} with a fresh decorator. */
    private static long sequentiallyMeteredSize(IndexShard shard, IndexRequest request) {
        final SourceToParse sourceToParse = new SourceToParse(
            request.id(),
            new BytesSource(request.source(), request.getContentType(), false),
            null,
            Map.of(),
            Map.of(),
            METERING_PROVIDER.newMeteringParserDecorator(request),
            null
        );
        return shard.mapperService()
            .documentParser()
            .parseDocument(sourceToParse, shard.mapperService().mappingLookup())
            .getNormalizedSize();
    }

    /** Helper to take the batch path **/
    private static EngineBatch mapBatch(IndexShard shard, BulkItemRequest[] items, SourceBatch batch, DocumentParsingProvider provider) {
        final BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            batch,
            shard.mapperService().mappingLookup(),
            shard.indexSettings()
        );
        assertNotNull("expected the columnar path to be taken", resolution);
        return ShardBatchMapper.mapColumnBatch(
            items,
            batch,
            shard,
            0,
            items.length,
            resolution,
            Engine.Operation.Origin.PRIMARY,
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            provider
        );
    }

    public void testBatchRowsReportTheSameNormalizedSizeAsSequentialParsing() throws IOException {
        IndexShard shard = newColumnarShard();
        try {
            final int numDocs = randomIntBetween(1, 10);
            final List<BytesReference> sources = randomDocuments(numDocs);
            final BulkItemRequest[] items = items(sources);
            final long[] expected = new long[numDocs];
            for (int i = 0; i < numDocs; i++) {
                expected[i] = sequentiallyMeteredSize(shard, (IndexRequest) items[i].request());
                assertThat(expected[i], greaterThan(0L));
            }

            try (
                SourceBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
                EngineBatch engineBatch = mapBatch(shard, items, batch, METERING_PROVIDER)
            ) {
                assertNotNull("expected the columnar path to be taken", engineBatch);
                final IndexOperationBatch indexBatch = engineBatch.batch();
                for (int i = 0; i < numDocs; i++) {
                    assertThat(indexBatch.normalizedSize(i), equalTo(expected[i]));
                    assertThat(indexBatch.toIndexOp(i).parsedDoc().getNormalizedSize(), equalTo(expected[i]));
                }
                if (numDocs > 1) {
                    final int from = randomIntBetween(1, numDocs - 1);
                    assertThat(indexBatch.slice(from, numDocs).normalizedSize(0), equalTo(expected[from]));
                }
            }
        } finally {
            closeShards(shard);
        }
    }

    public void testRowsAreNotMeteredWithoutAMeteringProvider() throws IOException {
        IndexShard shard = newColumnarShard();
        try {
            final int numDocs = randomIntBetween(1, 5);
            final List<BytesReference> sources = randomDocuments(numDocs);
            final BulkItemRequest[] items = items(sources);
            try (
                SourceBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
                EngineBatch engineBatch = mapBatch(shard, items, batch, DocumentParsingProvider.EMPTY_INSTANCE)
            ) {
                assertNotNull("expected the columnar path to be taken", engineBatch);
                for (int i = 0; i < numDocs; i++) {
                    assertThat(engineBatch.batch().normalizedSize(i), equalTo(XContentMeteringParserDecorator.UNKNOWN_SIZE));
                    assertThat(
                        engineBatch.batch().toIndexOp(i).parsedDoc().getNormalizedSize(),
                        equalTo(XContentMeteringParserDecorator.UNKNOWN_SIZE)
                    );
                }
            }
        } finally {
            closeShards(shard);
        }
    }
}
