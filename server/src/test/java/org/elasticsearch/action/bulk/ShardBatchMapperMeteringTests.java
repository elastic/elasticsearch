/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
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
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.MeteringModel;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies that the columnar batch path meters documents exactly like the sequential path: the batch is charged in one sweep
 * under the provider's {@link MeteringModel}, with the accounting its {@link XContentMeteringParserDecorator} applies to
 * the tokens of a row-parsed document, and the resulting normalized size travels with the {@link IndexOperationBatch}, so a
 * {@code DocumentSizeReporter} sees the same value whichever path indexed the document.
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

    /** Time series is the only non strict-columnar mode the batch path accepts, and its root keeps subobjects enabled. */
    private static final Settings TIME_SERIES_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        IndexSettings.MODE.getKey(),
        IndexMode.TIME_SERIES.getName()
    )
        .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
        .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
        .build();

    /** Root subobjects:true, so dotted names expand; metrics does not. */
    private static final String SUBOBJECTS_MAPPING = """
        {
          "_data_stream_timestamp": { "enabled": true },
          "dynamic": "strict",
          "properties": {
            "@timestamp": { "type": "date" },
            "dim":     { "type": "keyword", "time_series_dimension": true },
            "title":   { "type": "keyword" },
            "obj":     { "properties": { "name": { "type": "keyword" } } },
            "metrics": { "subobjects": false, "properties": { "time.max": { "type": "long" } } }
          }
        }""";

    /**
     * An arbitrary but deterministic accounting: the UTF-8 bytes of strings, 3 per number, 2 per boolean, and nothing below the
     * root fields {@code obj} and {@code metrics}, whichever way a document spells them.
     */
    private static final MeteringModel TEST_MODEL = new MeteringModel() {
        private final Set<String> excludedRootFields = Set.of("obj", "metrics");

        @Override
        public boolean hasExcludedRootField(String fieldName) {
            final int dot = fieldName.indexOf('.');
            return excludedRootFields.contains(dot < 0 ? fieldName : fieldName.substring(0, dot));
        }

        @Override
        public long stringBytes(int utf8Length) {
            return utf8Length;
        }

        @Override
        public long binaryBytes(int length) {
            return length;
        }

        @Override
        public long numberBytes() {
            return 3;
        }

        @Override
        public long booleanBytes() {
            return 2;
        }
    };

    /** {@link #TEST_MODEL} applied to the tokens of a row-parsed document, publishing the total only once the parser is closed. */
    private static final class SizeOnCloseDecorator implements XContentMeteringParserDecorator {
        private long inFlight = 0;
        private long published = UNKNOWN_SIZE;
        private int depth = 0;
        private boolean charged = true;

        @Override
        public XContentParser decorate(XContentParser xContentParser, Mapping mapping) {
            return new FilterXContentParserWrapper(xContentParser) {
                @Override
                public Token nextToken() throws IOException {
                    final Token token = super.nextToken();
                    if (token == null) {
                        return null;
                    }
                    switch (token) {
                        case START_OBJECT, START_ARRAY -> depth++;
                        case END_OBJECT, END_ARRAY -> depth--;
                        case FIELD_NAME -> {
                            if (depth == 1) {
                                // a dotted root name is only expanded below an enabled root object; the model cuts it at the first dot
                                charged = TEST_MODEL.hasExcludedRootField(currentName()) == false;
                            }
                        }
                        case VALUE_STRING -> {
                            if (charged) {
                                final String text = text();
                                inFlight += TEST_MODEL.stringBytes(UnicodeUtil.calcUTF16toUTF8Length(text, 0, text.length()));
                            }
                        }
                        case VALUE_NUMBER -> inFlight += charged ? TEST_MODEL.numberBytes() : 0;
                        case VALUE_BOOLEAN -> inFlight += charged ? TEST_MODEL.booleanBytes() : 0;
                        case VALUE_EMBEDDED_OBJECT, VALUE_NULL -> {
                        }
                        default -> throw new AssertionError(token);
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
        public XContentMeteringParserDecorator newMeteringParserDecorator() {
            return new SizeOnCloseDecorator();
        }

        @Override
        public MeteringModel newMeteringModel() {
            return TEST_MODEL;
        }
    };

    private IndexShard newColumnarShard() throws IOException {
        return newPrimaryShard(COLUMNAR_SETTINGS, MAPPING);
    }

    private IndexShard newPrimaryShard(Settings settings, String mapping) throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index").putMapping(mapping).settings(settings).primaryTerm(0, 1).build();
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

    /** Documents for {@link #SUBOBJECTS_MAPPING}; the metrics spelling is fixed per batch because mixing spellings falls back. */
    private static List<BytesReference> randomSubobjectsDocuments(int numDocs) throws IOException {
        final int metricsSpelling = randomIntBetween(0, 2);
        final boolean dottedObjectKey = randomBoolean();
        final List<BytesReference> sources = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            try (XContentBuilder b = XContentFactory.jsonBuilder()) {
                b.startObject();
                b.field("@timestamp", "2021-04-28T12:00:00Z");
                b.field("dim", randomAlphaOfLength(3));
                b.field("title", randomRealisticUnicodeOfLengthBetween(1, 30));
                if (dottedObjectKey) {
                    b.field("obj.name", randomAlphaOfLengthBetween(1, 10));
                } else {
                    b.startObject("obj").field("name", randomAlphaOfLengthBetween(1, 10)).endObject();
                }
                final long max = randomLong();
                switch (metricsSpelling) {
                    case 0 -> b.startObject("metrics").field("time.max", max).endObject();
                    case 1 -> b.field("metrics.time.max", max);
                    case 2 -> b.startObject("metrics").startObject("time").field("max", max).endObject().endObject();
                    default -> throw new AssertionError(metricsSpelling);
                }
                b.endObject();
                sources.add(BytesReference.bytes(b));
            }
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

    /** Time series requests carry the routing hash and the tsid the coordinating node computes; the id is derived from them. */
    private static BulkItemRequest[] timeSeriesItems(IndexShard shard, List<BytesReference> sources) {
        final var routing = (IndexRouting.ExtractFromSource.ForIndexDimensions) shard.indexSettings().getIndexRouting();
        final BulkItemRequest[] items = new BulkItemRequest[sources.size()];
        for (int i = 0; i < items.length; i++) {
            final IndexRequest request = new IndexRequest("index").source(sources.get(i), XContentType.JSON)
                .routing(TimeSeriesRoutingHashFieldMapper.encode(randomInt()))
                .tsid(routing.buildTsid(XContentType.JSON, sources.get(i)));
            items[i] = new BulkItemRequest(i, request);
        }
        return items;
    }

    /** The size the sequential path reports for {@code request}: parse it through {@code DocumentParser} with a fresh decorator. */
    private static long sequentiallyMeteredSize(IndexShard shard, IndexRequest request) {
        final SourceToParse sourceToParse = new SourceToParse(
            request.id(),
            new BytesSource(request.source(), request.getContentType(), false),
            request.routing(),
            Map.of(),
            Map.of(),
            METERING_PROVIDER.newMeteringParserDecorator(),
            request.tsid()
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
            final int numDocs = randomIntBetween(1, 100);
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

    public void testExcludedRootFieldsAcrossSpellings() throws IOException {
        IndexShard shard = newPrimaryShard(TIME_SERIES_SETTINGS, SUBOBJECTS_MAPPING);
        try {
            final int numDocs = randomIntBetween(1, 100);
            final List<BytesReference> sources = randomSubobjectsDocuments(numDocs);
            final BulkItemRequest[] items = timeSeriesItems(shard, sources);
            final long[] expected = new long[numDocs];
            for (int i = 0; i < numDocs; i++) {
                expected[i] = sequentiallyMeteredSize(shard, (IndexRequest) items[i].request());
            }

            try (
                SourceBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
                EngineBatch engineBatch = mapBatch(shard, items, batch, METERING_PROVIDER)
            ) {
                assertNotNull("expected the columnar path to be taken", engineBatch);
                for (int i = 0; i < numDocs; i++) {
                    assertThat(engineBatch.batch().normalizedSize(i), equalTo(expected[i]));
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
