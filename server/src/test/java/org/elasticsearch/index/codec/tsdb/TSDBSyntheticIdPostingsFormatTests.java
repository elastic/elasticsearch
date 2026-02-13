/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.codec.bloomfilter.LazyFilterTermsEnum;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdFieldsProducer.SyntheticIdTermsEnum;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.time.FormatNames.STRICT_DATE_OPTIONAL_TIME;
import static org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper.createSyntheticIdBytesRef;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TSDBSyntheticIdPostingsFormatTests extends ESTestCase {

    /**
     * Represents a time-series document
     *
     * Note: if version > 1, the document is processed as a soft-update of a previously indexed document
     */
    public record Doc(long timestamp, String hostName, String metricField, Integer metricValue, int version, int routing) {}

    public void testTerms() throws IOException {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        runTest((writer, parser) -> {

            final var now = Instant.now();
            final int routing = randomNonNegativeInt();

            final int segments = randomInt(7);
            final int[] docsPerSegments = new int[segments];

            // About documents in this test:
            // - multiple documents can have the same _tsid
            // - documents have unique timestamps within each segment, ensuring unique synthetic id terms per segment
            // - documents all have the same routing hash since they are supposed to be on the same shard
            for (int segment = 0; segment < segments; segment++) {
                docsPerSegments[segment] = randomIntBetween(1, 100);
                for (int doc = 0; doc < docsPerSegments[segment]; doc++) {
                    writer.addDocument(
                        parser.parse(
                            new Doc(
                                randomBoolean()
                                    ? now.minus(doc, ChronoUnit.MINUTES).toEpochMilli()
                                    : now.plus(doc, ChronoUnit.MINUTES).toEpochMilli(),
                                "vm-dev0" + randomInt(9),
                                "cpu-load",
                                randomInt(),
                                1,
                                routing
                            )
                        )
                    );
                }
                writer.flush();
            }

            try (var reader = DirectoryReader.open(writer)) {
                assertThat(reader.leaves(), hasSize(segments));

                for (int i = 0; i < reader.leaves().size(); i++) {
                    var leafReader = reader.leaves().get(i).reader();
                    assertThat(leafReader, notNullValue());

                    var terms = leafReader.terms(IdFieldMapper.NAME);
                    assertThat(terms, notNullValue());

                    assertThat(terms.hasPositions(), equalTo(false));
                    assertThat(terms.hasPayloads(), equalTo(false));
                    assertThat(terms.hasOffsets(), equalTo(false));
                    assertThat(terms.hasFreqs(), equalTo(false));

                    assertThat(terms.size(), equalTo(-1L));
                    assertThat(terms.getSumTotalTermFreq(), equalTo(0L));
                    assertThat(terms.getSumDocFreq(), equalTo(0L));
                    assertThat(terms.getDocCount(), equalTo(docsPerSegments[i]));

                    var lazyTermsEnum = terms.iterator();
                    assertThat(lazyTermsEnum, notNullValue());
                    assertThat(lazyTermsEnum, instanceOf(LazyFilterTermsEnum.class));
                    var unwrappedTermsEnum = LazyFilterTermsEnum.unwrap(lazyTermsEnum);
                    assertThat(unwrappedTermsEnum, instanceOf(SyntheticIdTermsEnum.class));
                    var termsEnum = randomFrom(lazyTermsEnum, unwrappedTermsEnum);

                    assertThat(termsEnum.impacts(0), nullValue());
                    assertThat(termsEnum.docFreq(), equalTo(0));
                    assertThat(termsEnum.totalTermFreq(), equalTo(0L));

                    expectThrows(UnsupportedOperationException.class, termsEnum::ord);
                    expectThrows(UnsupportedOperationException.class, () -> termsEnum.seekExact(0L));

                    PostingsEnum reuse = null;
                    BytesRef previous = null;
                    BytesRef current;
                    while ((current = termsEnum.next()) != null) {
                        assertThat(current, equalTo(termsEnum.term()));
                        if (previous != null) {
                            assertThat(current.compareTo(previous), greaterThan(0));
                        }
                        previous = termsEnum.term();

                        if (randomBoolean()) {
                            var postings = termsEnum.postings(reuse);
                            assertThat(termsEnum, notNullValue());
                            assertThat(postings.docID(), equalTo(-1));

                            var docId = postings.nextDoc();
                            assertThat(postings.docID(), equalTo(docId));

                            assertThat(postings.cost(), equalTo(0L));
                            assertThat(postings.freq(), equalTo(0));
                            assertThat(postings.nextPosition(), equalTo(-1));
                            assertThat(postings.startOffset(), equalTo(-1));
                            assertThat(postings.endOffset(), equalTo(-1));
                            assertThat(postings.getPayload(), nullValue());

                            // Each term is unique within a segment, so only 1 expected posting value
                            var advance = randomBoolean() ? postings.nextDoc() : postings.advance(docId + 1);
                            assertThat(advance, equalTo(DocIdSetIterator.NO_MORE_DOCS));

                            if (randomBoolean()) {
                                reuse = postings;
                            }
                        }
                    }
                }
            }
        });
    }

    public void testSeek() throws IOException {
        assumeTrue("Test should only run with feature flag", IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG);
        runTestWithRandomDocs((writer, finalDocs) -> {
            try (var reader = DirectoryReader.open(writer)) {
                assertThat(reader.getDocCount(IdFieldMapper.NAME), equalTo(finalDocs.values().stream().mapToInt(Doc::version).sum()));
                assertThat(reader.getIndexCommit().getSegmentCount(), equalTo(1));
                assertThat(reader.leaves().size(), equalTo(1));
                final var leafReader = reader.leaves().getFirst().reader();

                // Shuffle docs synthetic ids before seeking
                final var docsIdsTerms = new ArrayList<>(finalDocs.keySet());
                Collections.shuffle(docsIdsTerms, random());

                final Terms terms = leafReader.terms(IdFieldMapper.NAME);
                assertNotNull(terms);
                final TermsEnum wrappedTermsEnum = asInstanceOf(LazyFilterTermsEnum.class, terms.iterator());
                final TermsEnum unwrappedTermsEnum = asInstanceOf(SyntheticIdTermsEnum.class, LazyFilterTermsEnum.unwrap(wrappedTermsEnum));

                TermsEnum termsEnum = unwrappedTermsEnum;

                // Test seek the exact term
                {
                    for (var docIdTerm : docsIdsTerms) {
                        if (rarely()) {
                            termsEnum = randomFrom(random(), () -> unwrappedTermsEnum, () -> wrappedTermsEnum, () -> {
                                try {
                                    return leafReader.terms(IdFieldMapper.NAME).iterator();
                                } catch (IOException e) {
                                    throw new AssertionError(e);
                                }
                            });
                        }
                        switch (randomInt(2)) {
                            case 0:
                                assertThat(termsEnum.seekCeil(docIdTerm), equalTo(TermsEnum.SeekStatus.FOUND));
                                break;
                            case 1:
                                assertThat(termsEnum.seekExact(docIdTerm), equalTo(true));
                                break;
                            case 2:
                                termsEnum.seekExact(docIdTerm, new TermState() {
                                    @Override
                                    public void copyFrom(TermState other) {
                                        // Nothing
                                    }
                                });
                                break;
                            default:
                                throw new AssertionError();
                        }
                        assertThat(termsEnum.term(), equalTo(docIdTerm));

                        var postings = termsEnum.postings(null);
                        assertThat(postings, notNullValue());
                        assertThat(postings.cost(), equalTo(0L));
                        assertThat(postings.freq(), equalTo(0));
                        assertThat(postings.nextPosition(), equalTo(-1));
                        assertThat(postings.startOffset(), equalTo(-1));
                        assertThat(postings.endOffset(), equalTo(-1));
                        assertThat(postings.getPayload(), nullValue());

                        int minDocId = 0; // inclusive
                        int maxDocId = 0; // exclusive
                        for (var entry : finalDocs.entrySet()) {
                            int compare = docIdTerm.compareTo(entry.getKey());
                            if (compare == 0) {
                                maxDocId = minDocId + entry.getValue().version();
                                break;
                            } else if (compare > 0) {
                                minDocId += entry.getValue().version();
                            } else {
                                throw new AssertionError("Should have exited the loop before");
                            }
                        }

                        int docId = -1;
                        assertThat(postings.docID(), equalTo(docId));

                        if (randomBoolean()) {
                            int expectedDocId = minDocId;
                            while ((docId = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                                assertThat(docId, equalTo(expectedDocId));
                                assertThat(postings.docID(), equalTo(expectedDocId));
                                if (docId < maxDocId) {
                                    expectedDocId += 1;
                                }
                            }
                        } else {
                            int expectedDocId = minDocId;
                            while (expectedDocId < maxDocId) {
                                boolean canUseNextDoc = (docId + 1 == expectedDocId) || (docId == -1) /* first iteration */;

                                if (canUseNextDoc && randomBoolean()) {
                                    docId = postings.nextDoc();
                                } else {
                                    docId = postings.advance(expectedDocId);
                                }

                                assertThat(docId, equalTo(expectedDocId));
                                assertThat(postings.docID(), equalTo(expectedDocId));

                                if (randomBoolean() || expectedDocId + 1 >= maxDocId) {
                                    expectedDocId++;
                                } else {
                                    expectedDocId = randomIntBetween(expectedDocId + 1, maxDocId - 1);
                                }
                            }
                        }
                        assertThat(postings.nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
                        assertThat(postings.docID(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
                    }
                }

                // Test seek after the last term
                {
                    final var termAfterEnd = randomTermAfter(finalDocs.navigableKeySet().getLast());
                    switch (randomInt(2)) {
                        case 0:
                            assertThat(termsEnum.seekCeil(termAfterEnd), equalTo(TermsEnum.SeekStatus.END));
                            break;
                        case 1:
                            assertThat(termsEnum.seekExact(termAfterEnd), equalTo(false));
                            break;
                        case 2:
                            TermsEnum finalTermsEnum = termsEnum;
                            var e = expectThrows(
                                IllegalArgumentException.class,
                                () -> finalTermsEnum.seekExact(termAfterEnd, new TermState() {
                                    @Override
                                    public void copyFrom(TermState other) {
                                        // Nothing
                                    }
                                })
                            );
                            assertThat(e.getMessage(), containsString("term=" + termAfterEnd + " does not exist"));
                            break;
                        default:
                            throw new AssertionError();
                    }
                    assertThat(termsEnum.next(), nullValue());
                }

                // Test seek to non-existing terms
                {
                    if (finalDocs.size() > 1) {
                        final var randomDocIdsTermExceptLatest = randomSubsetOf(
                            finalDocs.navigableKeySet()
                                .stream()
                                .limit(finalDocs.size() - 1) // exclude last term
                                .collect(Collectors.toSet())
                        );

                        for (var currentTerm : randomDocIdsTermExceptLatest) {
                            // existing term right after the current term
                            var nextTerm = finalDocs.navigableKeySet().higher(currentTerm);
                            assertThat(nextTerm, notNullValue());
                            assertThat(currentTerm.compareTo(nextTerm), lessThan(0));

                            var seekTerm = randomTermBetweenOrNull(currentTerm, nextTerm);
                            if (seekTerm == null) {
                                continue;
                            }

                            assertThat(seekTerm.compareTo(currentTerm), greaterThan(0));
                            assertThat(seekTerm.compareTo(nextTerm), lessThan(0));

                            assertThat(termsEnum.seekCeil(seekTerm), equalTo(TermsEnum.SeekStatus.NOT_FOUND));
                            assertThat(termsEnum.term(), equalTo(nextTerm));

                            assertThat(termsEnum.seekExact(seekTerm), equalTo(false));
                        }
                    }
                }
            }
        });
    }

    /**
     * Indexes random documents with synthetic id in a time-series Lucene index.
     *
     * See {@link #runTest(CheckedBiConsumer)}.
     */
    public static void runTestWithRandomDocs(CheckedBiConsumer<IndexWriter, TreeMap<BytesRef, Doc>, IOException> test) throws IOException {
        final int routing = randomNonNegativeInt();
        final int maxHosts = randomIntBetween(1, 25);

        // Generate a list of unique random documents
        // Note: some documents will be later updated to a newer version so that 1 synthetic term have more than 1 posting (doc)
        final var randomDocs = new ArrayList<Doc>();
        for (int host = 0; host < maxHosts; host++) {
            var timestamp = Instant.now();

            int maxMetricsPerHost = randomIntBetween(1, 100);
            for (int metric = 0; metric < maxMetricsPerHost; metric++) {
                randomDocs.add(new Doc(timestamp.toEpochMilli(), "vm-prod-" + host, "cpu-load", metric, 1, routing));
                timestamp = timestamp.plus(randomIntBetween(1, 59), randomFrom(ChronoUnit.MILLIS, ChronoUnit.SECONDS, ChronoUnit.MINUTES));
            }
        }

        runTest((writer, parser) -> {
            // Last version of docs, keyed by their synthetic id term
            final var finalDocs = new TreeMap<BytesRef, Doc>();

            // Shuffle docs ordering before indexing
            final var randomlyOrderedDocs = new ArrayList<>(randomDocs);
            Collections.shuffle(randomlyOrderedDocs, random());

            for (int i = 0; i < randomlyOrderedDocs.size(); i++) {
                var doc = randomlyOrderedDocs.get(i);
                writer.addDocument(parser.parse(doc));

                var uid = createSyntheticIdBytesRef(buildTsId(doc), doc.timestamp(), doc.routing());
                assertThat(finalDocs.put(uid, doc), nullValue());

                if (i > 0 && rarely()) {
                    // Randomly picks a previously indexed doc and soft-update it to a newer version
                    uid = randomFrom(finalDocs.keySet());
                    var previousDoc = finalDocs.get(uid);
                    var updatedDoc = new Doc(
                        previousDoc.timestamp(),
                        previousDoc.hostName(),
                        previousDoc.metricField(),
                        previousDoc.metricValue(),
                        previousDoc.version() + 1,
                        previousDoc.routing()
                    );

                    Term uidTerm = new Term(IdFieldMapper.NAME, uid);
                    if (randomBoolean()) {
                        writer.softUpdateDocuments(uidTerm, List.of(parser.parse(updatedDoc)), Lucene.newSoftDeletesField());
                    } else {
                        writer.softUpdateDocument(uidTerm, parser.parse(updatedDoc), Lucene.newSoftDeletesField());
                    }
                    finalDocs.put(uid, updatedDoc);
                }
            }

            test.accept(writer, finalDocs);
        });
    }

    /**
     * Set up a time-series Lucene index with synthetic id.
     *
     * Note:
     * Synthetic id for time-series indices require 3 fields to be indexed for every document (_tsid, @timestamp, _ts_routing_hash) in
     * order to compute the synthetic ids.
     *
     * In the unit tests we could have added those fields explicitly for each doc, but that would mean duplicate the Lucene index
     * options/field names/sort configuration and also force us to update the unit tests if the mappings/index sort configuration change
     * (and it changed several time over the last years). So instead of adding Lucene fields explicitly as we would normally do in unit
     * tests, the tests use the default mappers and default index sort configuration to parse and to index documents. We think this is the
     * best way to stay close to the default options of time-series indices, while keeping it light enough for unit tests.
     */
    private static void runTest(CheckedBiConsumer<IndexWriter, TestDocParser, IOException> test) throws IOException {
        final var indexName = randomIdentifier();
        final var indexSettings = buildIndexSettings(indexName);
        final var mapperService = buildMapperService(indexSettings);
        final var documentParser = buildDocumentParser(mapperService);

        try (var directory = newDirectory()) {
            // Checking the index on close requires to support Terms#getMin()/getMax() methods on invalid (or incomplete) terms, something
            // that is not supported in TSDBSyntheticIdFieldsProducer today.
            //
            // TODO would be nice to enable check-index-on-close
            directory.setCheckIndexOnClose(false);

            final var indexWriterConfig = newIndexWriterConfig();
            // Configure the index writer for time-series indices
            indexWriterConfig.setMergePolicy(indexSettings.getMergePolicy(true));
            indexWriterConfig.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
            indexWriterConfig.setIndexSort(buildIndexSort(mapperService));
            // Allow tests to control flushed segments
            indexWriterConfig.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);

            try (var writer = new IndexWriter(directory, indexWriterConfig)) {
                test.accept(writer, documentParser);
            }
        }
    }

    /**
     * Builds time-series index settings.
     */
    private static IndexSettings buildIndexSettings(final String indexName) {
        return new IndexSettings(
            IndexMetadata.builder(indexName)
                .settings(
                    indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.SYNTHETIC_ID.getKey(), true)
                        .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                        .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), List.of("hostname", "metric.field"))
                        .build()
                )
                .putMapping("""
                    {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                            "hostname": {
                                "type": "keyword",
                                "time_series_dimension": true
                            },
                            "metric": {
                                "properties": {
                                    "field": {
                                        "type": "keyword",
                                        "time_series_dimension": true
                                    },
                                    "value": {
                                        "type": "integer",
                                        "time_series_metric": "counter"
                                    }
                                }
                            },
                            "version": {
                                "type": "integer"
                            }
                        }
                    }""")
                .build(),
            Settings.EMPTY
        );
    }

    /**
     * Builds the Lucene index sort configuration for time-series indices.
     */
    private static Sort buildIndexSort(final MapperService mapperService) {
        final var indexSortConfig = new IndexSortConfig(mapperService.getIndexSettings());
        return indexSortConfig.buildIndexSort(
            mapperService::fieldType,
            (ft, s) -> ft.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "")).build(null, null)
        );
    }

    /**
     * Builds a {@link MapperService} that can be used to parse time-series documents in test.
     */
    private static MapperService buildMapperService(final IndexSettings indexSettings) throws IOException {
        final var mapperService = MapperTestUtils.newMapperService(
            new NamedXContentRegistry(CollectionUtils.concatLists(ClusterModule.getNamedXWriteables(), IndicesModule.getNamedXContents())),
            createTempFile(),
            indexSettings.getSettings(),
            indexSettings.getIndex().getName()
        );
        var indexMode = indexSettings.getMode();
        assertThat(indexMode, equalTo(IndexMode.TIME_SERIES));
        if (indexSettings.getMode().getDefaultMapping(indexSettings) != null) {
            mapperService.merge(null, indexMode.getDefaultMapping(indexSettings), MapperService.MergeReason.MAPPING_UPDATE);
        }
        return mapperService;
    }

    @FunctionalInterface
    private interface TestDocParser {
        Iterable<? extends IndexableField> parse(Doc doc) throws IOException;
    }

    /**
     * Builds a parser for test documents that produces the required Lucene fields for synthetic id to work.
     */
    private static TestDocParser buildDocumentParser(final MapperService mapperService) {
        return doc -> {
            var documentParser = mapperService.documentParser();
            try (var builder = buildDocument(doc)) {
                var parsedDoc = documentParser.parseDocument(
                    new SourceToParse(
                        null,
                        BytesReference.bytes(builder),
                        builder.contentType(),
                        TimeSeriesRoutingHashFieldMapper.encode(doc.routing())
                    ),
                    mapperService.mappingLookup()
                );
                assertParsedDocument(parsedDoc, doc);
                return parsedDoc.docs().getFirst();
            }
        };
    }

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern(STRICT_DATE_OPTIONAL_TIME.getName());

    /**
     * Builds the JSON representation of a test document.
     */
    private static XContentBuilder buildDocument(final Doc document) throws IOException {
        var source = XContentFactory.jsonBuilder();
        source.startObject();
        {
            source.field("@timestamp", DATE_FORMATTER.formatMillis(document.timestamp()));
            source.field("hostname", document.hostName());
            source.startObject("metric");
            {
                source.field("field", document.metricField());
                source.field("value", document.metricValue());

            }
            source.endObject();
        }
        source.endObject();
        return source;
    }

    /**
     * Asserts that the test documents are correctly parsed in tests, producing the required Lucene fields for synthetic id to work.
     */
    private static void assertParsedDocument(ParsedDocument parsedDoc, Doc doc) {

        assertThat(parsedDoc, notNullValue());
        assertThat(parsedDoc.id(), notNullValue());
        assertThat(parsedDoc.docs().size(), equalTo(1));

        var luceneDoc = parsedDoc.docs().getFirst();

        // _tsid field
        var docTsId = luceneDoc.getField(TimeSeriesIdFieldMapper.NAME);
        assertThat(docTsId, notNullValue());
        assertThat(docTsId.binaryValue(), notNullValue());

        var expectedTsId = buildTsId(doc);
        assertThat(docTsId.binaryValue(), equalTo(expectedTsId));

        // @timestamp field
        var docTimestamp = luceneDoc.getField(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertThat(docTimestamp, notNullValue());
        assertThat(docTimestamp.numericValue(), notNullValue());

        long expectedTimestamp = doc.timestamp();
        assertThat(docTimestamp.numericValue().longValue(), equalTo(expectedTimestamp));

        // _ts_routing_hash field
        var docRoutingHash = luceneDoc.getField(TimeSeriesRoutingHashFieldMapper.NAME);
        assertThat(docRoutingHash, notNullValue());
        assertThat(docRoutingHash.binaryValue(), notNullValue());

        var expectedRoutingHash = Uid.encodeId(TimeSeriesRoutingHashFieldMapper.encode(doc.routing()));
        assertThat(docRoutingHash.binaryValue(), equalTo(expectedRoutingHash));

        // _id (synthetic)
        var docId = luceneDoc.getField(IdFieldMapper.NAME);
        assertThat(docId, notNullValue());
        assertThat(docId.binaryValue(), notNullValue());

        var expectedDocId = TsidExtractingIdFieldMapper.createSyntheticId(expectedTsId, expectedTimestamp, doc.routing());
        assertThat(Uid.decodeId(docId.binaryValue()), equalTo(expectedDocId));
    }

    private static BytesRef buildTsId(Doc doc) {
        return new TsidBuilder().addStringDimension("hostname", doc.hostName())
            .addStringDimension("metric.field", doc.metricField())
            .buildTsid();
    }

    /**
     * Generate a term that would be ordered after the provided term.
     */
    private static BytesRef randomTermAfter(BytesRef value) {
        final BytesRef tsId = TsidExtractingIdFieldMapper.extractTimeSeriesIdFromSyntheticId(value);
        final long timestamp = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(value);
        if (1 >= timestamp) {
            throw new IllegalArgumentException("Timestamp should be greater than 1 to allow generating the term");
        }

        final int choice = randomInt(2);
        boolean changeTsId = (choice == 0 || choice == 1);
        boolean changeTimestamp = (choice == 0 || choice == 2);

        final BytesRef modifiedTsId;
        if (changeTsId) {
            byte[] bytes = new byte[tsId.length];
            System.arraycopy(tsId.bytes, tsId.offset, bytes, 0, tsId.length);

            // Modify the last byte of the tsid to be greater/higher (tsid are sorted by asc order)
            boolean success = false;
            for (int i = bytes.length - 1; i >= 0; i--) {
                bytes[i]++;
                if (bytes[i] != 0) {
                    success = true;
                    break;
                }
            }
            if (success == false) {
                changeTimestamp = true;
            }
            modifiedTsId = new BytesRef(bytes);
        } else {
            modifiedTsId = tsId;
        }

        long modifiedTimestamp;
        if (changeTimestamp) {
            // Modify the timestamp to be smaller (timestamps are sorted by desc order)
            modifiedTimestamp = timestamp - 1L;
        } else {
            modifiedTimestamp = timestamp;
        }

        final var term = TsidExtractingIdFieldMapper.createSyntheticIdBytesRef(
            modifiedTsId,
            modifiedTimestamp,
            TsidExtractingIdFieldMapper.extractRoutingHashFromSyntheticId(value)
        );
        assert term.compareTo(value) > 0 : term + " <= " + value;
        return term;
    }

    /**
     * Generate a term that is greater than a {@code min} term and lower than a {@code max} term. Returns a null value if no such term can
     * be generated.
     */
    public static BytesRef randomTermBetweenOrNull(BytesRef min, BytesRef max) {
        BytesRef tsIdMin = TsidExtractingIdFieldMapper.extractTimeSeriesIdFromSyntheticId(min);
        BytesRef tsIdMax = TsidExtractingIdFieldMapper.extractTimeSeriesIdFromSyntheticId(max);

        if (tsIdMin.equals(tsIdMax) == false) {

            // tsids are not identical, find the first byte that differs
            int diffIndex = 0;
            final int minLen = Math.min(tsIdMin.length, tsIdMax.length);
            while (diffIndex < minLen && min.bytes[min.offset + diffIndex] == max.bytes[max.offset + diffIndex]) {
                diffIndex++;
            }

            // increment the first byte that differs (if there is room for doing so)
            if (diffIndex < minLen) {
                int valueMin = min.bytes[min.offset + diffIndex] & 0xFF;
                int valueMax = (diffIndex < max.length) ? (max.bytes[max.offset + diffIndex] & 0xFF) : 256;
                if (valueMax - valueMin > 1) {
                    byte[] tsid = new byte[tsIdMin.length];
                    System.arraycopy(tsIdMin.bytes, tsIdMin.offset, tsid, 0, tsIdMin.length);
                    tsid[diffIndex] = (byte) (valueMin + randomIntBetween(1, valueMax - valueMin - 1));
                    return TsidExtractingIdFieldMapper.createSyntheticIdBytesRef(
                        new BytesRef(tsid),
                        TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(min),
                        TsidExtractingIdFieldMapper.extractRoutingHashFromSyntheticId(min)
                    );
                }
            }
        }

        long timestampMin = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(min);
        long timestampMax = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(max);

        long diffTimestamps = Math.abs(timestampMin - timestampMax);
        if (diffTimestamps > 1L) {
            if (timestampMin > timestampMax) {
                return TsidExtractingIdFieldMapper.createSyntheticIdBytesRef(
                    tsIdMin,
                    timestampMin - randomLongBetween(1L, diffTimestamps - 1L),
                    TsidExtractingIdFieldMapper.extractRoutingHashFromSyntheticId(min)
                );
            } else {
                return TsidExtractingIdFieldMapper.createSyntheticIdBytesRef(
                    tsIdMax,
                    timestampMax + randomLongBetween(1L, diffTimestamps - 1L),
                    TsidExtractingIdFieldMapper.extractRoutingHashFromSyntheticId(max)
                );
            }
        }
        return null; // Nothing we can do, min and max have identical _tsid and @timestamp
    }

}
