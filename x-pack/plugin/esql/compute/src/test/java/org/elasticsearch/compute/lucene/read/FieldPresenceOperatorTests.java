/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperatorTests;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.core.IOUtils;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class FieldPresenceOperatorTests extends OperatorTestCase {
    private static final String INDEX = "metrics-1";
    /** The fields the operator reports. {@code noise} is indexed but never reported. */
    private static final List<String> REPORTED_FIELDS = List.of("host", "pod", "cpu", "latency");

    private final List<Directory> directories = new ArrayList<>();
    private final List<IndexReader> readers = new ArrayList<>();

    private FieldPresenceOperator.ShardFields simpleShardFields;
    private Set<Set<String>> simpleFieldSets;

    @After
    public void closeIndices() throws IOException {
        IOUtils.close(readers);
        IOUtils.close(directories);
    }

    @Override
    protected FieldPresenceOperator.Factory simple(SimpleOptions options) {
        return new FieldPresenceOperator.Factory(shard -> simpleShardFields, 0);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<List<Set<String>>> segments = new ArrayList<>();
        simpleFieldSets = new HashSet<>();
        int docsPerSegment = Math.max(1, size / 4);
        for (int doc = 0; doc < size; doc++) {
            if (doc % docsPerSegment == 0) {
                segments.add(new ArrayList<>());
            }
            Set<String> fields = simpleDocFields(doc);
            segments.getLast().add(fields);
            Set<String> reported = new HashSet<>(fields);
            reported.retainAll(REPORTED_FIELDS);
            if (reported.isEmpty() == false) {
                simpleFieldSets.add(reported);
            }
        }
        IndexReader reader = index(segments);
        simpleShardFields = shardFields(INDEX, reader);
        LuceneSourceOperator.Factory source = new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(reader, 0)),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())),
            randomFrom(DataPartitioning.values()),
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            randomIntBetween(1, 10),
            randomPageSize(),
            LuceneOperator.NO_LIMIT,
            false,
            () -> 0L,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            QueryWarnings.EMIT
        );
        return source.get(driverContext());
    }

    private static Set<String> simpleDocFields(int doc) {
        Set<String> fields = new HashSet<>();
        if (doc % 2 == 0) {
            fields.add("host");
        }
        if (doc % 3 != 0) {
            fields.add("pod");
        }
        if (doc % 4 != 3) {
            fields.add("cpu");
        }
        if (doc % 5 == 0) {
            fields.add("latency");
        }
        if (doc % 7 == 0) {
            fields.add("noise");
        }
        return fields;
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<Row> rows = rows(results);
        List<Set<String>> fieldSets = new ArrayList<>();
        for (Row row : rows) {
            assertThat(row.index(), equalTo(INDEX));
            fieldSets.add(row.fields());
        }
        assertThat(Set.copyOf(fieldSets), equalTo(simpleFieldSets));
        assertThat("every field set is emitted once", fieldSets, hasSize(simpleFieldSets.size()));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("FieldPresenceOperator[docChannel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("FieldPresenceOperator[docChannel=0]");
    }

    public void testReportsTheFieldsOfEachDocument() {
        IndexReader reader = index(List.of(List.of(Set.of("host", "cpu"), Set.of("pod", "latency"))));
        List<Row> rows = run(Map.of(0, shardFields(INDEX, reader)), docs(doc(0, 0, 0), doc(0, 0, 1)));
        assertThat(Set.copyOf(rows), equalTo(Set.of(new Row(INDEX, Set.of("host", "cpu")), new Row(INDEX, Set.of("pod", "latency")))));
    }

    public void testEmitsAFieldSetOnceAcrossPages() {
        IndexReader reader = index(List.of(List.of(Set.of("host", "cpu"), Set.of("host", "cpu"), Set.of("host", "cpu"))));
        List<Row> rows = run(Map.of(0, shardFields(INDEX, reader)), docs(doc(0, 0, 0), doc(0, 0, 1)), docs(doc(0, 0, 2)));
        assertThat(rows, equalTo(List.of(new Row(INDEX, Set.of("host", "cpu")))));
    }

    public void testEmitsTheSameFieldSetForEachShard() {
        IndexReader first = index(List.of(List.of(Set.of("host", "cpu"))));
        IndexReader second = index(List.of(List.of(Set.of("host", "cpu"))));
        List<Row> rows = run(
            Map.of(0, shardFields("metrics-1", first), 1, shardFields("metrics-2", second)),
            docs(doc(0, 0, 0), doc(1, 0, 0))
        );
        assertThat(
            Set.copyOf(rows),
            equalTo(Set.of(new Row("metrics-1", Set.of("host", "cpu")), new Row("metrics-2", Set.of("host", "cpu"))))
        );
    }

    public void testDocumentWithoutReportedFieldsEmitsNothing() {
        IndexReader reader = index(List.of(List.of(Set.of("noise"))));
        assertThat(run(Map.of(0, shardFields(INDEX, reader)), docs(doc(0, 0, 0))), equalTo(List.of()));
    }

    public void testReadsDocumentsOutOfOrderAcrossSegments() {
        IndexReader reader = index(
            List.of(List.of(Set.of("host"), Set.of("host", "cpu"), Set.of("pod")), List.of(Set.of("latency"), Set.of("host", "latency")))
        );
        List<Row> rows = run(
            Map.of(0, shardFields(INDEX, reader)),
            docs(doc(0, 1, 1), doc(0, 0, 2), doc(0, 1, 0), doc(0, 0, 0), doc(0, 0, 1))
        );
        assertThat(
            Set.copyOf(rows),
            equalTo(
                Set.of(
                    new Row(INDEX, Set.of("host")),
                    new Row(INDEX, Set.of("host", "cpu")),
                    new Row(INDEX, Set.of("pod")),
                    new Row(INDEX, Set.of("latency")),
                    new Row(INDEX, Set.of("host", "latency"))
                )
            )
        );
    }

    public void testReadsEarlierDocumentsOfTheSegmentInALaterPage() {
        IndexReader reader = index(List.of(List.of(Set.of("host"), Set.of("pod"), Set.of("cpu"), Set.of("latency"))));
        List<Row> rows = run(Map.of(0, shardFields(INDEX, reader)), docs(doc(0, 0, 3)), docs(doc(0, 0, 0)), docs(doc(0, 0, 2)));
        assertThat(
            rows,
            equalTo(List.of(new Row(INDEX, Set.of("latency")), new Row(INDEX, Set.of("host")), new Row(INDEX, Set.of("cpu"))))
        );
    }

    public void testFieldMissingFromASegment() {
        IndexReader reader = index(List.of(List.of(Set.of("host"), Set.of("host")), List.of(Set.of("host", "latency"))));
        List<Row> rows = run(Map.of(0, shardFields(INDEX, reader)), docs(doc(0, 0, 0), doc(0, 0, 1), doc(0, 1, 0)));
        assertThat(Set.copyOf(rows), equalTo(Set.of(new Row(INDEX, Set.of("host")), new Row(INDEX, Set.of("host", "latency")))));
    }

    public void testReadsOnTheThreadTheDriverResumesOn() throws Exception {
        IndexReader reader = index(List.of(List.of(Set.of("host"), Set.of("host", "cpu"))));
        FieldPresenceOperator.ShardFields fields = shardFields(INDEX, reader);
        try (Operator operator = new FieldPresenceOperator.Factory(shard -> fields, 0).get(driverContext())) {
            operator.addInput(docs(doc(0, 0, 0)));
            operator.getOutput().releaseBlocks();

            Page laterPage = docs(doc(0, 0, 1));
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread otherThread = new Thread(() -> {
                try {
                    operator.addInput(laterPage);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            otherThread.start();
            otherThread.join();
            assertNull(failure.get());

            Page output = operator.getOutput();
            try {
                assertThat(rows(List.of(output)), equalTo(List.of(new Row(INDEX, Set.of("host", "cpu")))));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    public void testShardWithoutFieldsFails() {
        index(List.of(List.of(Set.of("host"))));
        Map<Integer, FieldPresenceOperator.ShardFields> noShards = Map.of();
        DriverContext driverContext = driverContext();
        try (Operator operator = new FieldPresenceOperator.Factory(noShards::get, 0).get(driverContext)) {
            Exception e = expectThrows(IllegalStateException.class, () -> operator.addInput(docs(doc(0, 0, 0))));
            assertThat(e.getMessage(), equalTo("no fields configured for shard [0]"));
        }
    }

    public void testBuildsTheFieldsOfAShardOnceWhenFirstRead() {
        IndexReader first = index(List.of(List.of(Set.of("host", "cpu"), Set.of("pod", "cpu"))));
        IndexReader second = index(List.of(List.of(Set.of("host", "cpu"))));
        Map<Integer, FieldPresenceOperator.ShardFields> fieldsByShard = Map.of(
            0,
            shardFields("metrics-1", first),
            1,
            shardFields("metrics-2", second)
        );
        Map<Integer, Integer> builds = new ConcurrentHashMap<>();
        FieldPresenceOperator.Factory factory = new FieldPresenceOperator.Factory(shard -> {
            builds.merge(shard, 1, Integer::sum);
            return fieldsByShard.get(shard);
        }, 0);
        try (Operator firstDriver = factory.get(driverContext()); Operator secondDriver = factory.get(driverContext())) {
            assertThat("no shard is read yet", builds, equalTo(Map.of()));
            firstDriver.addInput(docs(doc(0, 0, 0)));
            firstDriver.getOutput().releaseBlocks();
            secondDriver.addInput(docs(doc(0, 0, 1)));
            secondDriver.getOutput().releaseBlocks();
            assertThat(builds, equalTo(Map.of(0, 1)));
        }
    }

    public void testTracksMemoryOfFieldSets() {
        IndexReader reader = index(List.of(List.of(Set.of("host", "cpu"), Set.of("host", "cpu"), Set.of("pod"))));
        FieldPresenceOperator.ShardFields fields = shardFields(INDEX, reader);
        DriverContext driverContext = driverContext();
        CircuitBreaker breaker = driverContext.breaker();
        long usedBefore = breaker.getUsed();
        Operator operator = new FieldPresenceOperator.Factory(shard -> fields, 0).get(driverContext);

        operator.addInput(docs(doc(0, 0, 0)));
        operator.getOutput().releaseBlocks();
        long usedAfterFirstFieldSet = breaker.getUsed();
        assertThat(usedAfterFirstFieldSet, greaterThan(usedBefore));

        operator.addInput(docs(doc(0, 0, 1)));
        assertNull(operator.getOutput());
        assertThat(breaker.getUsed(), equalTo(usedAfterFirstFieldSet));

        operator.addInput(docs(doc(0, 0, 2)));
        operator.getOutput().releaseBlocks();
        assertThat(breaker.getUsed(), greaterThan(usedAfterFirstFieldSet));

        operator.close();
        assertThat(breaker.getUsed(), equalTo(usedBefore));
    }

    private record Row(String index, Set<String> fields) {}

    private static int[] doc(int shard, int segment, int doc) {
        return new int[] { shard, segment, doc };
    }

    private Page docs(int[]... docs) {
        try (DocVector.FixedBuilder builder = DocVector.newFixedBuilder(driverContext().blockFactory(), docs.length)) {
            for (int[] doc : docs) {
                builder.append(doc[0], doc[1], doc[2]);
            }
            return new Page(builder.build(DocVector.config()).asBlock());
        }
    }

    private List<Row> run(Map<Integer, FieldPresenceOperator.ShardFields> fieldsByShard, Page... pages) {
        List<Page> output = new ArrayList<>();
        try (Operator operator = new FieldPresenceOperator.Factory(fieldsByShard::get, 0).get(driverContext())) {
            for (Page page : pages) {
                operator.addInput(page);
                Page out = operator.getOutput();
                if (out != null) {
                    output.add(out);
                }
            }
            operator.finish();
            assertTrue(operator.isFinished());
        }
        try {
            return rows(output);
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    private static List<Row> rows(List<Page> pages) {
        List<Row> rows = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            assertThat(page.getBlockCount(), equalTo(2));
            BytesRefBlock indexNames = page.getBlock(0);
            BytesRefBlock fieldNames = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                String index = indexNames.getBytesRef(indexNames.getFirstValueIndex(p), scratch).utf8ToString();
                Set<String> fields = new HashSet<>();
                int first = fieldNames.getFirstValueIndex(p);
                for (int i = first; i < first + fieldNames.getValueCount(p); i++) {
                    fields.add(fieldNames.getBytesRef(i, scratch).utf8ToString());
                }
                rows.add(new Row(index, fields));
            }
        }
        return rows;
    }

    /**
     * Builds an index with one segment per element of {@code segments}. Each document has the listed fields.
     */
    private IndexReader index(List<List<Set<String>>> segments) {
        try {
            Directory directory = newDirectory();
            directories.add(directory);
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (List<Set<String>> segment : segments) {
                    for (Set<String> fields : segment) {
                        writer.addDocument(document(fields));
                    }
                    writer.commit();
                }
            }
            // Fails reads of doc values on a thread other than the one that created them, like a driver that moved threads
            IndexReader reader = new AssertingDirectoryReader(DirectoryReader.open(directory));
            readers.add(reader);
            return reader;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Document document(Set<String> fields) {
        Document document = new Document();
        for (String field : fields) {
            switch (field) {
                case "host", "pod", "noise" -> document.add(new SortedDocValuesField(field, new BytesRef(field + "-value")));
                case "cpu" -> document.add(new NumericDocValuesField(field, 42));
                // Binary doc values, like the histogram metric types
                case "latency" -> document.add(new BinaryDocValuesField(field, new BytesRef(new byte[] { 1, 2, 3 })));
                default -> throw new IllegalArgumentException("unknown field [" + field + "]");
            }
        }
        return document;
    }

    private static FieldPresenceOperator.ShardFields shardFields(String index, IndexReader reader) {
        IndexSearcher searcher = new IndexSearcher(reader);
        List<FieldPresenceOperator.Field> fields = new ArrayList<>();
        try {
            for (String name : REPORTED_FIELDS) {
                var presence = searcher.rewrite(new FieldExistsQuery(name)).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                fields.add(new FieldPresenceOperator.Field(name, presence));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new FieldPresenceOperator.ShardFields(index, reader, fields);
    }
}
