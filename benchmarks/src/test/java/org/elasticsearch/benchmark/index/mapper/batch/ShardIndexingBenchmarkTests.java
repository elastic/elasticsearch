/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper.batch;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.benchmark.index.mapper.batch.ColumnIndexer.BenchShardContext;
import org.elasticsearch.benchmark.index.mapper.batch.ColumnIndexer.CountingRecycler;
import org.elasticsearch.benchmark.index.mapper.batch.LuceneSink.SeqNoStamper;
import org.elasticsearch.benchmark.index.mapper.batch.Shapes.BenchShape;
import org.elasticsearch.benchmark.index.mapper.batch.Shapes.Expect;
import org.elasticsearch.benchmark.index.mapper.batch.Shapes.Feature;
import org.elasticsearch.benchmark.index.mapper.batch.ShardIndexingBenchmark.ColumnState;
import org.elasticsearch.benchmark.index.mapper.batch.ShardIndexingBenchmark.FallbackState;
import org.elasticsearch.benchmark.index.mapper.batch.ShardIndexingBenchmark.PageCounters;
import org.elasticsearch.benchmark.index.mapper.batch.ShardIndexingBenchmark.RowState;
import org.elasticsearch.benchmark.index.mapper.batch.ShardIndexingBenchmark.ShardFixtureState;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Keeps the benchmark honest: every shape builds and has the batch features it declares, the row and column paths produce the
 * same Lucene fields and values, and each benchmark state runs its lifecycle and rejects shapes that would take another path.
 */
public class ShardIndexingBenchmarkTests extends ESTestCase {

    private static final String SOURCE_MODE = "synthetic";
    private static final String ALTERNATE_SOURCE_MODE = "columnar_stored";
    private static final int ALTERNATE_DOCS_PER_REQUEST = 100;
    private static final long SEED = 42L;

    public void testEveryShapeBuildsAndMatchesFeatures() throws IOException {
        for (String name : Shapes.names()) {
            try (IndexingFixture fixture = build(name, ShardIndexingBenchmark.DOCS_PER_INVOCATION)) {
                assertThat(name, fixture.requests().length, equalTo(1));
                assertFeaturesMatchBatch(name, fixture);
            }
        }
    }

    public void testSeededIdsAreUniqueAndDeterministic() {
        String[] ids = IndexingFixture.seededIds(SEED, ShardIndexingBenchmark.DOCS_PER_INVOCATION);
        assertThat(Set.of(ids).size(), equalTo(ids.length));
        assertArrayEquals(ids, IndexingFixture.seededIds(SEED, ShardIndexingBenchmark.DOCS_PER_INVOCATION));
        String[] longer = IndexingFixture.seededIds(SEED, 2 * ShardIndexingBenchmark.DOCS_PER_INVOCATION);
        assertArrayEquals(ids, Arrays.copyOf(longer, ids.length));
    }

    private static void assertFeaturesMatchBatch(String name, IndexingFixture fixture) {
        BenchShape shape = fixture.shape();
        EscfBatch batch = (EscfBatch) fixture.requests()[0].batch();
        if (shape.has(Feature.MULTI_VALUE)) {
            boolean unionAllowed = shape.expect() != Expect.COLUMN;
            assertTrue(name, anyColumn(batch, c -> {
                byte kind = batch.column(c).kind();
                return kind == EscfColumnKind.ARRAY || (unionAllowed && kind == EscfColumnKind.UNION);
            }));
        }
        if (shape.has(Feature.ABSENT)) {
            assertTrue(name, anyColumn(batch, c -> batch.column(c).isDense() == false));
        }
        if (shape.has(Feature.EXPLICIT_NULL)) {
            assertTrue(name, anyColumn(batch, c -> batch.column(c).kind() == EscfColumnKind.UNION));
        }
    }

    public void testRowAndColumnEmitSameFields() throws IOException {
        for (BenchShape shape : Shapes.where(s -> s.expect() == Expect.COLUMN)) {
            try (IndexingFixture fixture = IndexingFixture.build(shape, SOURCE_MODE, 1000, 1000, SEED)) {
                Snapshot column;
                try (LuceneSink sink = LuceneSink.open(fixture.codec(), fixture.mapperService(), createTempDir())) {
                    new ColumnIndexer(shardContext(fixture), CountingRecycler.pooled()).index(
                        sink.writer(),
                        new SeqNoStamper(),
                        fixture.requests()
                    );
                    column = snapshot(sink);
                }
                Snapshot row;
                try (LuceneSink sink = LuceneSink.open(fixture.codec(), fixture.mapperService(), createTempDir())) {
                    new RowIndexer(fixture.mapperService(), RowIndexer.RowSourceKind.BATCH_ROW).index(
                        sink.writer(),
                        new SeqNoStamper(),
                        fixture.requests()
                    );
                    row = snapshot(sink);
                }
                assertThat(shape.name(), column.numDocs(), equalTo(1000));
                assertThat(shape.name(), column, equalTo(row));
            }
        }
    }

    public void testBenchmarkLifecycle() throws IOException {
        ShardIndexingBenchmark bench = new ShardIndexingBenchmark();
        runLifecycles(ColumnState.class, (shape, docsPerRequest) -> {
            ColumnState state = new ColumnState();
            state.shape = shape;
            state.docsPerRequest = docsPerRequest;
            return state;
        }, List.of(s -> bench.column(s, new PageCounters())));
        runLifecycles(RowState.class, (shape, docsPerRequest) -> {
            RowState state = new RowState();
            state.shape = shape;
            state.docsPerRequest = docsPerRequest;
            return state;
        }, List.of(bench::rowBatch, bench::rowJson));
        runLifecycles(FallbackState.class, (shape, docsPerRequest) -> {
            FallbackState state = new FallbackState();
            state.shape = shape;
            state.docsPerRequest = docsPerRequest;
            return state;
        }, List.of(bench::columnThenRow));
        ColumnState column = new ColumnState();
        column.shape = "long.nullexplicit";
        column.docsPerRequest = ShardIndexingBenchmark.DOCS_PER_INVOCATION;
        assertSetupRejects(column);
        FallbackState fallback = new FallbackState();
        fallback.shape = "long.dense.small";
        fallback.docsPerRequest = ShardIndexingBenchmark.DOCS_PER_INVOCATION;
        assertSetupRejects(fallback);
    }

    private static <S extends ShardFixtureState> void runLifecycles(
        Class<S> type,
        BiFunction<String, Integer, S> factory,
        List<CheckedConsumer<S, IOException>> benchmarks
    ) throws IOException {
        for (String shape : Utils.possibleValues(type, "shape")) {
            runLifecycle(factory, shape, SOURCE_MODE, ShardIndexingBenchmark.DOCS_PER_INVOCATION, benchmarks);
        }
        for (String shape : defaultValues(type, "shape")) {
            runLifecycle(factory, shape, ALTERNATE_SOURCE_MODE, ShardIndexingBenchmark.DOCS_PER_INVOCATION, List.of());
            runLifecycle(factory, shape, SOURCE_MODE, ALTERNATE_DOCS_PER_REQUEST, List.of());
        }
    }

    private static <S extends ShardFixtureState> void runLifecycle(
        BiFunction<String, Integer, S> factory,
        String shape,
        String sourceMode,
        int docsPerRequest,
        List<CheckedConsumer<S, IOException>> benchmarks
    ) throws IOException {
        S state = factory.apply(shape, docsPerRequest);
        state.sourceMode = sourceMode;
        state.seed = SEED;
        state.setUp();
        try {
            for (CheckedConsumer<S, IOException> benchmark : benchmarks) {
                benchmark.accept(state);
                state.afterInvocation();
            }
            if (benchmarks.isEmpty() == false) {
                state.afterIteration();
            }
        } finally {
            state.tearDown();
        }
    }

    private static List<String> defaultValues(Class<?> type, String field) {
        try {
            return List.of(type.getField(field).getAnnotation(Param.class).value());
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertSetupRejects(ShardFixtureState state) {
        state.sourceMode = SOURCE_MODE;
        state.seed = SEED;
        expectThrows(IllegalStateException.class, state::setUp);
        assertThat(state.fixture, nullValue());
        assertThat(state.sink, nullValue());
    }

    public void testParamsAreKnownShapes() {
        assertStateShapes(ColumnState.class, EnumSet.of(Expect.COLUMN));
        assertStateShapes(RowState.class, EnumSet.allOf(Expect.class));
        assertStateShapes(FallbackState.class, EnumSet.of(Expect.FALLBACK_RESOLVE, Expect.FALLBACK_MAP));
    }

    private static void assertStateShapes(Class<?> state, Set<Expect> accepted) {
        List<String> names = Utils.possibleValues(state, "shape");
        for (String name : names) {
            assertTrue(state.getSimpleName() + " " + name, accepted.contains(Shapes.get(name).expect()));
        }
        List<String> expected = Shapes.where(s -> accepted.contains(s.expect())).stream().map(BenchShape::name).toList();
        assertThat(state.getSimpleName(), names, containsInAnyOrder(expected.toArray()));
    }

    private static IndexingFixture build(String name, int docs) throws IOException {
        return IndexingFixture.build(Shapes.get(name), SOURCE_MODE, docs, docs, SEED);
    }

    private static BenchShardContext shardContext(IndexingFixture fixture) {
        return new BenchShardContext(fixture.mapperService(), fixture.indexSettings(), 1L);
    }

    private static boolean anyColumn(EscfBatch batch, Predicate<Integer> predicate) {
        for (int c = 0; c < batch.columnCount(); c++) {
            if (predicate.test(c)) {
                return true;
            }
        }
        return false;
    }

    private record Snapshot(
        int numDocs,
        Map<String, String> fields,
        Map<String, Integer> storedCounts,
        Map<String, Map<String, List<Long>>> numericValues
    ) {}

    private static Snapshot snapshot(LuceneSink sink) throws IOException {
        sink.writer().commit();
        Map<String, String> fields = new TreeMap<>();
        Map<String, Integer> stored = new TreeMap<>();
        Map<String, Map<String, List<Long>>> numericValues = new TreeMap<>();
        try (DirectoryReader reader = DirectoryReader.open(sink.writer())) {
            for (LeafReaderContext leaf : reader.leaves()) {
                LeafReader r = leaf.reader();
                String[] ids = ids(r);
                for (FieldInfo fi : r.getFieldInfos()) {
                    fields.put(fi.name, describe(fi));
                    if (fi.getDocValuesType() == DocValuesType.NUMERIC || fi.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                        collectNumeric(r, fi.name, ids, numericValues);
                    }
                }
                StoredFields storedFields = r.storedFields();
                for (int d = 0; d < r.maxDoc(); d++) {
                    Document doc = storedFields.document(d);
                    Set<String> names = new TreeSet<>();
                    for (IndexableField f : doc.getFields()) {
                        names.add(f.name());
                    }
                    for (String name : names) {
                        stored.merge(name, 1, Integer::sum);
                    }
                }
            }
            assertThat(numericValues.size(), equalTo(reader.numDocs()));
            for (Map<String, List<Long>> doc : numericValues.values()) {
                assertThat(doc.keySet(), hasItems(SeqNoFieldMapper.NAME, SeqNoFieldMapper.PRIMARY_TERM_NAME, VersionFieldMapper.NAME));
            }
            return new Snapshot(reader.numDocs(), fields, stored, numericValues);
        }
    }

    private static String[] ids(LeafReader reader) throws IOException {
        String[] ids = new String[reader.maxDoc()];
        Terms terms = reader.terms(IdFieldMapper.NAME);
        assertThat(terms, notNullValue());
        TermsEnum termsEnum = terms.iterator();
        PostingsEnum postings = null;
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
            postings = termsEnum.postings(postings, PostingsEnum.NONE);
            for (int d = postings.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = postings.nextDoc()) {
                ids[d] = Uid.decodeId(term);
            }
        }
        return ids;
    }

    private static void collectNumeric(LeafReader reader, String field, String[] ids, Map<String, Map<String, List<Long>>> out)
        throws IOException {
        SortedNumericDocValues dv = DocValues.getSortedNumeric(reader, field);
        for (int d = dv.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
            int count = dv.docValueCount();
            List<Long> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(dv.nextValue());
            }
            out.computeIfAbsent(ids[d], k -> new TreeMap<>()).put(field, values);
        }
    }

    private static String describe(FieldInfo fi) {
        return "dv="
            + fi.getDocValuesType()
            + " skip="
            + fi.docValuesSkipIndexType()
            + " pointDims="
            + fi.getPointDimensionCount()
            + "/"
            + fi.getPointIndexDimensionCount()
            + "/"
            + fi.getPointNumBytes()
            + " index="
            + fi.getIndexOptions()
            + " omitNorms="
            + fi.omitsNorms()
            + " tv="
            + fi.hasTermVectors()
            + " payloads="
            + fi.hasPayloads()
            + " vec="
            + fi.getVectorDimension()
            + " soft="
            + fi.isSoftDeletesField()
            + " attrs="
            + new TreeMap<>(fi.attributes());
    }
}
