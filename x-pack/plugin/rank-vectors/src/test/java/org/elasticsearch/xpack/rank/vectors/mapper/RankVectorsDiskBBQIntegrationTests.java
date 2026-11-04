/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.IVFKnnFloatVectorQuery;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.diskbbq.DiskBBQPlugin;
import org.elasticsearch.xpack.rank.vectors.LocalStateRankVectors;
import org.elasticsearch.xpack.rank.vectors.fde.MuveraEncoder;
import org.elasticsearch.xpack.rank.vectors.query.RankVectorsQueryBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DEFAULT_OVERSAMPLE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RankVectorsDiskBBQIntegrationTests extends MapperServiceTestCase {

    private static final Settings STATELESS_SETTINGS = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new LocalStateRankVectors(STATELESS_SETTINGS), new TestDiskBBQPlugin(STATELESS_SETTINGS));
    }

    private static class TestDiskBBQPlugin extends DiskBBQPlugin {
        private final XPackLicenseState licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.TRIAL, true, null)
        );

        private TestDiskBBQPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected XPackLicenseState getLicenseState() {
            return licenseState;
        }
    }

    public void testGeneratedFdeSurvivesFlushReopenForceMergeAndSearch() throws Exception {
        MapperService mapperService = createMapperService(indexedMuveraMapping());
        DocumentMapper documentMapper = mapperService.documentMapper();
        float[][] first = unitTokens(0, 1);
        float[][] second = unitTokens(2, 3);
        float[] queryFde = encoder().encodeQuery(first);
        String fdeField = "field";

        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setCodec(
                    new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, threadPool).codec("default")
                )
            )
        ) {
            addDocument(documentMapper, writer, "first", first);
            writer.flush();
            addDocument(documentMapper, writer, "second", second);
            writer.flush();
            writer.commit();

            assertTopDocument(directory, fdeField, queryFde, "first");

            writer.forceMerge(1);
            writer.commit();
            assertTopDocument(directory, fdeField, queryFde, "first");
        }
    }

    public void testIndexedMuveraWorksOnPreviouslyCreatedCompatibleIndex() throws Exception {
        IndexVersion previousVersion = IndexVersionUtils.randomPreviousCompatibleWriteVersion(IndexVersion.current());
        MapperService mapperService = createMapperService(previousVersion, indexedMuveraMapping());
        float[][] vectors = unitTokens(0, 1);
        float[] queryFde = encoder().encodeQuery(vectors);

        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setCodec(
                    new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, threadPool).codec("default")
                )
            )
        ) {
            addDocument(mapperService.documentMapper(), writer, "old-index", vectors);
            writer.commit();
            assertTopDocument(directory, "field", queryFde, "old-index");
        }
    }

    public void testMuveraQueryReturnsOnlyExactMaxSimScores() throws Exception {
        MapperService mapperService = createMapperService(indexedMuveraMapping());
        DocumentMapper documentMapper = mapperService.documentMapper();
        float[][] queryVectors = unitTokens(0, 1);

        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setCodec(
                    new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, threadPool).codec("default")
                )
            )
        ) {
            addDocument(documentMapper, writer, "exact-two", unitTokens(0, 1));
            addDocument(documentMapper, writer, "exact-zero", unitTokens(2, 3));
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                RankVectorsQueryBuilder queryBuilder = new RankVectorsQueryBuilder("field", queryVectors, 2, 2);
                TopDocs topDocs = searcher.search(queryBuilder.toQuery(createSearchExecutionContext(mapperService, searcher)), 2);
                assertEquals(2, topDocs.scoreDocs.length);
                assertEquals("exact-two", searcher.storedFields().document(topDocs.scoreDocs[0].doc).get("test_id"));
                assertEquals(3f, topDocs.scoreDocs[0].score, 0f);
                assertEquals("exact-zero", searcher.storedFields().document(topDocs.scoreDocs[1].doc).get("test_id"));
                assertEquals(1f, topDocs.scoreDocs[1].score, 0f);
            }
        }
    }

    public void testMuveraCandidatesUseIvfQueryPath() {
        assertThat(
            MuveraCandidateQuery.approximateQuery("field", new float[RankVectorsFieldMapper.MUVERA_FDE_DIMS], 10, 100, null),
            instanceOf(IVFKnnFloatVectorQuery.class)
        );
    }

    public void testIvfCandidatesAreUniqueAndBoundedByRescoreOversample() throws Exception {
        MapperService mapperService = createMapperService(indexedMuveraMapping());
        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setCodec(
                    new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, threadPool).codec("default")
                )
            )
        ) {
            for (int i = 0; i < 100; i++) {
                addDocument(mapperService.documentMapper(), writer, Integer.toString(i), unitTokens(i % 64, (i + 1) % 64));
            }
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                int k = 10;
                int numCandidates = 50;
                TopDocs candidates = new IndexSearcher(reader).search(
                    new MuveraCandidateQuery("field", unitTokens(0, 1), k, numCandidates, null, encoder()),
                    numCandidates
                );
                assertTrue(candidates.scoreDocs.length > 0);
                assertTrue(candidates.scoreDocs.length <= Math.ceil(k * DEFAULT_OVERSAMPLE));
                HashSet<Integer> uniqueDocs = new HashSet<>();
                for (var scoreDoc : candidates.scoreDocs) {
                    assertTrue("duplicate candidate doc [" + scoreDoc.doc + "]", uniqueDocs.add(scoreDoc.doc));
                }
            }
        }
    }

    public void testMuveraQueryValidatesVectorsBeforeRewrite() throws Exception {
        MapperService mapperService = createMapperService(indexedMuveraMapping());
        float[][] nonUnitQuery = unitTokens(0, 1);
        nonUnitQuery[0][1] = 1f;
        RankVectorsFieldMapper.RankVectorsFieldType fieldType = (RankVectorsFieldMapper.RankVectorsFieldType) mapperService.fieldType(
            "field"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fieldType.maxSimQuery(nonUnitQuery, 1, 1, null));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("must have unit magnitude"));
    }

    public void testPublicQueryReturnsNoDocsForUnmappedField() throws Exception {
        MapperService mapperService = createMapperService(indexedMuveraMapping());
        RankVectorsQueryBuilder queryBuilder = new RankVectorsQueryBuilder("unmapped", unitTokens(0, 1), 1, 1);
        assertSame(Queries.NO_DOCS_INSTANCE, queryBuilder.toQuery(createSearchExecutionContext(mapperService)));
    }

    public void testPublicQueryRequiresIndexedRankVectorsField() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "rank_vectors");
            b.field("dims", RankVectorsFieldMapper.MUVERA_TOKEN_DIMS);
        }));
        RankVectorsQueryBuilder queryBuilder = new RankVectorsQueryBuilder("field", unitTokens(0, 1), 1, 1);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> queryBuilder.toQuery(createSearchExecutionContext(mapperService))
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("not indexed for approximate MaxSim search"));
    }

    public void testPublicQueryRejectsOtherFieldTypes() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        RankVectorsQueryBuilder queryBuilder = new RankVectorsQueryBuilder("field", unitTokens(0, 1), 1, 1);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> queryBuilder.toQuery(createSearchExecutionContext(mapperService))
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("only supported on [rank_vectors] fields"));
    }

    public void testExactMaxSimScoreTransformIsMonotonicAcrossZero() {
        assertTrue(MuveraMaxSimQuery.score(-2f) < MuveraMaxSimQuery.score(-1f));
        assertTrue(MuveraMaxSimQuery.score(-1f) < MuveraMaxSimQuery.score(0f));
        assertTrue(MuveraMaxSimQuery.score(0f) < MuveraMaxSimQuery.score(1f));
    }

    public void testExactMaxSimHonorsBytesRefOffset() {
        int prefixLength = 7;
        byte[] bytes = new byte[prefixLength + RankVectorsFieldMapper.MUVERA_TOKEN_DIMS * Float.BYTES];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putFloat(prefixLength, 1f);
        assertEquals(
            1f,
            MuveraMaxSimQuery.exactMaxSim(
                new float[][] { unitTokens(0, 1)[0] },
                new BytesRef(bytes, prefixLength, bytes.length - prefixLength),
                RankVectorsFieldMapper.MUVERA_TOKEN_DIMS
            ),
            0f
        );
    }

    public void testMuveraQueryRejectsExcessiveExactWork() {
        float[][] queryVectors = new float[RankVectorsFieldMapper.MAX_MUVERA_VECTORS][];
        Arrays.fill(queryVectors, new float[RankVectorsFieldMapper.MUVERA_TOKEN_DIMS]);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MuveraMaxSimQuery(
                "field",
                queryVectors,
                MuveraMaxSimQuery.MAX_CANDIDATES,
                MuveraMaxSimQuery.MAX_CANDIDATES,
                null,
                encoder()
            )
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("exact MaxSim work"));
    }

    public void testExactMaxSimChecksCancellationWhileScoringDocumentVectors() {
        float[][] queryVectors = unitTokens(0, 1);
        byte[] bytes = new byte[2 * RankVectorsFieldMapper.MUVERA_TOKEN_DIMS * Float.BYTES];
        AtomicInteger checks = new AtomicInteger();
        expectThrows(
            TestCancellationException.class,
            () -> MuveraMaxSimQuery.exactMaxSim(queryVectors, new BytesRef(bytes), RankVectorsFieldMapper.MUVERA_TOKEN_DIMS, () -> {
                if (checks.incrementAndGet() == 2) {
                    throw new TestCancellationException();
                }
            })
        );
        assertEquals(2, checks.get());
    }

    public void testMuveraQueryEncodingChecksCancellation() {
        AtomicInteger checks = new AtomicInteger();
        expectThrows(TestCancellationException.class, () -> encoder().encodeQuery(unitTokens(0, 1), () -> {
            if (checks.incrementAndGet() == 2) {
                throw new TestCancellationException();
            }
        }));
        assertEquals(2, checks.get());
    }

    public void testIndexedMuveraSyntheticSourceContainsOnlyOriginalVectors() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
            .build();
        MapperService mapperService = createMapperService(getVersion(), settings, () -> true, indexedMuveraMapping());
        float[][] vectors = unitTokens(0, 1);
        String reconstructed;
        try (
            TestThreadPool threadPool = new TestThreadPool(getTestName());
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig().setCodec(
                    new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, threadPool).codec("default")
                )
            )
        ) {
            ParsedDocument parsed = mapperService.documentMapper().parse(source(b -> b.field("field", vectors)));
            parsed.updateSeqID(0, 0);
            parsed.version().setLongValue(0);
            writer.addDocuments(parsed.docs());
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                reconstructed = syntheticSource(mapperService.documentMapper(), reader, parsed.docs().size() - 1);
            }
        }
        assertFalse(reconstructed.contains("_muvera"));
        assertThat(reconstructed, equalTo("{\"field\":[" + vectorJson(0) + "," + vectorJson(1) + "]}"));
    }

    private void addDocument(DocumentMapper mapper, IndexWriter writer, String id, float[][] vectors) throws IOException {
        ParsedDocument parsed = mapper.parse(source(b -> b.field("field", vectors)));
        parsed.rootDoc().add(new StoredField("test_id", id));
        writer.addDocuments(parsed.docs());
    }

    private static void assertTopDocument(Directory directory, String field, float[] query, String expectedId) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            assertNotNull(reader.leaves().get(0).reader().getBinaryDocValues(field));
            assertEquals(RankVectorsFieldMapper.MUVERA_FDE_DIMS, reader.leaves().get(0).reader().getFloatVectorValues(field).dimension());
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(new KnnFloatVectorQuery(field, query, 1), 1);
            assertEquals(1, topDocs.scoreDocs.length);
            assertEquals(expectedId, searcher.storedFields().document(topDocs.scoreDocs[0].doc).get("test_id"));
        }
    }

    private static MuveraEncoder encoder() {
        return new MuveraEncoder(
            RankVectorsFieldMapper.MUVERA_TOKEN_DIMS,
            RankVectorsFieldMapper.MUVERA_K_SIM,
            RankVectorsFieldMapper.MUVERA_D_PROJ,
            RankVectorsFieldMapper.MUVERA_R_REPS,
            RankVectorsFieldMapper.MUVERA_SEED
        );
    }

    private static float[][] unitTokens(int firstDimension, int secondDimension) {
        float[][] vectors = new float[2][RankVectorsFieldMapper.MUVERA_TOKEN_DIMS];
        vectors[0][firstDimension] = 1f;
        vectors[1][secondDimension] = 1f;
        return vectors;
    }

    private static String vectorJson(int unitDimension) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < RankVectorsFieldMapper.MUVERA_TOKEN_DIMS; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(i == unitDimension ? "1.0" : "0.0");
        }
        return builder.append(']').toString();
    }

    private static class TestCancellationException extends RuntimeException {}

    private static org.elasticsearch.xcontent.XContentBuilder indexedMuveraMapping() throws IOException {
        return fieldMapping(b -> {
            b.field("type", "rank_vectors");
            b.field("dims", RankVectorsFieldMapper.MUVERA_TOKEN_DIMS);
            b.field("index", true);
        });
    }
}
