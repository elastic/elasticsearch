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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
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
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.diskbbq.DiskBBQPlugin;
import org.elasticsearch.xpack.rank.vectors.LocalStateRankVectors;
import org.elasticsearch.xpack.rank.vectors.fde.MuveraEncoder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

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

    private static org.elasticsearch.xcontent.XContentBuilder indexedMuveraMapping() throws IOException {
        return fieldMapping(b -> {
            b.field("type", "rank_vectors");
            b.field("dims", RankVectorsFieldMapper.MUVERA_TOKEN_DIMS);
            b.field("index", true);
            b.startObject("index_options");
            b.field("type", "muvera");
            b.field("encoding_version", RankVectorsFieldMapper.MUVERA_ENCODING_VERSION);
            b.field("encoding_profile", RankVectorsFieldMapper.MUVERA_ENCODING_PROFILE);
            b.endObject();
        });
    }
}
