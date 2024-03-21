/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataMapperTestCase;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.model.TestModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.inference.mapper.InferenceResultFieldMapper.INFERENCE_CHUNKS_RESULTS;
import static org.elasticsearch.xpack.inference.mapper.InferenceResultFieldMapper.INFERENCE_CHUNKS_TEXT;
import static org.elasticsearch.xpack.inference.mapper.InferenceResultFieldMapper.RESULTS;
import static org.hamcrest.Matchers.containsString;

public class InferenceResultFieldMapperTests extends MetadataMapperTestCase {
    private record SemanticTextInferenceResults(String fieldName, ChunkedInferenceServiceResults results, List<String> text) {}

    private record VisitedChildDocInfo(String path, int numChunks) {}

    private record SparseVectorSubfieldOptions(boolean include, boolean includeEmbedding, boolean includeIsTruncated) {}

    @Override
    protected String fieldName() {
        return InferenceResultFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected boolean isSupportedOn(IndexVersion version) {
        return version.onOrAfter(IndexVersions.ES_VERSION_8_12_1); // TODO: Switch to ES_VERSION_8_14 when available
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {

    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    public void testSuccessfulParse() throws IOException {
        final String fieldName1 = randomAlphaOfLengthBetween(5, 15);
        final String fieldName2 = randomAlphaOfLengthBetween(5, 15);

        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> {
            addSemanticTextMapping(b, fieldName1, randomAlphaOfLength(8));
            addSemanticTextMapping(b, fieldName2, randomAlphaOfLength(8));
        }));
        ParsedDocument doc = documentMapper.parse(
            source(
                b -> addSemanticTextInferenceResults(
                    b,
                    List.of(
                        randomSemanticTextInferenceResults(fieldName1, List.of("a b", "c")),
                        randomSemanticTextInferenceResults(fieldName2, List.of("d e f"))
                    )
                )
            )
        );

        Set<VisitedChildDocInfo> visitedChildDocs = new HashSet<>();
        Set<VisitedChildDocInfo> expectedVisitedChildDocs = Set.of(
            new VisitedChildDocInfo(fieldName1, 2),
            new VisitedChildDocInfo(fieldName1, 1),
            new VisitedChildDocInfo(fieldName2, 3)
        );

        List<LuceneDocument> luceneDocs = doc.docs();
        assertEquals(4, luceneDocs.size());
        assertValidChildDoc(luceneDocs.get(0), doc.rootDoc(), visitedChildDocs);
        assertValidChildDoc(luceneDocs.get(1), doc.rootDoc(), visitedChildDocs);
        assertValidChildDoc(luceneDocs.get(2), doc.rootDoc(), visitedChildDocs);
        assertEquals(doc.rootDoc(), luceneDocs.get(3));
        assertNull(luceneDocs.get(3).getParent());
        assertEquals(expectedVisitedChildDocs, visitedChildDocs);

        MapperService nestedMapperService = createMapperService(mapping(b -> {
            addInferenceResultsNestedMapping(b, fieldName1);
            addInferenceResultsNestedMapping(b, fieldName2);
        }));
        withLuceneIndex(nestedMapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            NestedDocuments nested = new NestedDocuments(
                nestedMapperService.mappingLookup(),
                QueryBitSetProducer::new,
                IndexVersion.current()
            );
            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

            Set<SearchHit.NestedIdentity> visitedNestedIdentities = new HashSet<>();
            Set<SearchHit.NestedIdentity> expectedVisitedNestedIdentities = Set.of(
                new SearchHit.NestedIdentity(fieldName1, 0, null),
                new SearchHit.NestedIdentity(fieldName1, 1, null),
                new SearchHit.NestedIdentity(fieldName2, 0, null)
            );

            assertChildLeafNestedDocument(leaf, 0, 3, visitedNestedIdentities);
            assertChildLeafNestedDocument(leaf, 1, 3, visitedNestedIdentities);
            assertChildLeafNestedDocument(leaf, 2, 3, visitedNestedIdentities);
            assertEquals(expectedVisitedNestedIdentities, visitedNestedIdentities);

            assertNull(leaf.advance(3));
            assertEquals(3, leaf.doc());
            assertEquals(3, leaf.rootDoc());
            assertNull(leaf.nestedIdentity());

            IndexSearcher searcher = newSearcher(reader);
            {
                TopDocs topDocs = searcher.search(
                    generateNestedTermSparseVectorQuery(nestedMapperService.mappingLookup().nestedLookup(), fieldName1, List.of("a")),
                    10
                );
                assertEquals(1, topDocs.totalHits.value);
                assertEquals(3, topDocs.scoreDocs[0].doc);
            }
            {
                TopDocs topDocs = searcher.search(
                    generateNestedTermSparseVectorQuery(nestedMapperService.mappingLookup().nestedLookup(), fieldName1, List.of("a", "b")),
                    10
                );
                assertEquals(1, topDocs.totalHits.value);
                assertEquals(3, topDocs.scoreDocs[0].doc);
            }
            {
                TopDocs topDocs = searcher.search(
                    generateNestedTermSparseVectorQuery(nestedMapperService.mappingLookup().nestedLookup(), fieldName2, List.of("d")),
                    10
                );
                assertEquals(1, topDocs.totalHits.value);
                assertEquals(3, topDocs.scoreDocs[0].doc);
            }
            {
                TopDocs topDocs = searcher.search(
                    generateNestedTermSparseVectorQuery(nestedMapperService.mappingLookup().nestedLookup(), fieldName2, List.of("z")),
                    10
                );
                assertEquals(0, topDocs.totalHits.value);
            }
        });
    }

    public void testMissingSubfields() throws IOException {
        final String fieldName = randomAlphaOfLengthBetween(5, 15);

        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> addSemanticTextMapping(b, fieldName, randomAlphaOfLength(8))));

        {
            DocumentParsingException ex = expectThrows(
                DocumentParsingException.class,
                DocumentParsingException.class,
                () -> documentMapper.parse(
                    source(
                        b -> addSemanticTextInferenceResults(
                            b,
                            List.of(randomSemanticTextInferenceResults(fieldName, List.of("a b"))),
                            new SparseVectorSubfieldOptions(false, true, true),
                            true,
                            Map.of()
                        )
                    )
                )
            );
            assertThat(ex.getMessage(), containsString("Missing required subfields: [" + INFERENCE_CHUNKS_RESULTS + "]"));
        }
        {
            DocumentParsingException ex = expectThrows(
                DocumentParsingException.class,
                DocumentParsingException.class,
                () -> documentMapper.parse(
                    source(
                        b -> addSemanticTextInferenceResults(
                            b,
                            List.of(randomSemanticTextInferenceResults(fieldName, List.of("a b"))),
                            new SparseVectorSubfieldOptions(true, true, true),
                            false,
                            Map.of()
                        )
                    )
                )
            );
            assertThat(ex.getMessage(), containsString("Missing required subfields: [" + INFERENCE_CHUNKS_TEXT + "]"));
        }
        {
            DocumentParsingException ex = expectThrows(
                DocumentParsingException.class,
                DocumentParsingException.class,
                () -> documentMapper.parse(
                    source(
                        b -> addSemanticTextInferenceResults(
                            b,
                            List.of(randomSemanticTextInferenceResults(fieldName, List.of("a b"))),
                            new SparseVectorSubfieldOptions(false, true, true),
                            false,
                            Map.of()
                        )
                    )
                )
            );
            assertThat(
                ex.getMessage(),
                containsString("Missing required subfields: [" + INFERENCE_CHUNKS_RESULTS + ", " + INFERENCE_CHUNKS_TEXT + "]")
            );
        }
    }

    public void testExtraSubfields() throws IOException {
        final String fieldName = randomAlphaOfLengthBetween(5, 15);
        final List<SemanticTextInferenceResults> semanticTextInferenceResultsList = List.of(
            randomSemanticTextInferenceResults(fieldName, List.of("a b"))
        );

        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> addSemanticTextMapping(b, fieldName, randomAlphaOfLength(8))));

        Consumer<ParsedDocument> checkParsedDocument = d -> {
            Set<VisitedChildDocInfo> visitedChildDocs = new HashSet<>();
            Set<VisitedChildDocInfo> expectedVisitedChildDocs = Set.of(new VisitedChildDocInfo(fieldName, 2));

            List<LuceneDocument> luceneDocs = d.docs();
            assertEquals(2, luceneDocs.size());
            assertValidChildDoc(luceneDocs.get(0), d.rootDoc(), visitedChildDocs);
            assertEquals(d.rootDoc(), luceneDocs.get(1));
            assertNull(luceneDocs.get(1).getParent());
            assertEquals(expectedVisitedChildDocs, visitedChildDocs);
        };

        {
            ParsedDocument doc = documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        b,
                        semanticTextInferenceResultsList,
                        new SparseVectorSubfieldOptions(true, true, true),
                        true,
                        Map.of("extra_key", "extra_value")
                    )
                )
            );

            checkParsedDocument.accept(doc);
            LuceneDocument childDoc = doc.docs().get(0);
            assertEquals(0, childDoc.getFields(childDoc.getPath() + ".extra_key").size());
        }
        {
            ParsedDocument doc = documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        b,
                        semanticTextInferenceResultsList,
                        new SparseVectorSubfieldOptions(true, true, true),
                        true,
                        Map.of("extra_key", Map.of("k1", "v1"))
                    )
                )
            );

            checkParsedDocument.accept(doc);
            LuceneDocument childDoc = doc.docs().get(0);
            assertEquals(0, childDoc.getFields(childDoc.getPath() + ".extra_key").size());
        }
        {
            ParsedDocument doc = documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        b,
                        semanticTextInferenceResultsList,
                        new SparseVectorSubfieldOptions(true, true, true),
                        true,
                        Map.of("extra_key", List.of("v1"))
                    )
                )
            );

            checkParsedDocument.accept(doc);
            LuceneDocument childDoc = doc.docs().get(0);
            assertEquals(0, childDoc.getFields(childDoc.getPath() + ".extra_key").size());
        }
        {
            Map<String, Object> extraSubfields = new HashMap<>();
            extraSubfields.put("extra_key", null);

            ParsedDocument doc = documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        b,
                        semanticTextInferenceResultsList,
                        new SparseVectorSubfieldOptions(true, true, true),
                        true,
                        extraSubfields
                    )
                )
            );

            checkParsedDocument.accept(doc);
            LuceneDocument childDoc = doc.docs().get(0);
            assertEquals(0, childDoc.getFields(childDoc.getPath() + ".extra_key").size());
        }
    }

    public void testMissingSemanticTextMapping() throws IOException {
        final String fieldName = randomAlphaOfLengthBetween(5, 15);

        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> {}));
        DocumentParsingException ex = expectThrows(
            DocumentParsingException.class,
            DocumentParsingException.class,
            () -> documentMapper.parse(
                source(b -> addSemanticTextInferenceResults(b, List.of(randomSemanticTextInferenceResults(fieldName, List.of("a b")))))
            )
        );
        assertThat(
            ex.getMessage(),
            containsString(
                Strings.format("Field [%s] is not registered as a %s field type", fieldName, SemanticTextFieldMapper.CONTENT_TYPE)
            )
        );
    }

    private static void addSemanticTextMapping(XContentBuilder mappingBuilder, String fieldName, String modelId) throws IOException {
        mappingBuilder.startObject(fieldName);
        mappingBuilder.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mappingBuilder.field("model_id", modelId);
        mappingBuilder.endObject();
    }

    public static ChunkedTextEmbeddingResults randomTextEmbeddings(List<String> inputs) {
        List<org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk> chunks = new ArrayList<>();
        for (String input : inputs) {
            double[] values = new double[5];
            for (int j = 0; j < values.length; j++) {
                values[j] = randomDouble();
            }
            chunks.add(new org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk(input, values));
        }
        return new ChunkedTextEmbeddingResults(chunks);
    }

    public static ChunkedSparseEmbeddingResults randomSparseEmbeddings(List<String> inputs) {
        List<ChunkedTextExpansionResults.ChunkedResult> chunks = new ArrayList<>();
        for (String input : inputs) {
            var tokens = new ArrayList<TextExpansionResults.WeightedToken>();
            for (var token : input.split("\\s+")) {
                tokens.add(new TextExpansionResults.WeightedToken(token, randomFloat()));
            }
            chunks.add(new ChunkedTextExpansionResults.ChunkedResult(input, tokens));
        }
        return new ChunkedSparseEmbeddingResults(chunks);
    }

    private static SemanticTextInferenceResults randomSemanticTextInferenceResults(String semanticTextFieldName, List<String> chunks) {
        return new SemanticTextInferenceResults(semanticTextFieldName, randomSparseEmbeddings(chunks), chunks);
    }

    private static void addSemanticTextInferenceResults(
        XContentBuilder sourceBuilder,
        List<SemanticTextInferenceResults> semanticTextInferenceResults
    ) throws IOException {
        addSemanticTextInferenceResults(
            sourceBuilder,
            semanticTextInferenceResults,
            new SparseVectorSubfieldOptions(true, true, true),
            true,
            Map.of()
        );
    }

    @SuppressWarnings("unchecked")
    private static void addSemanticTextInferenceResults(
        XContentBuilder sourceBuilder,
        List<SemanticTextInferenceResults> semanticTextInferenceResults,
        SparseVectorSubfieldOptions sparseVectorSubfieldOptions,
        boolean includeTextSubfield,
        Map<String, Object> extraSubfields
    ) throws IOException {
        Map<String, Object> inferenceResultsMap = new HashMap<>();
        for (SemanticTextInferenceResults semanticTextInferenceResult : semanticTextInferenceResults) {
            InferenceResultFieldMapper.applyFieldInference(
                inferenceResultsMap,
                semanticTextInferenceResult.fieldName,
                randomModel(),
                semanticTextInferenceResult.results
            );
            Map<String, Object> optionsMap = (Map<String, Object>) inferenceResultsMap.get(semanticTextInferenceResult.fieldName);
            List<Map<String, Object>> fieldResultList = (List<Map<String, Object>>) optionsMap.get(RESULTS);
            for (var entry : fieldResultList) {
                if (includeTextSubfield == false) {
                    entry.remove(INFERENCE_CHUNKS_TEXT);
                }
                if (sparseVectorSubfieldOptions.include == false) {
                    entry.remove(INFERENCE_CHUNKS_RESULTS);
                }
                entry.putAll(extraSubfields);
            }
        }
        sourceBuilder.field(InferenceResultFieldMapper.NAME, inferenceResultsMap);
    }

    private static Model randomModel() {
        String serviceName = randomAlphaOfLengthBetween(5, 10);
        String inferenceId = randomAlphaOfLengthBetween(5, 10);
        return new TestModel(
            inferenceId,
            TaskType.SPARSE_EMBEDDING,
            serviceName,
            new TestModel.TestServiceSettings("my-model"),
            new TestModel.TestTaskSettings(randomIntBetween(1, 100)),
            new TestModel.TestSecretSettings(randomAlphaOfLength(10))
        );
    }

    private static void addInferenceResultsNestedMapping(XContentBuilder mappingBuilder, String semanticTextFieldName) throws IOException {
        mappingBuilder.startObject(semanticTextFieldName);
        {
            mappingBuilder.field("type", "nested");
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject(INFERENCE_CHUNKS_RESULTS);
                {
                    mappingBuilder.field("type", "sparse_vector");
                }
                mappingBuilder.endObject();
                mappingBuilder.startObject(INFERENCE_CHUNKS_TEXT);
                {
                    mappingBuilder.field("type", "text");
                    mappingBuilder.field("index", false);
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
    }

    private static Query generateNestedTermSparseVectorQuery(NestedLookup nestedLookup, String path, List<String> tokens) {
        NestedObjectMapper mapper = nestedLookup.getNestedMappers().get(path);
        assertNotNull(mapper);

        BitSetProducer parentFilter = new QueryBitSetProducer(Queries.newNonNestedFilter(IndexVersion.current()));
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (String token : tokens) {
            queryBuilder.add(
                new BooleanClause(new TermQuery(new Term(path + "." + INFERENCE_CHUNKS_RESULTS, token)), BooleanClause.Occur.MUST)
            );
        }
        queryBuilder.add(new BooleanClause(mapper.nestedTypeFilter(), BooleanClause.Occur.FILTER));

        return new ESToParentBlockJoinQuery(queryBuilder.build(), parentFilter, ScoreMode.Total, null);
    }

    private static void assertValidChildDoc(
        LuceneDocument childDoc,
        LuceneDocument expectedParent,
        Set<VisitedChildDocInfo> visitedChildDocs
    ) {
        assertEquals(expectedParent, childDoc.getParent());
        visitedChildDocs.add(
            new VisitedChildDocInfo(childDoc.getPath(), childDoc.getFields(childDoc.getPath() + "." + INFERENCE_CHUNKS_RESULTS).size())
        );
    }

    private static void assertChildLeafNestedDocument(
        LeafNestedDocuments leaf,
        int advanceToDoc,
        int expectedRootDoc,
        Set<SearchHit.NestedIdentity> visitedNestedIdentities
    ) throws IOException {

        assertNotNull(leaf.advance(advanceToDoc));
        assertEquals(advanceToDoc, leaf.doc());
        assertEquals(expectedRootDoc, leaf.rootDoc());
        assertNotNull(leaf.nestedIdentity());
        visitedNestedIdentities.add(leaf.nestedIdentity());
    }
}
