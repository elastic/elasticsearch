/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.ml.action.TransportCoordinatedInferenceAction;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.IVF_FORMAT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SemanticCrossClusterSearchIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, DEFAULT_SKIP_UNAVAILABLE);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, FakeMlPlugin.class);
    }

    public void testSemanticQuery() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };

        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String variableInferenceIdField = "variable-inference-id-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(localInferenceId)
            ),
            Map.of("local_doc_1", Map.of(commonInferenceIdField, "a"), "local_doc_2", Map.of(variableInferenceIdField, "b"))
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                remoteInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(remoteInferenceId)
            ),
            Map.of("remote_doc_1", Map.of(commonInferenceIdField, "x"), "remote_doc_2", Map.of(variableInferenceIdField, "y"))
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SemanticQueryBuilder(commonInferenceIdField, "a"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_1"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_1")
            )
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new SemanticQueryBuilder(variableInferenceIdField, "b"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2")
            )
        );
    }

    public void testSemanticQueryWithCcMinimizeRoundTripsFalse() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };
        final SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder("foo", "bar");
        final Consumer<SearchRequest> assertCcsMinimizeRoundTripsFalseFailure = s -> {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(s).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo("semantic query does not support cross-cluster search when [ccs_minimize_roundtrips] is false")
            );
        };

        final TestIndexInfo localIndexInfo = new TestIndexInfo(localIndexName, Map.of(), Map.of(), Map.of());
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(remoteIndexName, Map.of(), Map.of(), Map.of());
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Explicitly set ccs_minimize_roundtrips=false in the search request
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
        SearchRequest searchRequestWithCcMinimizeRoundTripsFalse = new SearchRequest(queryIndices, searchSourceBuilder);
        searchRequestWithCcMinimizeRoundTripsFalse.setCcsMinimizeRoundtrips(false);
        assertCcsMinimizeRoundTripsFalseFailure.accept(searchRequestWithCcMinimizeRoundTripsFalse);

        // Using a point in time implicitly sets ccs_minimize_roundtrips=false
        BytesReference pitId = openPointInTime(queryIndices, TimeValue.timeValueMinutes(2));
        SearchSourceBuilder searchSourceBuilderWithPit = new SearchSourceBuilder().query(queryBuilder)
            .pointInTimeBuilder(new PointInTimeBuilder(pitId));
        SearchRequest searchRequestWithPit = new SearchRequest().source(searchSourceBuilderWithPit);
        assertCcsMinimizeRoundTripsFalseFailure.accept(searchRequestWithPit);
    }

    public void testMatchQuery() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };

        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String variableInferenceIdField = "variable-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(localInferenceId),
                mixedTypeField1,
                semanticTextMapping(localInferenceId),
                mixedTypeField2,
                textMapping()
            ),
            Map.of(
                "local_doc_1",
                Map.of(commonInferenceIdField, "a"),
                "local_doc_2",
                Map.of(variableInferenceIdField, "b"),
                "local_doc_3",
                Map.of(mixedTypeField1, "c"),
                "local_doc_4",
                Map.of(mixedTypeField2, "d")
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                remoteInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                variableInferenceIdField,
                semanticTextMapping(remoteInferenceId),
                mixedTypeField1,
                textMapping(),
                mixedTypeField2,
                semanticTextMapping(remoteInferenceId)
            ),
            Map.of(
                "remote_doc_1",
                Map.of(commonInferenceIdField, "w"),
                "remote_doc_2",
                Map.of(variableInferenceIdField, "x"),
                "remote_doc_3",
                Map.of(mixedTypeField1, "y"),
                "remote_doc_4",
                Map.of(mixedTypeField2, "z")
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new MatchQueryBuilder(commonInferenceIdField, "a"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_1"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_1")
            )
        );

        // Query a field that has different inference ID values across clusters
        assertSearchResponse(
            new MatchQueryBuilder(variableInferenceIdField, "b"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2")
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new MatchQueryBuilder(mixedTypeField1, "y"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_3")
            )
        );
        assertSearchResponse(
            new MatchQueryBuilder(mixedTypeField2, "d"),
            queryIndices,
            List.of(
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_4"),
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_4")
            )
        );
    }

    public void testMatchQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };
        final Consumer<QueryBuilder> assertCcsMinimizeRoundTripsFalseFailure = q -> {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(q);
            SearchRequest searchRequest = new SearchRequest(queryIndices, searchSourceBuilder);
            searchRequest.setCcsMinimizeRoundtrips(false);

            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(searchRequest).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "match query does not support cross-cluster search when querying a [semantic_text] field when "
                        + "[ccs_minimize_roundtrips] is false"
                )
            );
        };

        final String commonInferenceId = "common-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";
        final String textField = "text-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                textMapping(),
                textField,
                textMapping()
            ),
            Map.of(mixedTypeField2 + "_doc", Map.of(mixedTypeField2, "a"), textField + "_doc", Map.of(textField, "b b b"))
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                textMapping(),
                mixedTypeField2,
                semanticTextMapping(commonInferenceId),
                textField,
                textMapping()
            ),
            Map.of(textField + "_doc", Map.of(textField, "b"))
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(new MatchQueryBuilder(commonInferenceIdField, randomAlphaOfLength(5)));
        assertCcsMinimizeRoundTripsFalseFailure.accept(new MatchQueryBuilder(mixedTypeField1, randomAlphaOfLength(5)));

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new MatchQueryBuilder(mixedTypeField2, "a"),
            queryIndices,
            List.of(new SearchResult(null, localIndexName, mixedTypeField2 + "_doc")),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(
                    new FailureCause(
                        QueryShardException.class,
                        "failed to create query: Field [mixed-type-field-2] of type [semantic_text] does not support match queries"
                    )
                )
            ),
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Validate that a CCS match query functions when only text fields are queried
        assertSearchResponse(
            new MatchQueryBuilder(textField, "b"),
            queryIndices,
            List.of(
                new SearchResult(null, localIndexName, textField + "_doc"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, textField + "_doc")
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }

    public void testKnnQuery() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };

        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                localInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                denseVectorMapping(384),
                mixedTypeField2,
                semanticTextMapping(localInferenceId)
            ),
            Map.of(
                "local_doc_1",
                Map.of(commonInferenceIdField, "a"),
                "local_doc_2",
                Map.of(mixedTypeField1, generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                "local_doc_3",
                Map.of(mixedTypeField2, "c")
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                denseVectorMapping(384)
            ),
            Map.of(
                "remote_doc_1",
                Map.of(commonInferenceIdField, "x"),
                "remote_doc_2",
                Map.of(mixedTypeField1, "y"),
                "remote_doc_3",
                Map.of(mixedTypeField2, generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                commonInferenceIdField,
                new TextEmbeddingQueryVectorBuilder(null, "a"),
                10,
                100,
                IVF_FORMAT.isEnabled() ? 10f : null,
                null
            ),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_1"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_1")
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                mixedTypeField1,
                new TextEmbeddingQueryVectorBuilder(localInferenceId, "y"),
                10,
                100,
                IVF_FORMAT.isEnabled() ? 10f : null,
                null
            ),
            queryIndices,
            List.of(
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2"),
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2")
            )
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                mixedTypeField2,
                new TextEmbeddingQueryVectorBuilder(localInferenceId, "c"),
                10,
                100,
                IVF_FORMAT.isEnabled() ? 10f : null,
                null
            ),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_3")
            )
        );

        // Query a field that has mixed types across clusters using a query vector
        final VectorData queryVector = new VectorData(
            generateDenseVectorFieldValue(384, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(mixedTypeField1, queryVector, 10, 100, IVF_FORMAT.isEnabled() ? 10f : null, null, null),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2")
            )
        );
        assertSearchResponse(
            new KnnVectorQueryBuilder(mixedTypeField2, queryVector, 10, 100, IVF_FORMAT.isEnabled() ? 10f : null, null, null),
            queryIndices,
            List.of(
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_3"),
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_3")
            )
        );

        // Check that omitting the inference ID when querying a remote dense vector field leads to the expected partial failure
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                mixedTypeField2,
                new TextEmbeddingQueryVectorBuilder(null, "c"),
                10,
                100,
                IVF_FORMAT.isEnabled() ? 10f : null,
                null
            ),
            queryIndices,
            List.of(new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_3")),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(new FailureCause(IllegalArgumentException.class, "[model_id] must not be null."))
            ),
            null
        );
    }

    public void testKnnQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };
        final BiConsumer<String, TextEmbeddingQueryVectorBuilder> assertCcsMinimizeRoundTripsFalseFailure = (f, qvb) -> {
            KnnVectorQueryBuilder queryBuilder = new KnnVectorQueryBuilder(f, qvb, 10, 100, IVF_FORMAT.isEnabled() ? 10f : null, null);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
            SearchRequest searchRequest = new SearchRequest(queryIndices, searchSourceBuilder);
            searchRequest.setCcsMinimizeRoundtrips(false);

            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(searchRequest).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "knn query does not support cross-cluster search when querying a [semantic_text] field when "
                        + "[ccs_minimize_roundtrips] is false"
                )
            );
        };

        final int dimensions = 256;
        final String commonInferenceId = "common-inference-id";
        final MinimalServiceSettings commonInferenceIdServiceSettings = textEmbeddingServiceSettings(
            dimensions,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";
        final String denseVectorField = "dense-vector-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, commonInferenceIdServiceSettings),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                denseVectorMapping(dimensions),
                denseVectorField,
                denseVectorMapping(dimensions)
            ),
            Map.of(
                mixedTypeField2 + "_doc",
                Map.of(mixedTypeField2, generateDenseVectorFieldValue(dimensions, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f)),
                denseVectorField + "_doc",
                Map.of(denseVectorField, generateDenseVectorFieldValue(dimensions, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f))
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(commonInferenceId, commonInferenceIdServiceSettings),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                denseVectorMapping(dimensions),
                mixedTypeField2,
                semanticTextMapping(commonInferenceId),
                denseVectorField,
                denseVectorMapping(dimensions)
            ),
            Map.of(
                mixedTypeField2 + "_doc",
                Map.of(mixedTypeField2, "a"),
                denseVectorField + "_doc",
                Map.of(denseVectorField, generateDenseVectorFieldValue(dimensions, DenseVectorFieldMapper.ElementType.FLOAT, -128.0f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            commonInferenceIdField,
            new TextEmbeddingQueryVectorBuilder(null, randomAlphaOfLength(5))
        );
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            mixedTypeField1,
            new TextEmbeddingQueryVectorBuilder(commonInferenceId, randomAlphaOfLength(5))
        );

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                mixedTypeField2,
                new TextEmbeddingQueryVectorBuilder(commonInferenceId, "foo"),
                10,
                100,
                IVF_FORMAT.isEnabled() ? 10f : null,
                null
            ),
            queryIndices,
            List.of(new SearchResult(null, localIndexName, mixedTypeField2 + "_doc")),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(
                    new FailureCause(
                        QueryShardException.class,
                        "failed to create query: [knn] queries are only supported on [dense_vector] fields"
                    )
                )
            ),
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Validate that a CCS knn query functions when only dense vector fields are queried
        assertSearchResponse(
            new KnnVectorQueryBuilder(
                denseVectorField,
                generateDenseVectorFieldValue(dimensions, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f),
                10,
                100,
                IVF_FORMAT.isEnabled() ? 10f : null,
                null,
                null
            ),
            queryIndices,
            List.of(
                new SearchResult(null, localIndexName, denseVectorField + "_doc"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, denseVectorField + "_doc")
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }

    public void testSparseVectorQuery() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };

        final String commonInferenceId = "common-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                sparseVectorMapping(),
                mixedTypeField2,
                semanticTextMapping(commonInferenceId)
            ),
            Map.of(
                "local_doc_1",
                Map.of(commonInferenceIdField, "a"),
                "local_doc_2",
                Map.of(mixedTypeField1, generateSparseVectorFieldValue(1.0f)),
                "local_doc_3",
                Map.of(mixedTypeField2, "c")
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                sparseVectorMapping()
            ),
            Map.of(
                "remote_doc_1",
                Map.of(commonInferenceIdField, "x"),
                "remote_doc_2",
                Map.of(mixedTypeField1, "y"),
                "remote_doc_3",
                Map.of(mixedTypeField2, generateSparseVectorFieldValue(1.0f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Query a field has the same inference ID value across clusters, but with different backing inference services
        assertSearchResponse(
            new SparseVectorQueryBuilder(commonInferenceIdField, null, "a"),
            queryIndices,
            List.of(
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_1"),
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_1")
            )
        );

        // Query a field that has mixed types across clusters
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField1, commonInferenceId, "b"),
            queryIndices,
            List.of(
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2"),
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2")
            )
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField2, commonInferenceId, "c"),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_3")
            )
        );

        // Query a field that has mixed types across clusters using a query vector
        final List<WeightedToken> queryVector = generateSparseVectorFieldValue(1.0f).entrySet()
            .stream()
            .map(e -> new WeightedToken(e.getKey(), e.getValue()))
            .toList();
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField1, queryVector, null, null, null, null),
            queryIndices,
            List.of(
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_2"),
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_2")
            )
        );
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField2, queryVector, null, null, null, null),
            queryIndices,
            List.of(
                new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_3"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, "remote_doc_3")
            )
        );

        // Check that omitting the inference ID when querying a remote sparse vector field leads to the expected partial failure
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField2, null, "c"),
            queryIndices,
            List.of(new SearchResult(LOCAL_CLUSTER, localIndexName, "local_doc_3")),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(new FailureCause(IllegalArgumentException.class, "inference_id required to perform vector search on query string"))
            ),
            null
        );
    }

    public void testSparseVectorQueryWithCcsMinimizeRoundTripsFalse() throws Exception {
        final String localIndexName = "local-index";
        final String remoteIndexName = "remote-index";
        final String[] queryIndices = new String[] { localIndexName, fullyQualifiedIndexName(REMOTE_CLUSTER, remoteIndexName) };
        final Consumer<QueryBuilder> assertCcsMinimizeRoundTripsFalseFailure = q -> {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(q);
            SearchRequest searchRequest = new SearchRequest(queryIndices, searchSourceBuilder);
            searchRequest.setCcsMinimizeRoundtrips(false);

            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().search(searchRequest).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "sparse_vector query does not support cross-cluster search when querying a [semantic_text] field when "
                        + "[ccs_minimize_roundtrips] is false"
                )
            );
        };

        final String commonInferenceId = "common-inference-id";

        final String commonInferenceIdField = "common-inference-id-field";
        final String mixedTypeField1 = "mixed-type-field-1";
        final String mixedTypeField2 = "mixed-type-field-2";
        final String sparseVectorField = "sparse-vector-field";

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            localIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                semanticTextMapping(commonInferenceId),
                mixedTypeField2,
                sparseVectorMapping(),
                sparseVectorField,
                sparseVectorMapping()
            ),
            Map.of(
                mixedTypeField2 + "_doc",
                Map.of(mixedTypeField2, generateSparseVectorFieldValue(1.0f)),
                sparseVectorField + "_doc",
                Map.of(sparseVectorField, generateSparseVectorFieldValue(1.0f))
            )
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            remoteIndexName,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                commonInferenceIdField,
                semanticTextMapping(commonInferenceId),
                mixedTypeField1,
                sparseVectorMapping(),
                mixedTypeField2,
                semanticTextMapping(commonInferenceId),
                sparseVectorField,
                sparseVectorMapping()
            ),
            Map.of(
                mixedTypeField2 + "_doc",
                Map.of(mixedTypeField2, "a"),
                sparseVectorField + "_doc",
                Map.of(sparseVectorField, generateSparseVectorFieldValue(0.5f))
            )
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);

        // Validate that expected cases fail
        assertCcsMinimizeRoundTripsFalseFailure.accept(new SparseVectorQueryBuilder(commonInferenceIdField, null, randomAlphaOfLength(5)));
        assertCcsMinimizeRoundTripsFalseFailure.accept(
            new SparseVectorQueryBuilder(mixedTypeField1, commonInferenceId, randomAlphaOfLength(5))
        );

        // Validate the expected ccs_minimize_roundtrips=false detection gap and failure mode when querying non-inference fields locally
        assertSearchResponse(
            new SparseVectorQueryBuilder(mixedTypeField2, commonInferenceId, "foo"),
            queryIndices,
            List.of(new SearchResult(null, localIndexName, mixedTypeField2 + "_doc")),
            new ClusterFailure(
                SearchResponse.Cluster.Status.SKIPPED,
                Set.of(
                    new FailureCause(
                        QueryShardException.class,
                        "failed to create query: field [mixed-type-field-2] must be type [sparse_vector] but is type [semantic_text]"
                    )
                )
            ),
            s -> s.setCcsMinimizeRoundtrips(false)
        );

        // Validate that a CCS sparse vector query functions when only sparse vector fields are queried
        assertSearchResponse(
            new SparseVectorQueryBuilder(sparseVectorField, commonInferenceId, "foo"),
            queryIndices,
            List.of(
                new SearchResult(null, localIndexName, sparseVectorField + "_doc"),
                new SearchResult(REMOTE_CLUSTER, remoteIndexName, sparseVectorField + "_doc")
            ),
            null,
            s -> s.setCcsMinimizeRoundtrips(false)
        );
    }

    private void setupTwoClusters(TestIndexInfo localIndexInfo, TestIndexInfo remoteIndexInfo) throws IOException {
        setupCluster(LOCAL_CLUSTER, localIndexInfo);
        setupCluster(REMOTE_CLUSTER, remoteIndexInfo);
    }

    private void setupCluster(String clusterAlias, TestIndexInfo indexInfo) throws IOException {
        final Client client = client(clusterAlias);
        final String indexName = indexInfo.name();

        for (var entry : indexInfo.inferenceEndpoints().entrySet()) {
            String inferenceId = entry.getKey();
            MinimalServiceSettings minimalServiceSettings = entry.getValue();

            Map<String, Object> serviceSettings = new HashMap<>();
            serviceSettings.put("model", randomAlphaOfLength(5));
            serviceSettings.put("api_key", randomAlphaOfLength(5));
            if (minimalServiceSettings.taskType() == TaskType.TEXT_EMBEDDING) {
                serviceSettings.put("dimensions", minimalServiceSettings.dimensions());
                serviceSettings.put("similarity", minimalServiceSettings.similarity());
                serviceSettings.put("element_type", minimalServiceSettings.elementType());
            }

            createInferenceEndpoint(client, minimalServiceSettings.taskType(), inferenceId, serviceSettings);
        }

        Settings indexSettings = indexSettings(randomIntBetween(2, 5), randomIntBetween(0, 1)).build();
        assertAcked(client.admin().indices().prepareCreate(indexName).setSettings(indexSettings).setMapping(indexInfo.mappings()));
        assertFalse(
            client.admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, indexName)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );

        for (var entry : indexInfo.docs().entrySet()) {
            String docId = entry.getKey();
            Map<String, Object> doc = entry.getValue();

            DocWriteResponse response = client.prepareIndex(indexName).setId(docId).setSource(doc).execute().actionGet();
            assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        }
        BroadcastResponse refreshResponse = client.admin().indices().prepareRefresh(indexName).execute().actionGet();
        assertThat(refreshResponse.getStatus(), is(RestStatus.OK));
    }

    private BytesReference openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    private static void createInferenceEndpoint(Client client, TaskType taskType, String inferenceId, Map<String, Object> serviceSettings)
        throws IOException {
        final String service = switch (taskType) {
            case TEXT_EMBEDDING -> TestDenseInferenceServiceExtension.TestInferenceService.NAME;
            case SPARSE_EMBEDDING -> TestSparseInferenceServiceExtension.TestInferenceService.NAME;
            default -> throw new IllegalArgumentException("Unhandled task type [" + taskType + "]");
        };

        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", service);
            builder.field("service_settings", serviceSettings);
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        PutInferenceModelAction.Request request = new PutInferenceModelAction.Request(
            taskType,
            inferenceId,
            content,
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );
        var responseFuture = client.execute(PutInferenceModelAction.INSTANCE, request);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(), equalTo(inferenceId));
    }

    private void assertSearchResponse(QueryBuilder queryBuilder, String[] indices, List<SearchResult> expectedSearchResults)
        throws Exception {
        assertSearchResponse(queryBuilder, indices, expectedSearchResults, null, null);
    }

    private void assertSearchResponse(
        QueryBuilder queryBuilder,
        String[] indices,
        List<SearchResult> expectedSearchResults,
        ClusterFailure expectedRemoteFailure,
        Consumer<SearchRequest> searchRequestModifier
    ) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder).size(expectedSearchResults.size());
        SearchRequest searchRequest = new SearchRequest(indices, searchSourceBuilder);
        if (searchRequestModifier != null) {
            searchRequestModifier.accept(searchRequest);
        }

        assertResponse(client().search(searchRequest), response -> {
            SearchHit[] hits = response.getHits().getHits();
            assertThat(hits.length, equalTo(expectedSearchResults.size()));

            Iterator<SearchResult> searchResultIterator = expectedSearchResults.iterator();
            for (int i = 0; i < hits.length; i++) {
                SearchResult expectedSearchResult = searchResultIterator.next();
                SearchHit actualSearchResult = hits[i];

                assertThat(actualSearchResult.getClusterAlias(), equalTo(expectedSearchResult.clusterAlias()));
                assertThat(actualSearchResult.getIndex(), equalTo(expectedSearchResult.index()));
                assertThat(actualSearchResult.getId(), equalTo(expectedSearchResult.id()));
            }

            SearchResponse.Clusters clusters = response.getClusters();
            assertThat(clusters.getCluster(LOCAL_CLUSTER).getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(clusters.getCluster(LOCAL_CLUSTER).getFailures().isEmpty(), is(true));

            SearchResponse.Cluster remoteCluster = clusters.getCluster(REMOTE_CLUSTER);
            if (expectedRemoteFailure != null) {
                assertThat(remoteCluster.getStatus(), equalTo(expectedRemoteFailure.status()));

                Set<FailureCause> expectedFailures = expectedRemoteFailure.failures();
                Set<FailureCause> actualFailures = remoteCluster.getFailures()
                    .stream()
                    .map(f -> new FailureCause(f.getCause().getClass(), f.getCause().getMessage()))
                    .collect(Collectors.toSet());
                assertThat(actualFailures, equalTo(expectedFailures));
            } else {
                assertThat(remoteCluster.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getFailures().isEmpty(), is(true));
            }
        });
    }

    private static MinimalServiceSettings sparseEmbeddingServiceSettings() {
        return new MinimalServiceSettings(null, TaskType.SPARSE_EMBEDDING, null, null, null);
    }

    private static MinimalServiceSettings textEmbeddingServiceSettings(
        int dimensions,
        SimilarityMeasure similarity,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        return new MinimalServiceSettings(null, TaskType.TEXT_EMBEDDING, dimensions, similarity, elementType);
    }

    private static Map<String, Object> semanticTextMapping(String inferenceId) {
        return Map.of("type", SemanticTextFieldMapper.CONTENT_TYPE, "inference_id", inferenceId);
    }

    private static Map<String, Object> textMapping() {
        return Map.of("type", "text");
    }

    private static Map<String, Object> denseVectorMapping(int dimensions) {
        return Map.of("type", DenseVectorFieldMapper.CONTENT_TYPE, "dims", dimensions);
    }

    private static Map<String, Object> sparseVectorMapping() {
        return Map.of("type", SparseVectorFieldMapper.CONTENT_TYPE);
    }

    private static String fullyQualifiedIndexName(String clusterAlias, String indexName) {
        return clusterAlias + ":" + indexName;
    }

    private static float[] generateDenseVectorFieldValue(int dimensions, DenseVectorFieldMapper.ElementType elementType, float value) {
        if (elementType == DenseVectorFieldMapper.ElementType.BIT) {
            assert dimensions % 8 == 0;
            dimensions /= 8;
        }

        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            // Use a constant value so that relevance is consistent
            vector[i] = value;
        }

        return vector;
    }

    private static Map<String, Float> generateSparseVectorFieldValue(float weight) {
        // Generate values that have the same recall behavior as those produced by TestSparseInferenceServiceExtension. Use a constant token
        // weight so that relevance is consistent.
        return Map.of("feature_0", weight);
    }

    public static class FakeMlPlugin extends Plugin implements ActionPlugin, SearchPlugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return new MlInferenceNamedXContentProvider().getNamedWriteables();
        }

        @Override
        public List<QueryVectorBuilderSpec<?>> getQueryVectorBuilders() {
            return List.of(
                new QueryVectorBuilderSpec<>(
                    TextEmbeddingQueryVectorBuilder.NAME,
                    TextEmbeddingQueryVectorBuilder::new,
                    TextEmbeddingQueryVectorBuilder.PARSER
                )
            );
        }

        @Override
        public Collection<ActionHandler> getActions() {
            return List.of(new ActionHandler(CoordinatedInferenceAction.INSTANCE, TransportCoordinatedInferenceAction.class));
        }
    }

    private record TestIndexInfo(
        String name,
        Map<String, MinimalServiceSettings> inferenceEndpoints,
        Map<String, Object> mappings,
        Map<String, Map<String, Object>> docs
    ) {
        @Override
        public Map<String, Object> mappings() {
            return Map.of("properties", mappings);
        }
    }

    private record SearchResult(@Nullable String clusterAlias, String index, String id) {}

    private record FailureCause(Class<? extends Throwable> causeClass, String message) {}

    private record ClusterFailure(SearchResponse.Cluster.Status status, Set<FailureCause> failures) {}
}
