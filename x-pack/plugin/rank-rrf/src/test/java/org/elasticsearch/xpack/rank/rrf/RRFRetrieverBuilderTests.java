/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;

/** Tests for the rrf retriever. */
public class RRFRetrieverBuilderTests extends ESTestCase {

    /** Tests extraction errors related to compound retrievers. These tests require a compound retriever which is why they are here. */
    public void testRetrieverExtractionErrors() throws IOException {
        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"retriever\":{\"rrf_nl\":{\"retrievers\":"
                    + "[{\"standard\":{\"search_after\":[1]}},{\"standard\":{\"search_after\":[2]}}]}}}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> ssb.parseXContent(parser, true, nf -> true)
                    .rewrite(
                        new QueryRewriteContext(parserConfig(), null, null, null, new PointInTimeBuilder(new BytesArray("pitid")), null)
                    )
            );
            assertEquals("[search_after] cannot be used in children of compound retrievers", iae.getMessage());
        }

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"retriever\":{\"rrf_nl\":{\"retrievers\":"
                    + "[{\"standard\":{\"terminate_after\":1}},{\"standard\":{\"terminate_after\":2}}]}}}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> ssb.parseXContent(parser, true, nf -> true)
                    .rewrite(
                        new QueryRewriteContext(parserConfig(), null, null, null, new PointInTimeBuilder(new BytesArray("pitid")), null)
                    )
            );
            assertEquals("[terminate_after] cannot be used in children of compound retrievers", iae.getMessage());
        }
    }

    public void testMultiFieldsParamsRewrite() {
        final String indexName = "test-index";
        final List<String> testInferenceFields = List.of("semantic_field_1", "semantic_field_2");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(indexName, testInferenceFields, null);
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null
        );

        // No wildcards
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "foo"
        );

        // Non-default rank window size and rank constant
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_1", "field_2", "semantic_field_1", "semantic_field_2"),
            "foo2",
            DEFAULT_RANK_WINDOW_SIZE * 2,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT / 2
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_1", 1.0f, "field_2", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "foo2"
        );

        // Glob matching on inference and non-inference fields
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("field_*", "*_field_1"),
            "bar",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("field_*", 1.0f, "*_field_1", 1.0f),
            Map.of("semantic_field_1", 1.0f),
            "bar"
        );

        // All-fields wildcard
        rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            List.of("*"),
            "baz",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT
        );
        assertMultiFieldsParamsRewrite(
            rrfRetrieverBuilder,
            queryRewriteContext,
            Map.of("*", 1.0f),
            Map.of("semantic_field_1", 1.0f, "semantic_field_2", 1.0f),
            "baz"
        );
    }

    public void testSearchRemoteIndex() {
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(
            "local-index",
            List.of(),
            Map.of("remote-cluster", "remote-index")
        );
        final QueryRewriteContext queryRewriteContext = new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null
        );

        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder(
            null,
            null,
            "foo",
            DEFAULT_RANK_WINDOW_SIZE,
            RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT
        );

        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> rrfRetrieverBuilder.doRewrite(queryRewriteContext)
        );
        assertEquals("[rrf] cannot specify [query] when querying remote indices", iae.getMessage());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new SearchModule(Settings.EMPTY, List.of()).getNamedXContents();
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                new ParseField(RRFRetrieverBuilder.NAME),
                (p, c) -> RRFRetrieverBuilder.fromXContent(p, (RetrieverParserContext) c)
            )
        );
        // Add an entry with no license requirement for unit testing
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                new ParseField(RRFRetrieverBuilder.NAME + "_nl"),
                (p, c) -> RRFRetrieverBuilder.PARSER.apply(p, (RetrieverParserContext) c)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    private static ResolvedIndices createMockResolvedIndices(
        String localIndexName,
        List<String> inferenceFields,
        Map<String, String> remoteIndexNames
    ) {
        Index index = new Index(localIndexName, randomAlphaOfLength(10));
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            )
            .numberOfShards(1)
            .numberOfReplicas(0);

        for (String inferenceField : inferenceFields) {
            indexMetadataBuilder.putInferenceField(
                new InferenceFieldMetadata(inferenceField, randomAlphaOfLengthBetween(3, 5), new String[] { inferenceField }, null)
            );
        }

        Map<String, OriginalIndices> remoteIndices = new HashMap<>();
        if (remoteIndexNames != null) {
            for (Map.Entry<String, String> entry : remoteIndexNames.entrySet()) {
                remoteIndices.put(entry.getKey(), new OriginalIndices(new String[] { entry.getValue() }, IndicesOptions.DEFAULT));
            }
        }

        return new MockResolvedIndices(
            remoteIndices,
            new OriginalIndices(new String[] { localIndexName }, IndicesOptions.DEFAULT),
            Map.of(index, indexMetadataBuilder.build())
        );
    }

    private static void assertMultiFieldsParamsRewrite(
        RRFRetrieverBuilder retriever,
        QueryRewriteContext ctx,
        Map<String, Float> expectedNonInferenceFields,
        Map<String, Float> expectedInferenceFields,
        String expectedQuery
    ) {
        Set<Object> expectedInnerRetrievers = Set.of(
            CompoundRetrieverBuilder.RetrieverSource.from(
                new StandardRetrieverBuilder(
                    new MultiMatchQueryBuilder(expectedQuery).type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
                        .fields(expectedNonInferenceFields)
                )
            ),
            Set.of(expectedInferenceFields.entrySet().stream().map(e -> {
                if (e.getValue() != 1.0f) {
                    throw new IllegalArgumentException("Cannot apply per-field weights in RRF");
                }
                return CompoundRetrieverBuilder.RetrieverSource.from(
                    new StandardRetrieverBuilder(new MatchQueryBuilder(e.getKey(), expectedQuery))
                );
            }).toArray())
        );

        RetrieverBuilder rewritten = retriever.doRewrite(ctx);
        assertNotSame(retriever, rewritten);
        assertTrue(rewritten instanceof RRFRetrieverBuilder);

        RRFRetrieverBuilder rewrittenRrf = (RRFRetrieverBuilder) rewritten;
        assertEquals(retriever.rankWindowSize(), rewrittenRrf.rankWindowSize());
        assertEquals(retriever.rankConstant(), rewrittenRrf.rankConstant());
        assertEquals(expectedInnerRetrievers, getInnerRetrieversAsSet(rewrittenRrf));
    }

    private static Set<Object> getInnerRetrieversAsSet(RRFRetrieverBuilder retriever) {
        Set<Object> innerRetrieversSet = new HashSet<>();
        for (CompoundRetrieverBuilder.RetrieverSource innerRetriever : retriever.innerRetrievers()) {
            if (innerRetriever.retriever() instanceof RRFRetrieverBuilder innerRrfRetriever) {
                assertEquals(retriever.rankWindowSize(), innerRrfRetriever.rankWindowSize());
                assertEquals(retriever.rankConstant(), innerRrfRetriever.rankConstant());
                innerRetrieversSet.add(getInnerRetrieversAsSet(innerRrfRetriever));
            } else {
                innerRetrieversSet.add(innerRetriever);
            }
        }

        return innerRetrieversSet;
    }
}
