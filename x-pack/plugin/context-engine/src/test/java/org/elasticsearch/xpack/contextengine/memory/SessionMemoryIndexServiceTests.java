/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.contextengine.memory.SessionMemoryIndexService.RerankInputs;
import org.elasticsearch.xpack.contextengine.memory.action.SearchSessionMemoryAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults.RankedDoc;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SessionMemoryIndexServiceTests extends ESTestCase {

    private static final String IMAGE = "data:image/png;base64,iVBORw0KGgo=";
    private static final String PDF = "data:application/pdf;base64,JVBERi0=";

    public void testInferenceStringFromQuery() {
        assertThat(SessionMemoryIndexService.inferenceString("dark mode"), equalTo(InferenceString.ofText("dark mode")));
        assertThat(
            SessionMemoryIndexService.inferenceString(Map.of("type", "image", "value", IMAGE)),
            equalTo(new InferenceString(DataType.IMAGE, DataFormat.BASE64, IMAGE))
        );
        assertThat(
            SessionMemoryIndexService.inferenceString(Map.of("type", "pdf", "format", "url", "value", "https://example.com/a.pdf")),
            equalTo(new InferenceString(DataType.PDF, DataFormat.URL, "https://example.com/a.pdf"))
        );
    }

    public void testRerankInputsOneDocumentPerContentAndImageSkippingPdf() {
        RerankInputs inputs = RerankInputs.of(
            List.of(
                memory("text", "The user prefers dark mode.", List.of()),
                memory("both", "Hero shot.", List.of(Map.of("type", "image", "value", IMAGE))),
                memory("media", null, List.of(Map.of("type", "image", "value", IMAGE))),
                memory("pdf", null, List.of(Map.of("type", "pdf", "value", PDF)))
            )
        );

        assertThat(
            inputs.documents(),
            contains(
                InferenceString.ofText("The user prefers dark mode."),
                InferenceString.ofText("Hero shot."),
                new InferenceString(DataType.IMAGE, IMAGE),
                new InferenceString(DataType.IMAGE, IMAGE)
            )
        );
        assertThat(inputs.owners(), contains(0, 1, 1, 2));
    }

    public void testRankUsesBestDocumentPerMemoryAndTruncates() {
        RerankInputs inputs = RerankInputs.of(
            List.of(
                memory("text", "The user prefers dark mode.", List.of()),
                memory("both", "Hero shot.", List.of(Map.of("type", "image", "value", IMAGE))),
                memory("media", null, List.of(Map.of("type", "image", "value", IMAGE)))
            )
        );
        List<RankedDoc> rankedDocs = List.of(
            new RankedDoc(0, 0.4f, null),
            new RankedDoc(1, 0.2f, null),
            new RankedDoc(2, 0.9f, null),
            new RankedDoc(3, 0.5f, null)
        );

        List<SearchSessionMemoryAction.Hit> hits = inputs.rank(rankedDocs, 10);
        assertThat(hits.stream().map(hit -> hit.memory().id()).toList(), contains("both", "media", "text"));
        assertThat(hits.get(0).score(), equalTo(0.9f));

        assertThat(inputs.rank(rankedDocs, 2).stream().map(hit -> hit.memory().id()).toList(), contains("both", "media"));
    }

    public void testRankPutsUnscoredMemoriesLastInFirstStageOrder() {
        RerankInputs inputs = RerankInputs.of(
            List.of(
                memory("pdf-first", null, List.of(Map.of("type", "pdf", "value", PDF))),
                memory("text", "The user prefers dark mode.", List.of()),
                memory("pdf-second", null, List.of(Map.of("type", "pdf", "value", PDF))),
                memory("other", "Something else.", List.of())
            )
        );

        List<RankedDoc> rankedDocs = List.of(new RankedDoc(0, 0.2f, null), new RankedDoc(1, 0.3f, null));

        List<SearchSessionMemoryAction.Hit> hits = inputs.rank(rankedDocs, 10);
        assertThat(hits.stream().map(hit -> hit.memory().id()).toList(), contains("other", "text", "pdf-first", "pdf-second"));
        assertThat(hits.get(2).score(), equalTo(0f));
        assertThat(inputs.rank(rankedDocs, 3).stream().map(hit -> hit.memory().id()).toList(), contains("other", "text", "pdf-first"));
    }

    public void testRetrieverSourceFusesOneKnnQueryPerFieldWithFilters() throws IOException {
        String json = SessionMemoryIndexService.retrieverSource(
            "dark mode",
            "test-embedding",
            20,
            List.of(QueryBuilders.termQuery("user_id", "user-1"))
        );

        Map<String, Object> source = parse(json);
        assertThat(source.get("size"), equalTo(20));
        Map<?, ?> rrf = (Map<?, ?>) ((Map<?, ?>) source.get("retriever")).get("rrf");
        assertThat(rrf.get("rank_window_size"), equalTo(20));
        List<?> retrievers = (List<?>) rrf.get("retrievers");
        assertThat(retrievers, hasSize(2));
        Map<?, ?> content = knnQuery(retrievers.get(0));
        Map<?, ?> media = knnQuery(retrievers.get(1));
        assertThat(content.get("field"), equalTo("content.semantic"));
        assertThat(media.get("field"), equalTo("media"));
        assertThat(content.get("k"), equalTo(20));
        assertThat(((Map<?, ?>) ((Map<?, ?>) content.get("query_vector_builder")).get("embedding")).get("input"), equalTo("dark mode"));
        assertThat(((List<?>) content.get("filter")), hasSize(1));
        assertThat(media.get("filter"), equalTo(content.get("filter")));
    }

    private static SessionMemory memory(String id, String content, List<Map<String, Object>> media) {
        return new SessionMemory(null, id, null, null, "session-1", "user-1", "agent-1", null, content, media, null);
    }

    private static Map<?, ?> knnQuery(Object retriever) {
        Map<?, ?> standard = (Map<?, ?>) ((Map<?, ?>) retriever).get("standard");
        return (Map<?, ?>) ((Map<?, ?>) standard.get("query")).get("knn");
    }

    private static Map<String, Object> parse(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }
}
