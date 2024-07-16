/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.synonyms.PutSynonymsAction;
import org.elasticsearch.action.synonyms.SynonymUpdateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.synonyms.PutSynonymsAction.Request.SYNONYMS_SET_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SynonymsApiIT extends ESIntegTestCase {
    private static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.SECONDS);
    private static final String INDEX_ANALYZER_NAME = "standard_analyzer";
    private static final String SEARCH_ANALYZER_NAME = "synonym_analyzer";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(CommonAnalysisPlugin.class, MapperExtrasPlugin.class, ReindexPlugin.class);
    }

    public void testLoadIndexWithEmptySynonymsSet() throws Exception {
        final String indexName = "test-index";
        final String fieldName = "field";
        final String synonymsSetId = "test-synonyms-set";

        assertCreateSynonymsSet(createPutSynonymsRequest(synonymsSetId, List.of()));
        assertCreateIndexWithSynonyms(indexName, fieldName, synonymsSetId, null);
        ensureGreen(DEFAULT_TIMEOUT, indexName);
        assertAnalysis(DEFAULT_TIMEOUT, indicesAdmin().prepareAnalyze(indexName, "foo").setAnalyzer(SEARCH_ANALYZER_NAME), List.of("foo"));
    }

    public void testLoadIndexWithInvalidSynonymRule() throws Exception {
        final String indexName = "test-index";
        final String fieldName = "field";
        final String synonymsSetId = "test-synonyms-set";
        final List<String> stopwords = List.of("baz");

        assertCreateSynonymsSet(createPutSynonymsRequest(synonymsSetId, List.of(new SynonymRule(null, "foo => bar, baz"))));
        assertCreateIndexWithSynonyms(indexName, fieldName, synonymsSetId, stopwords);
        ensureGreen(DEFAULT_TIMEOUT, indexName);
        assertAnalysis(DEFAULT_TIMEOUT, indicesAdmin().prepareAnalyze(indexName, "foo").setAnalyzer(SEARCH_ANALYZER_NAME), List.of("bar"));
    }

    public void testReloadIndexWithInvalidSynonymRule() throws Exception {
        final String indexName = "test-index";
        final String fieldName = "field";
        final String synonymsSetId = "test-synonyms-set";
        final List<String> stopwords = List.of("baz");

        assertCreateSynonymsSet(createPutSynonymsRequest(synonymsSetId, List.of(new SynonymRule(null, "foo => bar"))));
        assertCreateIndexWithSynonyms(indexName, fieldName, synonymsSetId, stopwords);
        ensureGreen(DEFAULT_TIMEOUT, indexName);
        assertAnalysis(DEFAULT_TIMEOUT, indicesAdmin().prepareAnalyze(indexName, "foo").setAnalyzer(SEARCH_ANALYZER_NAME), List.of("bar"));

        assertUpdateSynonymsSet(createPutSynonymsRequest(synonymsSetId, List.of(new SynonymRule(null, "foo => bar, baz"))));
        ensureGreen(DEFAULT_TIMEOUT, indexName);
        assertAnalysis(DEFAULT_TIMEOUT, indicesAdmin().prepareAnalyze(indexName, "foo").setAnalyzer(SEARCH_ANALYZER_NAME), List.of("bar"));

        reloadIndex(DEFAULT_TIMEOUT, indexName);
        ensureGreen(DEFAULT_TIMEOUT, indexName);
        assertAnalysis(DEFAULT_TIMEOUT, indicesAdmin().prepareAnalyze(indexName, "foo").setAnalyzer(SEARCH_ANALYZER_NAME), List.of("bar"));
    }

    private void assertCreateIndexWithSynonyms(String indexName, String fieldName, String synonymsSetId, List<String> stopwords) {
        Settings.Builder builder = Settings.builder()
            .put(indexSettings())
            .put("index.analysis.analyzer." + INDEX_ANALYZER_NAME + ".type", "standard")
            .put("index.analysis.analyzer." + SEARCH_ANALYZER_NAME + ".tokenizer", "standard")
            .putList("index.analysis.analyzer." + SEARCH_ANALYZER_NAME + ".filter", "lowercase", "synonym")
            .put("index.analysis.filter.synonym.type", "synonym_graph")
            .put("index.analysis.filter.synonym.synonyms_set", synonymsSetId)
            .put("index.analysis.filter.synonym.updateable", true);

        if (stopwords != null && stopwords.isEmpty() == false) {
            builder.putList("index.analysis.analyzer." + INDEX_ANALYZER_NAME + ".stopwords", stopwords)
                .put("index.analysis.filter.stopwords.type", "stop")
                .putList("index.analysis.filter.stopwords.stopwords", stopwords)
                .putList("index.analysis.analyzer." + SEARCH_ANALYZER_NAME + ".filter", "lowercase", "stopwords", "synonym");
        }

        assertAcked(
            prepareCreate(indexName).setSettings(builder)
                .setMapping(fieldName, "type=text,analyzer=" + INDEX_ANALYZER_NAME + ",search_analyzer=" + SEARCH_ANALYZER_NAME)
        );
    }

    private static PutSynonymsAction.Request createPutSynonymsRequest(String synonymsSetId, List<SynonymRule> synonymRules)
        throws IOException {
        BytesReference content;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startArray(SYNONYMS_SET_FIELD.getPreferredName());
            for (SynonymRule rule : synonymRules) {
                rule.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endArray();
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        return new PutSynonymsAction.Request(synonymsSetId, content, XContentType.JSON);
    }

    private static void assertCreateSynonymsSet(PutSynonymsAction.Request request) {
        ActionFuture<SynonymUpdateResponse> response = client().execute(PutSynonymsAction.INSTANCE, request);
        assertThat(response.actionGet().status(), is(RestStatus.CREATED));
    }

    private static void assertUpdateSynonymsSet(PutSynonymsAction.Request request) {
        ActionFuture<SynonymUpdateResponse> response = client().execute(PutSynonymsAction.INSTANCE, request);
        assertThat(response.actionGet().status(), is(RestStatus.OK));
    }

    private static void assertAnalysis(TimeValue timeout, AnalyzeRequestBuilder analyzeRequest, List<String> expectedTokens) {
        AnalyzeAction.Response response = analyzeRequest.get(timeout);
        assertThat(expectedTokens, equalTo(response.getTokens().stream().map(AnalyzeAction.AnalyzeToken::getTerm).toList()));
    }

    private static void reloadIndex(TimeValue timeout, String indexName) {
        assertAcked(indicesAdmin().close(new CloseIndexRequest(indexName)).actionGet(timeout));
        assertAcked(indicesAdmin().open(new OpenIndexRequest(indexName)).actionGet(timeout));
    }
}
