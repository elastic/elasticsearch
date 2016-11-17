/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SearchPlugin.RescoreSpec;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singletonList;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class RescorePluginIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomRescorePlugin.class);
    }

    public void testRescorePlugin() throws IOException {
        client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1)
                .addMapping(
                        "post",
                        jsonBuilder()
                        .startObject().startObject("post").startObject("properties")
                        .startObject("title")
                                .field("type", "text")
                        .endObject()
                        .endObject().endObject().endObject())
                .execute()
                .actionGet();
        client().admin().cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForYellowStatus()
                .execute()
                .actionGet();

        client().index(indexRequest("test").type("post").id("1").source(
                        jsonBuilder()
                        .startObject()
                                .field("title", "The quick brown")
                        .endObject()))
                .actionGet();
        client().index(indexRequest("test").type("post").id("2").source(
                        jsonBuilder()
                        .startObject()
                                .field("title", "quick brown fox")
                        .endObject()))
                .actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        ActionFuture<SearchResponse> response;
        SearchResponse resp;
        SearchHits hits;

        response = client().search(
                searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                        searchSource()
                        .addRescorer(new DummyRescorerBuilder(4.0f))
                        .explain(false)));

        resp = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(resp);
        hits = resp.getHits();

        assertThat(hits.getHits().length, equalTo(2));
        assertThat(hits.getMaxScore(), equalTo(4.0f));
        assertThat(hits.getAt(0).getScore(), equalTo(4.0f));
        assertThat(hits.getAt(1).getScore(), equalTo(4.0f));

        response = client().search(
                searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                        searchSource()
                        .addRescorer(new DummyRescorerBuilder(4.0f)
                                .windowSize(1))
                        .explain(true)));

        resp = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(resp);
        hits = resp.getHits();

        assertThat(hits.getHits().length, equalTo(2));
        assertThat(hits.getMaxScore(), equalTo(4.0f));
        SearchHit hit = hits.getAt(0);
        logger.info("Hit[0]: {}, Score: {},\nExplanation: {}",
                    hit.getId(), hit.getScore(), hit.getExplanation());
        assertThat(hit.getScore(), equalTo(4.0f));
        assertThat(hit.getExplanation().getDetails()[1].toString(), equalTo("4.0 = weight\n"));
        hit = hits.getAt(1);
        logger.info("Hit[1]: {}, Score: {},\nExplanation: {}",
                    hit.getId(), hit.getScore(), hit.getExplanation());
        assertThat(hits.getAt(1).getScore(), equalTo(1.0f));
        // rescore explanation considers top hits behind window size were rescored
        assertThat(hit.getExplanation().getDetails()[1].toString(), equalTo("4.0 = weight\n"));
    }

    public static class CustomRescorePlugin extends Plugin implements SearchPlugin {
        @Override
        public List<RescoreSpec<?>> getRescorers() {
            return singletonList(new RescoreSpec(
                    DummyRescorerBuilder.NAME,
                    DummyRescorerBuilder::new,
                    DummyRescorerBuilder.PARSER));
        }
    }

    public static class DummyRescorerBuilder extends RescoreBuilder<DummyRescorerBuilder> {
        public static final String NAME = "dummy";
        public static final RescoreParser<DummyRescorerBuilder> PARSER = new DummyRescorerParser(
                DummyRescorerBuilder::new);

        public static final float DEFAULT_WEIGHT = 1.0f;
        private float weight = DEFAULT_WEIGHT;

        public DummyRescorerBuilder(float weight) {
            this.weight = weight;
        }

        public DummyRescorerBuilder(StreamInput in) throws IOException {
            super(in);
            weight = in.readFloat();
        }

        @Override
        public void doWriteTo(StreamOutput out) throws IOException {
            out.writeFloat(weight);
        }

        public DummyRescorerBuilder setWeight(float weight) {
            this.weight = weight;
            return this;
        }

        public float getWeight() {
            return this.weight;
        }

        @Override
        public void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.field("weight", weight);
            builder.endObject();
        }

        @Override
        public RescoreSearchContext build(QueryShardContext context) throws IOException {
            DummyRescoreContext dummyRescoreCtx = new DummyRescoreContext(DummyRescorer.INSTANCE);
            dummyRescoreCtx.setWeight(weight);
            if (windowSize != null) {
                dummyRescoreCtx.setWindowSize(windowSize);
            }
            return dummyRescoreCtx;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public static final class DummyRescorer implements Rescorer {
        public static final String NAME = "dummy";
        public static final DummyRescorer INSTANCE = new DummyRescorer();

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public TopDocs rescore(TopDocs topDocs,
                               SearchContext context,
                               RescoreSearchContext rescoreContext)
            throws IOException
        {
            assert rescoreContext != null;
            if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
                return topDocs;
            }

            DummyRescoreContext rescoreCtx = (DummyRescoreContext) rescoreContext;
            int windowSize = Math.min(rescoreCtx.window(), topDocs.scoreDocs.length);
            for (int i = 0; i < windowSize; i++) {
                // Just multiply score by weight
                topDocs.scoreDocs[i].score *= rescoreCtx.weight;
            }

            topDocs.setMaxScore(topDocs.scoreDocs[0].score);
            return topDocs;
        }

        @Override
        public Explanation explain(int topLevelDocId,
                                   SearchContext context,
                                   RescoreSearchContext rescoreContext,
                                   Explanation sourceExplanation)
            throws IOException
        {
            DummyRescoreContext rescore = (DummyRescoreContext) rescoreContext;

            if (sourceExplanation.isMatch()) {
                return Explanation.match(
                        sourceExplanation.getValue() * rescore.weight(),
                        "product of:",
                        sourceExplanation,
                        Explanation.match(rescore.weight, "weight")
                );
            } else {
                return sourceExplanation;
            }
        }

        @Override
        public void extractTerms(SearchContext context,
                                 RescoreSearchContext rescoreContext,
                                 Set<Term> termsSet) {
        }
    }

    public static class DummyRescoreContext extends RescoreSearchContext {

        static final int DEFAULT_WINDOW_SIZE = 10;

        public DummyRescoreContext(DummyRescorer rescorer) {
            super(DummyRescorerBuilder.NAME, DEFAULT_WINDOW_SIZE, rescorer);
        }

        private float weight = 1.0f;

        public float weight() {
            return weight;
        }

        public void setWeight(float weight) {
            this.weight = weight;
        }
    }

    public static class DummyRescorerParser implements RescoreParser<DummyRescorerBuilder> {
        private static final ParseField WEIGHT_FIELD = new ParseField("weight");

        private final Function<Float, DummyRescorerBuilder> builder;

        public DummyRescorerParser(Function<Float, DummyRescorerBuilder> builder) {
            this.builder = builder;
        }

        @Override
        public DummyRescorerBuilder fromXContent(QueryParseContext parseContext)
            throws IOException, ParsingException
        {
            Float weight = null;
            String fieldName = null;
            XContentParser parser = parseContext.parser();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (WEIGHT_FIELD.match(fieldName)) {
                        weight = parser.floatValue();
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "dummy rescorer doesn't support [" + fieldName + "]");
                    }
                }
            }

            if (weight == null) {
                throw new ParsingException(
                    parser.getTokenLocation(), "dummy rescorer requires [weight] field");
            }

            return builder.apply(weight);
        }
    }
}
