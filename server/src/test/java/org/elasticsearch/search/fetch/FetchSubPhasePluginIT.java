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

package org.elasticsearch.search.fetch;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 2)
public class FetchSubPhasePluginIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(FetchTermVectorsPlugin.class);
    }

    @SuppressWarnings("unchecked")
    public void testPlugin() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .setMapping(
                        jsonBuilder()
                                .startObject().startObject("_doc")
                                .startObject("properties")
                                .startObject("test")
                                .field("type", "text").field("term_vector", "yes")
                                .endObject()
                                .endObject()
                                .endObject().endObject()).get();

        client().index(
                indexRequest("test").id("1")
                        .source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())).actionGet();

        client().admin().indices().prepareRefresh().get();

         SearchResponse response = client().prepareSearch().setSource(new SearchSourceBuilder()
                 .ext(Collections.singletonList(new TermVectorsFetchBuilder("test")))).get();
        assertSearchResponse(response);
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("i"),
                equalTo(2));
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("am"),
                equalTo(2));
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("sam"),
                equalTo(1));
    }

    public static class FetchTermVectorsPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
            return singletonList(new TermVectorsFetchSubPhase());
        }

        @Override
        public List<SearchExtSpec<?>> getSearchExts() {
            return Collections.singletonList(new SearchExtSpec<>(TermVectorsFetchSubPhase.NAME,
                    TermVectorsFetchBuilder::new, TermVectorsFetchBuilder::fromXContent));
        }
    }

    private static final class TermVectorsFetchSubPhase implements FetchSubPhase {
        private static final String NAME = "term_vectors_fetch";

        @Override
        public void hitExecute(SearchContext context, HitContext hitContext) {
            TermVectorsFetchBuilder fetchSubPhaseBuilder = (TermVectorsFetchBuilder)context.getSearchExt(NAME);
            if (fetchSubPhaseBuilder == null) {
                return;
            }
            String field = fetchSubPhaseBuilder.getField();
            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<>());
            }
            DocumentField hitField = hitContext.hit().getFields().get(NAME);
            if (hitField == null) {
                hitField = new DocumentField(NAME, new ArrayList<>(1));
                hitContext.hit().getFields().put(NAME, hitField);
            }
            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(context.indexShard().shardId().getIndex().getName(),
                    hitContext.hit().getId());
            TermVectorsResponse termVector = TermVectorsService.getTermVectors(context.indexShard(), termVectorsRequest);
            try {
                Map<String, Integer> tv = new HashMap<>();
                TermsEnum terms = termVector.getFields().terms(field).iterator();
                BytesRef term;
                while ((term = terms.next()) != null) {
                    tv.put(term.utf8ToString(), terms.postings(null, PostingsEnum.ALL).freq());
                }
                hitField.getValues().add(tv);
            } catch (IOException e) {
                LogManager.getLogger(FetchSubPhasePluginIT.class).info("Swallowed exception", e);
            }
        }
    }

    private static final class TermVectorsFetchBuilder extends SearchExtBuilder {
        public static TermVectorsFetchBuilder fromXContent(XContentParser parser) throws IOException {
            String field;
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                field = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected a VALUE_STRING but got " + token);
            }
            if (field == null) {
                throw new ParsingException(parser.getTokenLocation(), "no fields specified for " + TermVectorsFetchSubPhase.NAME);
            }
            return new TermVectorsFetchBuilder(field);
        }

        private final String field;

        private TermVectorsFetchBuilder(String field) {
            this.field = field;
        }

        private TermVectorsFetchBuilder(StreamInput in) throws IOException {
            this.field = in.readString();
        }

        private String getField() {
            return field;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TermVectorsFetchBuilder that = (TermVectorsFetchBuilder) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String getWriteableName() {
            return TermVectorsFetchSubPhase.NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(field);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(TermVectorsFetchSubPhase.NAME, field);
        }
    }
}
