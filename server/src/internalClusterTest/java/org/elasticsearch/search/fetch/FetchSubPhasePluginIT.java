/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
        public FetchSubPhaseProcessor getProcessor(FetchContext searchContext) {
            return new FetchSubPhaseProcessor() {
                @Override
                public void setNextReader(LeafReaderContext readerContext) {

                }

                @Override
                public void process(HitContext hitContext) throws IOException {
                    hitExecute(searchContext, hitContext);
                }
            };
        }

        private void hitExecute(FetchContext context, HitContext hitContext) throws IOException {
            TermVectorsFetchBuilder fetchSubPhaseBuilder = (TermVectorsFetchBuilder)context.getSearchExt(NAME);
            if (fetchSubPhaseBuilder == null) {
                return;
            }
            String field = fetchSubPhaseBuilder.getField();
            DocumentField hitField = hitContext.hit().getFields().get(NAME);
            if (hitField == null) {
                hitField = new DocumentField(NAME, new ArrayList<>(1));
                hitContext.hit().setDocumentField(NAME, hitField);
            }
            Terms terms = hitContext.reader().getTermVector(hitContext.docId(), field);
            if (terms != null) {
                TermsEnum te = terms.iterator();
                Map<String, Integer> tv = new HashMap<>();
                BytesRef term;
                PostingsEnum pe = null;
                while ((term = te.next()) != null) {
                    pe = te.postings(pe, PostingsEnum.FREQS);
                    pe.nextDoc();
                    tv.put(term.utf8ToString(), pe.freq());
                }
                hitField.getValues().add(tv);
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
