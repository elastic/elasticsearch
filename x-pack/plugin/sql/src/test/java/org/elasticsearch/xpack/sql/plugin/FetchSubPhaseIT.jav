/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSubPhase;
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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class FetchSubPhaseIT extends ESIntegTestCase {

    public static class MyPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
            return singletonList(new SqlFieldsFetchSubPhase());
        }

        @Override
        public List<SearchExtSpec<?>> getSearchExts() {
            return Collections.singletonList(
                    new SearchExtSpec<>(SqlFieldsFetchSubPhase.NAME, SqlFieldsFetchBuilder::new, SqlFieldsFetchBuilder::fromXContent));
        }
    }


    private static final class SqlFieldsFetchSubPhase implements FetchSubPhase {
        private static final String NAME = "_sql";

        @Override
        public void hitExecute(SearchContext context, HitContext hitContext) {
            SqlFieldsFetchBuilder fetchSubPhaseBuilder = (SqlFieldsFetchBuilder)context.getSearchExt(NAME);
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

            // TODO: Check that there's no source filtering

            Map<String, Object> sourceAsMap = hitContext.hit().getSourceAsMap();
            // remove the source from the response
            hitContext.hit().sourceRef(null);

            Map<String, Integer> tv = new HashMap<>();
            MappedFieldType keyword = context.mapperService().fullName("keyword");
            MappedFieldType date = context.mapperService().fullName("date");
            MappedFieldType scaledFloat = context.mapperService().fullName("scaled_float");
            MappedFieldType text = context.mapperService().fullName("text");

            hitField.getValues().add(tv);
        }
    }

    private static final class SqlFieldsFetchBuilder extends SearchExtBuilder {
        public static SqlFieldsFetchBuilder fromXContent(XContentParser parser) throws IOException {
            String field;
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                field = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected a VALUE_STRING but got " + token);
            }
            if (field == null) {
                throw new ParsingException(parser.getTokenLocation(), "no fields specified for " + SqlFieldsFetchSubPhase.NAME);
            }
            return new SqlFieldsFetchBuilder(field);
        }

        private final String field;

        private SqlFieldsFetchBuilder(String field) {
            this.field = field;
        }

        private SqlFieldsFetchBuilder(StreamInput in) throws IOException {
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
            SqlFieldsFetchBuilder that = (SqlFieldsFetchBuilder) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String getWriteableName() {
            return SqlFieldsFetchSubPhase.NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(field);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(SqlFieldsFetchSubPhase.NAME, field);
        }
    }

    
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return asList(MapperExtrasPlugin.class, MyPlugin.class);
    }

    public void testPlugin() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("doc", jsonBuilder().startObject().startObject("properties")
                            .startObject("long").field("type", "long").endObject()
                            .startObject("keyword").field("type", "keyword").field("ignore_above", "10").endObject()
                            .startObject("text").field("type", "text").endObject()
                            .startObject("scaled_float").field("type", "scaled_float").field("scaling_factor", 100).endObject()
                            .startObject("date").field("type", "date").endObject()
                        .endObject().endObject())
                .get();

        client().prepareBulk("test", "doc")
            .add(indexRequest().id("1").source(jsonBuilder().startObject()
                    .field("long", 123l)
                    .field("keyword", "the one")
                    .field("text", "the one")
                    .field("scaled_float", 123.456f)
                        .field("date", "2019-08-05")
                    .endObject()))
            .get();


        client().admin().indices().prepareRefresh().get();

        SearchResponse response = client().prepareSearch()
                .setSource(new SearchSourceBuilder().ext(Collections.singletonList(new SqlFieldsFetchBuilder("scaled_float")))).get();
        assertSearchResponse(response);
        SearchHit hit = response.getHits().getAt(0);
        System.out.println(hit);
    }
}
