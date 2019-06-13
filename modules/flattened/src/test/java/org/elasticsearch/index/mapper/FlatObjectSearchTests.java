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

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugin.mapper.FlattenedMapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;

public class FlatObjectSearchTests extends ESSingleNodeTestCase {

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FlattenedMapperPlugin.class);
    }

    @Before
    public void setUpIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_doc")
                .startObject("properties")
                    .startObject("flat_object")
                        .field("type", "flattened")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                    .startObject("headers")
                        .field("type", "flattened")
                        .field("split_queries_on_whitespace", true)
                    .endObject()

                .endObject()
           .endObject()
        .endObject();
        createIndex("test", Settings.EMPTY, "_doc", mapping);
    }

    public void testMatchQuery() throws Exception {
        client().prepareIndex("test", "_doc", "1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("headers")
                        .field("content-type", "application/json")
                        .field("origin", "https://www.elastic.co")
                    .endObject()
            .endObject())
            .get();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchQuery("headers", "application/json"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(matchQuery("headers.content-type", "application/json text/plain"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(matchQuery("headers.origin", "application/json"))
            .get();
        assertHitCount(searchResponse, 0L);
    }

    public void testMultiMatchQuery() throws Exception {
        client().prepareIndex("test", "_doc", "1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("headers")
                        .field("content-type", "application/json")
                        .field("origin", "https://www.elastic.co")
                    .endObject()
            .endObject())
            .get();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(multiMatchQuery("application/json", "headers"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(multiMatchQuery("application/json text/plain", "headers.content-type"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(multiMatchQuery("application/json", "headers.origin"))
            .get();
        assertHitCount(searchResponse, 0L);
    }


    public void testQueryString() throws Exception {
        client().prepareIndex("test", "_doc", "1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("flat_object")
                        .field("field1", "value")
                        .field("field2", "2.718")
                    .endObject()
                .endObject())
            .get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(queryStringQuery("flat_object.field1:value"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 1);

        response = client().prepareSearch("test")
            .setQuery(queryStringQuery("flat_object.field1:value AND flat_object:2.718"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 1);

        response = client().prepareSearch("test")
            .setQuery(queryStringQuery("2.718").field("flat_object.field2"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 1);
    }

    public void testSimpleQueryString() throws Exception {
        client().prepareIndex("test", "_doc", "1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("flat_object")
                        .field("field1", "value")
                        .field("field2", "2.718")
                    .endObject()
                .endObject())
            .get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(simpleQueryStringQuery("value").field("flat_object.field1"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 1);

        response = client().prepareSearch("test")
            .setQuery(simpleQueryStringQuery("+value +2.718").field("flat_object"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 1);

        response = client().prepareSearch("test")
            .setQuery(simpleQueryStringQuery("+value +3.141").field("flat_object"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 0);
    }

    public void testExists() throws Exception {
        client().prepareIndex("test", "_doc", "1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("headers")
                        .field("content-type", "application/json")
                    .endObject()
                .endObject())
            .get();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(existsQuery("headers"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(existsQuery("headers.content-type"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(existsQuery("headers.nonexistent"))
            .get();
        assertHitCount(searchResponse, 0L);
    }

    public void testDocValuesFields() throws Exception {
        client().prepareIndex("test", "_doc", "1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("flat_object")
                        .field("key", "value")
                        .field("other_key", "other_value")
                    .endObject()
                .endObject())
            .get();

        SearchResponse response = client().prepareSearch("test")
            .addDocValueField("flat_object")
            .addDocValueField("flat_object.key")
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 1);

        Map<String, DocumentField> fields = response.getHits().getAt(0).getFields();

        DocumentField field = fields.get("flat_object");
        assertEquals("flat_object", field.getName());
        assertEquals(Arrays.asList("other_value", "value"), field.getValues());

        DocumentField keyedField = fields.get("flat_object.key");
        assertEquals("flat_object.key", keyedField.getName());
        assertEquals("value", keyedField.getValue());
     }


    public void testFieldSort() throws Exception {
        client().prepareIndex("test", "_doc", "1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("flat_object")
                    .field("key", "A")
                    .field("other_key", "D")
                .endObject()
            .endObject())
            .get();

        client().prepareIndex("test", "_doc", "2")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("flat_object")
                    .field("key", "B")
                    .field("other_key", "C")
                .endObject()
            .endObject())
            .get();

        client().prepareIndex("test", "_doc", "3")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("flat_object")
                    .field("other_key", "E")
                .endObject()
            .endObject())
            .get();

        SearchResponse response = client().prepareSearch("test")
            .addSort("flat_object", SortOrder.DESC)
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 3);
        assertOrderedSearchHits(response, "3", "1", "2");

        response = client().prepareSearch("test")
            .addSort("flat_object.key", SortOrder.DESC)
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 3);
        assertOrderedSearchHits(response, "2", "1", "3");

        response = client().prepareSearch("test")
            .addSort(new FieldSortBuilder("flat_object.key").order(SortOrder.DESC).missing("Z"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 3);
        assertOrderedSearchHits(response, "3", "2", "1");
    }

    public void testSourceFiltering() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("content-type", "application/json");
        headers.put("origin", "https://www.elastic.co");
        Map<String, Object> source = Collections.singletonMap("headers", headers);

        client().prepareIndex("test", "_doc", "1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(source)
            .get();

        SearchResponse response = client().prepareSearch("test").setFetchSource(true).get();
        assertThat(response.getHits().getAt(0).getSourceAsMap(), equalTo(source));

        // Check 'include' filtering.
        response = client().prepareSearch("test").setFetchSource("headers", null).get();
        assertThat(response.getHits().getAt(0).getSourceAsMap(), equalTo(source));

        response = client().prepareSearch("test").setFetchSource("headers.content-type", null).get();
        Map<String, Object> filteredSource = Collections.singletonMap("headers",
            Collections.singletonMap("content-type", "application/json"));
        assertThat(response.getHits().getAt(0).getSourceAsMap(), equalTo(filteredSource));

        // Check 'exclude' filtering.
        response = client().prepareSearch("test").setFetchSource(null, "headers.content-type").get();
        filteredSource = Collections.singletonMap("headers",
            Collections.singletonMap("origin", "https://www.elastic.co"));
        assertThat(response.getHits().getAt(0).getSourceAsMap(), equalTo(filteredSource));
    }


// TODO(jtibs): convert these tests brought over from MultiMatchQueryTests, CardinalityIT, and StringTermsIT
//    public void testFlatObjectSplitQueriesOnWhitespace() throws IOException {
//        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
//            .startObject("type")
//                .startObject("properties")
//                    .startObject("field")
//                        .field("type", "flattened")
//                    .endObject()
//                    .startObject("field_split")
//                        .field("type", "flattened")
//                        .field("split_queries_on_whitespace", true)
//                    .endObject()
//                .endObject()
//            .endObject().endObject());
//        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
//
//        QueryShardContext queryShardContext = indexService.newQueryShardContext(randomInt(20),
//            null, () -> { throw new UnsupportedOperationException(); }, null);
//        MultiMatchQuery parser = new MultiMatchQuery(queryShardContext);
//
//        Map<String, Float> fieldNames = new HashMap<>();
//        fieldNames.put("field", 1.0f);
//        fieldNames.put("field_split", 1.0f);
//        Query query = parser.parse(MultiMatchQueryBuilder.Type.BEST_FIELDS, fieldNames, "Foo Bar", null);
//
//        DisjunctionMaxQuery expected = new DisjunctionMaxQuery(Arrays.asList(
//            new TermQuery(new Term("field", "Foo Bar")),
//            new BooleanQuery.Builder()
//                .add(new TermQuery(new Term("field_split", "Foo")), BooleanClause.Occur.SHOULD)
//                .add(new TermQuery(new Term("field_split", "Bar")), BooleanClause.Occur.SHOULD)
//                .build()
//        ), 0.0f);
//        assertThat(query, equalTo(expected));
//    }
//
//      public void testFlatObjectField() {
//        SearchResponse response = client().prepareSearch("idx")
//            .addAggregation(cardinality("cardinality")
//                .precisionThreshold(precisionThreshold)
//                .field("flattened_values"))
//            .get();
//
//        assertSearchResponse(response);
//        Cardinality count = response.getAggregations().get("cardinality");
//        assertCount(count, numDocs);
//    }
//
//    public void testFlatObjectWithKey() {
//        SearchResponse firstResponse = client().prepareSearch("idx")
//            .addAggregation(cardinality("cardinality")
//                .precisionThreshold(precisionThreshold)
//                .field("flattened_values.first"))
//            .get();
//        assertSearchResponse(firstResponse);
//
//        Cardinality firstCount = firstResponse.getAggregations().get("cardinality");
//        assertCount(firstCount, numDocs);
//
//        SearchResponse secondResponse = client().prepareSearch("idx")
//            .addAggregation(cardinality("cardinality")
//                .precisionThreshold(precisionThreshold)
//                .field("flattened_values.second"))
//            .get();
//        assertSearchResponse(secondResponse);
//
//        Cardinality secondCount = secondResponse.getAggregations().get("cardinality");
//        assertCount(secondCount, (numDocs + 1) / 2);
//    }
//
//
//    public void testFlatObjectField() {
//        TermsAggregationBuilder builder = terms("terms")
//            .field(FLAT_OBJECT_FIELD_NAME)
//            .collectMode(randomFrom(SubAggCollectionMode.values()))
//            .executionHint(randomExecutionHint());
//
//        SearchResponse response = client().prepareSearch("idx")
//            .addAggregation(builder)
//            .get();
//        assertSearchResponse(response);
//
//        Terms terms = response.getAggregations().get("terms");
//        assertThat(terms, notNullValue());
//        assertThat(terms.getName(), equalTo("terms"));
//        assertThat(terms.getBuckets().size(), equalTo(6));
//
//        Bucket bucket1 = terms.getBuckets().get(0);
//        assertEquals("urgent", bucket1.getKey());
//        assertEquals(5, bucket1.getDocCount());
//
//        Bucket bucket2 = terms.getBuckets().get(1);
//        assertThat(bucket2.getKeyAsString(), startsWith("v1.2."));
//        assertEquals(1, bucket2.getDocCount());
//    }
//
//    public void testKeyedFlatObject() {
//        // Aggregate on the 'priority' subfield.
//        TermsAggregationBuilder priorityAgg = terms("terms")
//            .field(FLAT_OBJECT_FIELD_NAME + ".priority")
//            .collectMode(randomFrom(SubAggCollectionMode.values()))
//            .executionHint(randomExecutionHint());
//
//        SearchResponse priorityResponse = client().prepareSearch("idx")
//            .addAggregation(priorityAgg)
//            .get();
//        assertSearchResponse(priorityResponse);
//
//        Terms priorityTerms = priorityResponse.getAggregations().get("terms");
//        assertThat(priorityTerms, notNullValue());
//        assertThat(priorityTerms.getName(), equalTo("terms"));
//        assertThat(priorityTerms.getBuckets().size(), equalTo(1));
//
//        Bucket priorityBucket = priorityTerms.getBuckets().get(0);
//        assertEquals("urgent", priorityBucket.getKey());
//        assertEquals(5, priorityBucket.getDocCount());
//
//        // Aggregate on the 'release' subfield.
//        TermsAggregationBuilder releaseAgg = terms("terms")
//            .field(FLAT_OBJECT_FIELD_NAME + ".release")
//            .collectMode(randomFrom(SubAggCollectionMode.values()))
//            .executionHint(randomExecutionHint());
//
//        SearchResponse releaseResponse = client().prepareSearch("idx")
//            .addAggregation(releaseAgg)
//            .get();
//        assertSearchResponse(releaseResponse);
//
//        Terms releaseTerms = releaseResponse.getAggregations().get("terms");
//        assertThat(releaseTerms, notNullValue());
//        assertThat(releaseTerms.getName(), equalTo("terms"));
//        assertThat(releaseTerms.getBuckets().size(), equalTo(5));
//
//        for (Bucket bucket : releaseTerms.getBuckets()) {
//            assertThat(bucket.getKeyAsString(), startsWith("v1.2."));
//            assertEquals(1, bucket.getDocCount());
//        }
//    }
//
//    public void testKeyedFlatObjectWithMinDocCount() {
//        TermsAggregationBuilder priorityAgg = terms("terms")
//            .field(FLAT_OBJECT_FIELD_NAME + ".priority")
//            .collectMode(randomFrom(SubAggCollectionMode.values()))
//            .executionHint(randomExecutionHint())
//            .minDocCount(0);
//
//        SearchResponse priorityResponse = client().prepareSearch("idx")
//            .addAggregation(priorityAgg)
//            .get();
//        assertSearchResponse(priorityResponse);
//
//        Terms priorityTerms = priorityResponse.getAggregations().get("terms");
//        assertThat(priorityTerms, notNullValue());
//        assertThat(priorityTerms.getName(), equalTo("terms"));
//        assertThat(priorityTerms.getBuckets().size(), equalTo(1));
//    }
}
