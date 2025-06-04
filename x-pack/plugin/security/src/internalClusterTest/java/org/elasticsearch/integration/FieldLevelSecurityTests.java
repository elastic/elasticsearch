/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.percolator.PercolateQueryBuilder;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class FieldLevelSecurityTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            LocalStateSecurity.class,
            CommonAnalysisPlugin.class,
            ParentJoinPlugin.class,
            InternalSettingsPlugin.class,
            PercolatorPlugin.class,
            SpatialPlugin.class,
            MapperExtrasPlugin.class,
            Wildcard.class
        );
    }

    @Override
    protected String configUsers() {
        final String usersPasswHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers()
            + "user1:"
            + usersPasswHashed
            + "\n"
            + "user2:"
            + usersPasswHashed
            + "\n"
            + "user3:"
            + usersPasswHashed
            + "\n"
            + "user4:"
            + usersPasswHashed
            + "\n"
            + "user5:"
            + usersPasswHashed
            + "\n"
            + "user6:"
            + usersPasswHashed
            + "\n"
            + "user7:"
            + usersPasswHashed
            + "\n"
            + "user8:"
            + usersPasswHashed
            + "\n"
            + "user9:"
            + usersPasswHashed
            + "\n"
            + "user_different_fields:"
            + usersPasswHashed
            + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + """
            role1:user1
            role2:user1,user7,user8
            role3:user2,user7,user8
            role4:user3,user7
            role5:user4,user7
            role6:user5,user7
            role7:user6
            role8:user9
            role_different_fields:user_different_fields""";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """
            %s
            role1:
              cluster: [ none ]
              indices:
                - names: '*'
                  privileges: [ none ]
            role2:
              cluster: [ all ]
              indices:
                  - names: '*'
                    privileges: [ ALL ]
                    field_security:
                       grant: [ field1, join_field*, vector ]
            role3:
              cluster: [ all ]
              indices:
                  - names: '*'
                    privileges: [ ALL ]
                    field_security:
                       grant: [ field2, query*]
            role4:
              cluster: [ all ]
              indices:
                 - names: '*'
                   privileges: [ ALL ]
                   field_security:
                       grant: [ field1, field2]
            role5:
              cluster: [ all ]
              indices:
                  - names: '*'
                    privileges: [ ALL ]
                    field_security:
                       grant: [ ]
            role6:
              cluster: [ all ]
              indices:
                 - names: '*'
                   privileges: [ALL]
            role7:
              cluster: [ all ]
              indices:
                  - names: '*'
                    privileges: [ ALL ]
                    field_security:
                       grant: [ 'field*' ]
            role8:
              indices:
                  - names: 'doc_index'
                    privileges: [ ALL ]
                    field_security:
                       grant: [ 'field*' ]
                       except: [ 'field2' ]
                  - names: 'query_index'
                    privileges: [ ALL ]
                    field_security:
                       grant: [ 'field*', 'query' ]
            role_different_fields:
              indices:
                - names: [ 'partial1*' ]
                  privileges: [ 'read' ]
                  field_security:
                    grant: [ value, partial ]
                - names: [ 'partial2*' ]
                  privileges: [ 'read' ]
                  field_security:
                    grant: [ value ]
            """;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
            .build();
    }

    public void testQuery() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text", "alias", "type=alias,path=field1")
        );
        prepareIndex("test").setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // user1 has access to field1, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1")),
            1
        );
        // user2 has no access to field1, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1")),
            0
        );
        // user3 has access to field1 and field2, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1")),
            1
        );
        // user4 has access to no fields, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1")),
            0
        );
        // user5 has no field level security configured, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1")),
            1
        );
        // user7 has roles with field level security configured and without field level security
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1")),
            1
        );
        // user8 has roles with field level security configured for field1 and field2
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1")),
            1
        );

        // user1 has no access to field2, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2")),
            0
        );
        // user2 has access to field1, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2")),
            1
        );
        // user3 has access to field1 and field2, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2")),
            1
        );
        // user4 has access to no fields, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2")),
            0
        );
        // user5 has no field level security configured, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2")),
            1
        );
        // user7 has role with field level security and without field level security
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2")),
            1
        );
        // user8 has roles with field level security configured for field1 and field2
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2")),
            1
        );

        // user1 has access to field3, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3")),
            0
        );
        // user2 has no access to field3, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3")),
            0
        );
        // user3 has access to field1 and field2 but not field3, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3")),
            0
        );
        // user4 has access to no fields, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3")),
            0
        );
        // user5 has no field level security configured, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3")),
            1
        );
        // user7 has roles with field level security and without field level security
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3")),
            1
        );
        // user8 has roles with field level security configured for field1 and field2
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3")),
            0
        );

        // user1 has access to field1, so a query on its field alias should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("alias", "value1")),
            1
        );
        // user2 has no access to field1, so a query on its field alias should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("alias", "value1")),
            0
        );
    }

    public void testKnnSearch() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 3)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(indicesAdmin().prepareCreate("test").setMapping(builder));

        prepareIndex("test").setSource("field1", "value1", "field2", "value2", "vector", new float[] { 0.0f, 0.0f, 0.0f })
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // Since there's no kNN search action at the transport layer, we just emulate
        // how the action works (it builds a kNN query under the hood)
        float[] queryVector = new float[] { 0.0f, 0.0f, 0.0f };
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("vector", queryVector, 10, 10, null, null);

        // user1 has access to vector field, so the query should match with the document:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(query)
                .addFetchField("vector"),
            response -> {
                assertHitCount(response, 1);
                assertNotNull(response.getHits().getAt(0).field("vector"));
            }
        );

        // user2 has no access to vector field, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(query)
                .addFetchField("vector"),
            0
        );

        // check user2 cannot see the vector field, even when their search matches the document
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addFetchField("vector"),
            response -> {
                assertHitCount(response, 1);
                assertNull(response.getHits().getAt(0).field("vector"));
            }
        );
        // user1 can access field1, so the filtered query should match with the document:
        KnnVectorQueryBuilder filterQuery1 = new KnnVectorQueryBuilder("vector", queryVector, 10, 10, null, null).addFilterQuery(
            QueryBuilders.matchQuery("field1", "value1")
        );
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(filterQuery1),
            1
        );

        // user1 cannot access field2, so the filtered query should not match with the document:
        KnnVectorQueryBuilder filterQuery2 = new KnnVectorQueryBuilder("vector", queryVector, 10, 10, null, null).addFilterQuery(
            QueryBuilders.matchQuery("field2", "value2")
        );
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(filterQuery2),
            0
        );
    }

    public void testPercolateQueryWithIndexedDocWithFLS() {
        assertAcked(indicesAdmin().prepareCreate("query_index").setMapping("query", "type=percolator", "field2", "type=text"));
        assertAcked(indicesAdmin().prepareCreate("doc_index").setMapping("field2", "type=text", "field1", "type=text"));
        prepareIndex("query_index").setId("1").setSource("""
            {"query": {"match": {"field2": "bonsai tree"}}}""", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("doc_index").setId("1").setSource("""
            {"field1": "value1", "field2": "A new bonsai tree in the office"}""", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        QueryBuilder percolateQuery = new PercolateQueryBuilder("query", "doc_index", "1", null, null, null);
        // user7 sees everything
        assertHitCountAndNoFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareSearch("query_index")
                .setQuery(percolateQuery),
            1
        );
        assertHitCountAndNoFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("query_index")
                .setQuery(QueryBuilders.matchAllQuery()),
            1
        );
        // user 3 can see the fields of the percolated document, but not the "query" field of the indexed query
        assertHitCountAndNoFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("query_index")
                .setQuery(percolateQuery),
            0
        );
        assertHitCountAndNoFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user9", USERS_PASSWD)))
                .prepareSearch("query_index")
                .setQuery(QueryBuilders.matchAllQuery()),
            1
        );
        // user 9 can see the fields of the index query, but not the field of the indexed document to be percolated
        assertHitCountAndNoFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user9", USERS_PASSWD)))
                .prepareSearch("query_index")
                .setQuery(percolateQuery),
            0
        );
    }

    public void testGeoQueryWithIndexedShapeWithFLS() {
        assertAcked(indicesAdmin().prepareCreate("search_index").setMapping("field", "type=shape", "other", "type=shape"));
        assertAcked(indicesAdmin().prepareCreate("shape_index").setMapping("field", "type=shape", "other", "type=shape"));
        prepareIndex("search_index").setId("1").setSource("""
            {
              "field": {
                "type": "point",
                "coordinates": [ 1, 1 ]
              }
            }""", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("search_index").setId("2").setSource("""
            {
              "other": {
                "type": "point",
                "coordinates": [ 1, 1 ]
              }
            }""", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("shape_index").setId("1").setSource("""
            {
                "field": {
                  "type": "envelope",
                  "coordinates": [
                    [ 0, 2 ],
                    [ 2, 0 ]
                  ]
                },
                "field2": {
                  "type": "envelope",
                  "coordinates": [
                    [ 0, 2 ],
                    [ 2, 0 ]
                  ]
                }
              }""", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("shape_index").setId("2").setSource("""
            {
              "other": {
                "type": "envelope",
                "coordinates": [
                  [ 0, 2 ],
                  [ 2, 0 ]
                ]
              }
            }""", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        // user sees both the querying shape and the queried point
        SearchRequestBuilder requestBuilder = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD))
        ).prepareSearch("search_index");
        final ShapeQueryBuilder shapeQuery1 = new ShapeQueryBuilder("field", "1").relation(ShapeRelation.WITHIN)
            .indexedShapeIndex("shape_index")
            .indexedShapePath("field");
        if (randomBoolean()) {
            requestBuilder.setQuery(QueryBuilders.matchAllQuery()).setPostFilter(shapeQuery1);
        } else {
            requestBuilder.setQuery(shapeQuery1);
        }
        assertHitCountAndNoFailures(requestBuilder, 1);
        // user sees the queried point but not the querying shape
        final ShapeQueryBuilder shapeQuery2 = new ShapeQueryBuilder("field", "2").relation(ShapeRelation.WITHIN)
            .indexedShapeIndex("shape_index")
            .indexedShapePath("other");
        IllegalStateException e;
        if (randomBoolean()) {
            e = expectThrows(
                IllegalStateException.class,
                () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                    .prepareSearch("search_index")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setPostFilter(shapeQuery2)
                    .get()
            );
        } else {
            e = expectThrows(
                IllegalStateException.class,
                () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                    .prepareSearch("search_index")
                    .setQuery(shapeQuery2)
                    .get()
            );
        }
        assertThat(e.getMessage(), is("Shape with name [2] found but missing other field"));
        // user sees the querying shape but not the queried point
        requestBuilder = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
            .prepareSearch("search_index");
        final ShapeQueryBuilder shapeQuery3 = new ShapeQueryBuilder("other", "1").relation(ShapeRelation.WITHIN)
            .indexedShapeIndex("shape_index")
            .indexedShapePath("field");
        if (randomBoolean()) {
            requestBuilder.setQuery(QueryBuilders.matchAllQuery()).setPostFilter(shapeQuery3);
        } else {
            requestBuilder.setQuery(shapeQuery3);
        }
        assertHitCountAndNoFailures(requestBuilder, 0);
    }

    public void testTermsLookupOnIndexWithFLS() {
        assertAcked(indicesAdmin().prepareCreate("search_index").setMapping("field", "type=keyword", "other", "type=text"));
        assertAcked(indicesAdmin().prepareCreate("lookup_index").setMapping("field", "type=keyword", "other", "type=text"));
        prepareIndex("search_index").setId("1").setSource("field", List.of("value1", "value2")).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("search_index").setId("2")
            .setSource("field", "value1", "other", List.of("value1", "value2"))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("search_index").setId("3")
            .setSource("field", "value3", "other", List.of("value1", "value2"))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("lookup_index").setId("1").setSource("field", List.of("value1", "value2")).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("lookup_index").setId("2").setSource("other", "value2", "field", "value2").setRefreshPolicy(IMMEDIATE).get();

        // user sees the terms doc field
        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(QueryBuilders.termsLookupQuery("field", new TermsLookup("lookup_index", "1", "field"))),
            "1",
            "2"
        );
        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(QueryBuilders.termsLookupQuery("field", new TermsLookup("lookup_index", "2", "field"))),
            "1"
        );
        // user does not see the terms doc field
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(QueryBuilders.termsLookupQuery("field", new TermsLookup("lookup_index", "2", "other"))),
            0
        );
        // user does not see the queried field
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(QueryBuilders.termsLookupQuery("other", new TermsLookup("lookup_index", "1", "field"))),
            0
        );
    }

    public void testGetApi() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text"));

        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2", "field3", "value3").get();

        boolean realtime = randomBoolean();
        // user1 is granted access to field1 only:
        GetResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareGet("test", "1").setRealtime(realtime).setRefresh(true).get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(1));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
            .prepareGet("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(1));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
            .prepareGet("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(2));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));

        // user4 is granted access to no fields, so the get response does say the doc exist, but no fields are returned:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
            .prepareGet("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(0));

        // user5 has no field level security configured, so all fields are returned:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
            .prepareGet("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(3));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getSource().get("field3").toString(), equalTo("value3"));

        // user6 has access to field*
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
            .prepareGet("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(3));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getSource().get("field3").toString(), equalTo("value3"));

        // user7 has roles with field level security and without field level security
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
            .prepareGet("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(3));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getSource().get("field3").toString(), equalTo("value3"));

        // user8 has roles with field level security with access to field1 and field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
            .prepareGet("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(2));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
    }

    public void testRealtimeGetApi() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
                .setSettings(Settings.builder().put("refresh_interval", "-1").build())
        );
        final boolean realtime = true;
        final boolean refresh = false;

        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2", "field3", "value3").get();
        // do a realtime get beforehand to flip an internal translog flag so that subsequent realtime gets are
        // served from the translog (this first one is NOT, it internally forces a refresh of the index)
        client().prepareGet("test", "1").setRealtime(realtime).setRefresh(refresh).get();
        refresh("test");
        // updates don't change the doc visibility for users
        // but updates populate the translog and the FLS filter must apply to the translog operations too
        if (randomBoolean()) {
            prepareIndex("test").setId("1")
                .setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
                .get();
        } else {
            client().prepareUpdate("test", "1").setDoc(Map.of("field3", "value3")).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).get();
        }

        GetResponse getResponse;
        MultiGetResponse mgetResponse;
        // user1 is granted access to field1 only:
        if (randomBoolean()) {
            getResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareGet("test", "1").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getSource().size(), equalTo(1));
            assertThat(getResponse.getSource().get("field1").toString(), equalTo("value1"));
        } else {
            mgetResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareMultiGet().add("test", "1").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));
            assertThat(mgetResponse.getResponses()[0].getResponse().getSource().size(), equalTo(1));
            assertThat(mgetResponse.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        }
        // user2 is granted access to field2 only:
        if (randomBoolean()) {
            getResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).prepareGet("test", "1").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getSource().size(), equalTo(1));
            assertThat(getResponse.getSource().get("field2").toString(), equalTo("value2"));
        } else {
            mgetResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).prepareMultiGet().add("test", "1").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));
            assertThat(mgetResponse.getResponses()[0].getResponse().getSource().size(), equalTo(1));
            assertThat(mgetResponse.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
        }
    }

    public void testMGetApi() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2", "field3", "value3").get();

        boolean realtime = randomBoolean();
        // user1 is granted access to field1 only:
        MultiGetResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareMultiGet().add("test", "1").setRealtime(realtime).setRefresh(true).get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
            .prepareMultiGet()
            .add("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
            .prepareMultiGet()
            .add("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));

        // user4 is granted access to no fields, so the get response does say the doc exist, but no fields are returned:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
            .prepareMultiGet()
            .add("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(0));

        // user5 has no field level security configured, so all fields are returned:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
            .prepareMultiGet()
            .add("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field3").toString(), equalTo("value3"));

        // user6 has access to field*
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
            .prepareMultiGet()
            .add("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field3").toString(), equalTo("value3"));

        // user7 has roles with field level security and without field level security
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
            .prepareMultiGet()
            .add("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field3").toString(), equalTo("value3"));

        // user8 has roles with field level security with access to field1 and field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
            .prepareMultiGet()
            .add("test", "1")
            .setRealtime(realtime)
            .setRefresh(true)
            .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
    }

    public void testMSearchApi() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test1").setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text"));
        assertAcked(indicesAdmin().prepareCreate("test2").setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text"));

        prepareIndex("test1").setId("1").setSource("field1", "value1", "field2", "value2", "field3", "value3").get();
        prepareIndex("test2").setId("1").setSource("field1", "value1", "field2", "value2", "field3", "value3").get();
        indicesAdmin().prepareRefresh("test1", "test2").get();

        // user1 is granted access to field1 only
        {
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
                    Map<String, Object> source0 = response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap();
                    Map<String, Object> source1 = response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap();
                    assertFalse(response.getResponses()[0].isFailure());
                    assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source0.size(), is(1));
                    assertThat(source0.get("field1"), is("value1"));
                    assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source1.size(), is(1));
                    assertThat(source1.get("field1"), is("value1"));
                }
            );
        }

        {
            // user2 is granted access to field2 only
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
                    Map<String, Object> source0 = response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap();
                    Map<String, Object> source1 = response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap();
                    assertFalse(response.getResponses()[0].isFailure());
                    assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source0.size(), is(1));
                    assertThat(source0.get("field2"), is("value2"));
                    assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source1.size(), is(1));
                    assertThat(source1.get("field2"), is("value2"));
                }
            );
        }
        {
            // user3 is granted access to field1 and field2
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
                    Map<String, Object> source0 = response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap();
                    Map<String, Object> source1 = response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap();
                    assertFalse(response.getResponses()[0].isFailure());
                    assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source0.size(), is(2));
                    assertThat(source0.get("field1"), is("value1"));
                    assertThat(source0.get("field2"), is("value2"));
                    assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source1.size(), is(2));
                    assertThat(source1.get("field1"), is("value1"));
                    assertThat(source1.get("field2"), is("value2"));
                }
            );
        }
        {
            // user4 is granted access to no fields, so the search response does say the doc exist, but no fields are returned
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
                    assertFalse(response.getResponses()[0].isFailure());
                    assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().size(), is(0));
                    assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(response.getResponses()[01].getResponse().getHits().getAt(0).getSourceAsMap().size(), is(0));
                }
            );
        }
        {
            // user5 has no field level security configured, so all fields are returned
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
                    Map<String, Object> source0 = response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap();
                    Map<String, Object> source1 = response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap();
                    assertFalse(response.getResponses()[0].isFailure());
                    assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source0.size(), is(3));
                    assertThat(source0.get("field1"), is("value1"));
                    assertThat(source0.get("field2"), is("value2"));
                    assertThat(source0.get("field3"), is("value3"));
                    assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source1.size(), is(3));
                    assertThat(source1.get("field1"), is("value1"));
                    assertThat(source1.get("field2"), is("value2"));
                    assertThat(source1.get("field3"), is("value3"));
                }
            );
        }
        {
            // user6 has access to field*
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
                    Map<String, Object> source0 = response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap();
                    Map<String, Object> source1 = response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap();
                    assertFalse(response.getResponses()[0].isFailure());
                    assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source0.size(), is(3));
                    assertThat(source0.get("field1"), is("value1"));
                    assertThat(source0.get("field2"), is("value2"));
                    assertThat(source0.get("field3"), is("value3"));
                    assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source1.size(), is(3));
                    assertThat(source1.get("field1"), is("value1"));
                    assertThat(source1.get("field2"), is("value2"));
                    assertThat(source1.get("field3"), is("value3"));
                }
            );
        }
        {
            // user7 has roles with field level security and without field level security
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
                    Map<String, Object> source0 = response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap();
                    Map<String, Object> source1 = response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap();
                    assertFalse(response.getResponses()[0].isFailure());
                    assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source0.size(), is(3));
                    assertThat(source0.get("field1"), is("value1"));
                    assertThat(source0.get("field2"), is("value2"));
                    assertThat(source0.get("field3"), is("value3"));
                    assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source1.size(), is(3));
                    assertThat(source1.get("field1"), is("value1"));
                    assertThat(source1.get("field2"), is("value2"));
                    assertThat(source1.get("field3"), is("value3"));
                }
            );
        }
        {
            // user8 has roles with field level security with access to field1 and field2
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
                    Map<String, Object> source0 = response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap();
                    Map<String, Object> source1 = response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap();
                    assertFalse(response.getResponses()[0].isFailure());
                    assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source0.size(), is(2));
                    assertThat(source0.get("field1"), is("value1"));
                    assertThat(source0.get("field2"), is("value2"));
                    assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value(), is(1L));
                    assertThat(source1.size(), is(2));
                    assertThat(source1.get("field1"), is("value1"));
                    assertThat(source1.get("field2"), is("value2"));
                }
            );
        }
    }

    public void testScroll() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        final int numDocs = scaledRandomIntBetween(2, 10);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(String.valueOf(i)).setSource("field1", "value1", "field2", "value2", "field3", "value3").get();
        }
        refresh("test");

        SearchResponse response = null;
        try {
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(1)
                .setQuery(constantScoreQuery(termQuery("field1", "value1")))
                .setFetchSource(true)
                .get();

            do {
                assertThat(response.getHits().getTotalHits().value(), is((long) numDocs));
                assertThat(response.getHits().getHits().length, is(1));
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), is(1));
                assertThat(source.get("field1"), is("value1"));

                if (response.getScrollId() == null) {
                    break;
                }

                response.decRef();
                response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                ).prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueMinutes(1L)).get();
            } while (response.getHits().getHits().length > 0);

        } finally {
            if (response != null) {
                String scrollId = response.getScrollId();
                response.decRef();
                if (scrollId != null) {
                    client().prepareClearScroll().addScrollId(scrollId).get();
                }
            }
        }
    }

    static BytesReference openPointInTime(String userName, TimeValue keepAlive, String... indices) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(userName, USERS_PASSWD))
        ).execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    public void testPointInTimeId() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        final int numDocs = scaledRandomIntBetween(2, 10);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(String.valueOf(i)).setSource("field1", "value1", "field2", "value2", "field3", "value3").get();
        }
        refresh("test");

        BytesReference pitId = openPointInTime("user1", TimeValue.timeValueMinutes(1), "test");
        try {
            for (int from = 0; from < numDocs; from++) {
                assertResponse(
                    client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                        .prepareSearch()
                        .setPointInTime(new PointInTimeBuilder(pitId))
                        .setSize(1)
                        .setFrom(from)
                        .setQuery(constantScoreQuery(termQuery("field1", "value1")))
                        .setFetchSource(true),
                    response -> {
                        Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                        assertThat(response.getHits().getTotalHits().value(), is((long) numDocs));
                        assertThat(response.getHits().getHits().length, is(1));
                        assertThat(source.size(), is(1));
                        assertThat(source.get("field1"), is("value1"));
                    }
                );
            }
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        }
    }

    public void testQueryCache() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        prepareIndex("test").setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setQuery(constantScoreQuery(termQuery("field1", "value1"))),
                response -> {
                    Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                    assertHitCount(response, 1);
                    assertThat(source.size(), is(1));
                    assertThat(source.get("field1"), is("value1"));
                }
            );
            assertHitCountAndNoFailures(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setQuery(constantScoreQuery(termQuery("field1", "value1"))),
                0
            );
            String multipleFieldsUser = randomFrom("user5", "user6", "user7");
            assertResponse(
                client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(multipleFieldsUser, USERS_PASSWD))
                ).prepareSearch("test").setQuery(constantScoreQuery(termQuery("field1", "value1"))),
                response -> {
                    Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                    assertHitCount(response, 1);
                    assertThat(source.size(), is(3));
                    assertThat(source.get("field1"), is("value1"));
                    assertThat(source.get("field2"), is("value2"));
                    assertThat(source.get("field3"), is("value3"));
                }
            );
        }
    }

    public void testScrollWithQueryCache() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text")
        );

        final int numDocs = scaledRandomIntBetween(2, 4);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(String.valueOf(i)).setSource("field1", "value1", "field2", "value2").get();
        }
        refresh("test");

        final QueryBuilder cacheableQueryBuilder = constantScoreQuery(termQuery("field1", "value1"));

        SearchResponse user1SearchResponse = null;
        SearchResponse user2SearchResponse = null;
        int scrolledDocsUser1 = 0;
        final int numScrollSearch = scaledRandomIntBetween(20, 30);

        try {
            for (int i = 0; i < numScrollSearch; i++) {
                if (randomBoolean()) {
                    if (user2SearchResponse == null) {
                        user2SearchResponse = client().filterWithHeader(
                            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                        )
                            .prepareSearch("test")
                            .setQuery(cacheableQueryBuilder)
                            .setScroll(TimeValue.timeValueMinutes(10L))
                            .setSize(1)
                            .setFetchSource(true)
                            .get();
                        assertThat(user2SearchResponse.getHits().getTotalHits().value(), is((long) 0));
                        assertThat(user2SearchResponse.getHits().getHits().length, is(0));
                    } else {
                        user2SearchResponse.decRef();
                        // make sure scroll is empty
                        user2SearchResponse = client().filterWithHeader(
                            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                        ).prepareSearchScroll(user2SearchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(10L)).get();
                        assertThat(user2SearchResponse.getHits().getTotalHits().value(), is((long) 0));
                        assertThat(user2SearchResponse.getHits().getHits().length, is(0));
                        if (randomBoolean()) {
                            // maybe reuse the scroll even if empty
                            client().prepareClearScroll().addScrollId(user2SearchResponse.getScrollId()).get();
                            user2SearchResponse.decRef();
                            user2SearchResponse = null;
                        }
                    }
                } else {
                    if (user1SearchResponse == null) {
                        user1SearchResponse = client().filterWithHeader(
                            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                        )
                            .prepareSearch("test")
                            .setQuery(cacheableQueryBuilder)
                            .setScroll(TimeValue.timeValueMinutes(10L))
                            .setSize(1)
                            .setFetchSource(true)
                            .get();
                        assertThat(user1SearchResponse.getHits().getTotalHits().value(), is((long) numDocs));
                        assertThat(user1SearchResponse.getHits().getHits().length, is(1));
                        Map<String, Object> source = user1SearchResponse.getHits().getAt(0).getSourceAsMap();
                        assertThat(source.size(), is(1));
                        assertThat(source.get("field1"), is("value1"));
                        scrolledDocsUser1++;
                    } else {
                        user1SearchResponse.decRef();
                        user1SearchResponse = client().filterWithHeader(
                            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                        ).prepareSearchScroll(user1SearchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(10L)).get();
                        assertThat(user1SearchResponse.getHits().getTotalHits().value(), is((long) numDocs));
                        if (scrolledDocsUser1 < numDocs) {
                            assertThat(user1SearchResponse.getHits().getHits().length, is(1));
                            Map<String, Object> source = user1SearchResponse.getHits().getAt(0).getSourceAsMap();
                            assertThat(source.size(), is(1));
                            assertThat(source.get("field1"), is("value1"));
                            scrolledDocsUser1++;
                        } else {
                            assertThat(user1SearchResponse.getHits().getHits().length, is(0));
                            if (randomBoolean()) {
                                // maybe reuse the scroll even if empty
                                if (user1SearchResponse.getScrollId() != null) {
                                    client().prepareClearScroll().addScrollId(user1SearchResponse.getScrollId()).get();
                                }
                                user1SearchResponse.decRef();
                                user1SearchResponse = null;
                                scrolledDocsUser1 = 0;
                            }
                        }
                    }
                }
            }
        } finally {
            if (user1SearchResponse != null) {
                String scrollId = user1SearchResponse.getScrollId();
                user1SearchResponse.decRef();
                if (scrollId != null) {
                    client().prepareClearScroll().addScrollId(scrollId).get();
                }
            }
            if (user2SearchResponse != null) {
                String scrollId = user2SearchResponse.getScrollId();
                user2SearchResponse.decRef();
                if (scrollId != null) {
                    client().prepareClearScroll().addScrollId(scrollId).get();
                }
            }
        }
    }

    public void testRequestCache() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            Boolean requestCache = randomFrom(true, null);
            assertHitCountAndNoFailures(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache),
                1
            );
            assertHitCountAndNoFailures(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache),
                0
            );
            String multipleFieldsUser = randomFrom("user5", "user6", "user7");
            assertHitCountAndNoFailures(
                client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(multipleFieldsUser, USERS_PASSWD))
                ).prepareSearch("test").setSize(0).setQuery(termQuery("field1", "value1")).setRequestCache(requestCache),
                1
            );
        }
    }

    public void testFields() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping(
                    "field1",
                    "type=text,store=true",
                    "field2",
                    "type=text,store=true",
                    "field3",
                    "type=text,store=true",
                    "alias",
                    "type=alias,path=field1"
                )
        );
        prepareIndex("test").setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // user1 is granted access to field1 only:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3"),
            response -> {
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(1));
                assertThat(response.getHits().getAt(0).getFields().get("field1").<String>getValue(), equalTo("value1"));
            }
        );

        // user2 is granted access to field2 only:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3"),
            response -> {
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(1));
                assertThat(response.getHits().getAt(0).getFields().get("field2").<String>getValue(), equalTo("value2"));
            }
        );

        // user3 is granted access to field1 and field2:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3"),
            response -> {
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(2));
                assertThat(response.getHits().getAt(0).getFields().get("field1").<String>getValue(), equalTo("value1"));
                assertThat(response.getHits().getAt(0).getFields().get("field2").<String>getValue(), equalTo("value2"));
            }
        );

        // user4 is granted access to no fields:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3"),
            response -> assertThat(response.getHits().getAt(0).getFields().size(), equalTo(0))
        );

        // user5 has no field level security configured:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3"),
            response -> {
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(3));
                assertThat(response.getHits().getAt(0).getFields().get("field1").<String>getValue(), equalTo("value1"));
                assertThat(response.getHits().getAt(0).getFields().get("field2").<String>getValue(), equalTo("value2"));
                assertThat(response.getHits().getAt(0).getFields().get("field3").<String>getValue(), equalTo("value3"));
            }
        );

        // user6 has field level security configured with access to field*:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3"),
            response -> {
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(3));
                assertThat(response.getHits().getAt(0).getFields().get("field1").<String>getValue(), equalTo("value1"));
                assertThat(response.getHits().getAt(0).getFields().get("field2").<String>getValue(), equalTo("value2"));
                assertThat(response.getHits().getAt(0).getFields().get("field3").<String>getValue(), equalTo("value3"));
            }
        );

        // user7 has access to all fields due to a mix of roles without field level security and with:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3"),
            response -> {
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(3));
                assertThat(response.getHits().getAt(0).getFields().get("field1").<String>getValue(), equalTo("value1"));
                assertThat(response.getHits().getAt(0).getFields().get("field2").<String>getValue(), equalTo("value2"));
                assertThat(response.getHits().getAt(0).getFields().get("field3").<String>getValue(), equalTo("value3"));
            }
        );

        // user8 has field level security configured with access to field1 and field2:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3"),
            response -> {
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(2));
                assertThat(response.getHits().getAt(0).getFields().get("field1").<String>getValue(), equalTo("value1"));
                assertThat(response.getHits().getAt(0).getFields().get("field2").<String>getValue(), equalTo("value2"));
            }
        );

        // user1 is granted access to field1 only, and so should be able to load it by alias:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("alias"),
            response -> {
                assertThat(response.getHits().getAt(0).getFields().size(), equalTo(1));
                assertThat(response.getHits().getAt(0).getFields().get("alias").getValue(), equalTo("value1"));
            }
        );

        // user2 is not granted access to field1, and so should not be able to load it by alias:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("alias"),
            response -> assertThat(response.getHits().getAt(0).getFields().size(), equalTo(0))
        );
    }

    public void testSource() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text"));
        prepareIndex("test").setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // user1 is granted access to field1 only:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(1));
                assertThat(source.get("field1").toString(), equalTo("value1"));
            }
        );

        // user2 is granted access to field2 only:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(1));
                assertThat(source.get("field2").toString(), equalTo("value2"));
            }
        );

        // user3 is granted access to field1 and field2:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(2));
                assertThat(source.get("field1").toString(), equalTo("value1"));
                assertThat(source.get("field2").toString(), equalTo("value2"));
            }
        );

        // user4 is granted access to no fields:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(0))
        );

        // user5 has no field level security configured:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(3));
                assertThat(source.get("field1").toString(), equalTo("value1"));
                assertThat(source.get("field2").toString(), equalTo("value2"));
                assertThat(source.get("field3").toString(), equalTo("value3"));
            }
        );

        // user6 has field level security configured with access to field*:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(3));
                assertThat(source.get("field1").toString(), equalTo("value1"));
                assertThat(source.get("field2").toString(), equalTo("value2"));
                assertThat(source.get("field3").toString(), equalTo("value3"));
            }
        );

        // user7 has access to all fields
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(3));
                assertThat(source.get("field1").toString(), equalTo("value1"));
                assertThat(source.get("field2").toString(), equalTo("value2"));
                assertThat(source.get("field3").toString(), equalTo("value3"));
            }
        );

        // user8 has field level security configured with access to field1 and field2:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test"),
            response -> {
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(2));
                assertThat(source.get("field1").toString(), equalTo("value1"));
                assertThat(source.get("field2").toString(), equalTo("value2"));
            }
        );
    }

    public void testSort() {
        assertAcked(
            indicesAdmin().prepareCreate("test").setMapping("field1", "type=long", "field2", "type=long", "alias", "type=alias,path=field1")
        );
        prepareIndex("test").setId("1").setSource("field1", 1d, "field2", 2d).setRefreshPolicy(IMMEDIATE).get();

        // user1 is granted to use field1, so it is included in the sort_values
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("field1", SortOrder.ASC),
            response -> assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo(1L))
        );

        // user2 is not granted to use field1, so the default missing sort value is included
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("field1", SortOrder.ASC),
            response -> assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo(Long.MAX_VALUE))
        );

        // user1 is not granted to use field2, so the default missing sort value is included
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("field2", SortOrder.ASC),
            response -> assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo(Long.MAX_VALUE))
        );

        // user2 is granted to use field2, so it is included in the sort_values
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("field2", SortOrder.ASC),
            response -> assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo(2L))
        );

        // user1 is granted to use field1, so it is included in the sort_values when using its alias:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("alias", SortOrder.ASC),
            response -> assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo(1L))
        );

        // user2 is not granted to use field1, so the default missing sort value is included when using its alias:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("alias", SortOrder.ASC),
            response -> assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo(Long.MAX_VALUE))
        );
    }

    public void testHighlighting() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text", "alias", "type=alias,path=field1")
        );
        prepareIndex("test").setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // user1 has access to field1, so the highlight should be visible:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .highlighter(new HighlightBuilder().field("field1")),
            response -> {
                assertHitCount(response, 1);
                SearchHit hit = response.getHits().iterator().next();
                assertEquals(hit.getHighlightFields().size(), 1);
            }
        );

        // user2 has no access to field1, so the highlight should not be visible:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .highlighter(new HighlightBuilder().field("field1")),
            response -> {
                assertHitCount(response, 1);
                var hit = response.getHits().iterator().next();
                assertEquals(hit.getHighlightFields().size(), 0);
            }
        );

        // user1 has access to field1, so the highlight on its alias should be visible:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .highlighter(new HighlightBuilder().field("alias")),
            response -> {
                assertHitCount(response, 1);
                var hit = response.getHits().iterator().next();
                assertEquals(hit.getHighlightFields().size(), 1);
            }
        );

        // user2 has no access to field1, so the highlight on its alias should not be visible:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .highlighter(new HighlightBuilder().field("alias")),
            response -> {
                assertHitCount(response, 1);
                var hit = response.getHits().iterator().next();
                assertEquals(hit.getHighlightFields().size(), 0);
            }
        );
    }

    public void testAggs() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("field1", "type=text,fielddata=true", "field2", "type=text,fielddata=true", "alias", "type=alias,path=field1")
        );
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        // user1 is authorized to use field1, so buckets are include for a term agg on field1
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("field1")),
            response -> assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1").getDocCount(), equalTo(1L))
        );

        // user2 is not authorized to use field1, so no buckets are include for a term agg on field1
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("field1")),
            response -> assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1"), nullValue())
        );

        // user1 is not authorized to use field2, so no buckets are include for a term agg on field2
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("field2")),
            response -> assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value2"), nullValue())
        );

        // user2 is authorized to use field2, so buckets are include for a term agg on field2
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("field2")),
            response -> assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value2").getDocCount(), equalTo(1L))
        );

        // user1 is authorized to use field1, so buckets are include for a term agg on its alias:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("alias")),
            response -> assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1").getDocCount(), equalTo(1L))
        );

        // user2 is not authorized to use field1, so no buckets are include for a term agg on its alias:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("alias")),
            response -> assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1"), nullValue())
        );
    }

    public void testTVApi() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping(
                    "field1",
                    "type=text,term_vector=with_positions_offsets_payloads",
                    "field2",
                    "type=text,term_vector=with_positions_offsets_payloads",
                    "field3",
                    "type=text,term_vector=with_positions_offsets_payloads"
                )
        );
        prepareIndex("test").setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        boolean realtime = randomBoolean();
        TermVectorsResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareTermVectors("test", "1").setRealtime(realtime).get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
            .prepareTermVectors("test", "1")
            .setRealtime(realtime)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
            .prepareTermVectors("test", "1")
            .setRealtime(realtime)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(2));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
            .prepareTermVectors("test", "1")
            .setRealtime(realtime)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(0));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
            .prepareTermVectors("test", "1")
            .setRealtime(realtime)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(3));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
            .prepareTermVectors("test", "1")
            .setRealtime(realtime)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(3));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
            .prepareTermVectors("test", "1")
            .setRealtime(realtime)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(3));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
            .prepareTermVectors("test", "1")
            .setRealtime(realtime)
            .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(2));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));
    }

    public void testMTVApi() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping(
                    "field1",
                    "type=text,term_vector=with_positions_offsets_payloads",
                    "field2",
                    "type=text,term_vector=with_positions_offsets_payloads",
                    "field3",
                    "type=text,term_vector=with_positions_offsets_payloads"
                )
        );
        prepareIndex("test").setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        boolean realtime = randomBoolean();
        MultiTermVectorsResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareMultiTermVectors().add(new TermVectorsRequest("test", "1").realtime(realtime)).get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
            .prepareMultiTermVectors()
            .add(new TermVectorsRequest("test", "1").realtime(realtime))
            .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
            .prepareMultiTermVectors()
            .add(new TermVectorsRequest("test", "1").realtime(realtime))
            .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
            .prepareMultiTermVectors()
            .add(new TermVectorsRequest("test", "1").realtime(realtime))
            .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(0));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
            .prepareMultiTermVectors()
            .add(new TermVectorsRequest("test", "1").realtime(realtime))
            .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
            .prepareMultiTermVectors()
            .add(new TermVectorsRequest("test", "1").realtime(realtime))
            .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
            .prepareMultiTermVectors()
            .add(new TermVectorsRequest("test", "1").realtime(realtime))
            .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
            .prepareMultiTermVectors()
            .add(new TermVectorsRequest("test", "1").realtime(realtime))
            .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));
    }

    public void testParentChild() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("alias")
            .field("type", "alias")
            .field("path", "field1")
            .endObject()
            .startObject("join_field")
            .field("type", "join")
            .startObject("relations")
            .field("parent", "child")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").setMapping(mapping));
        ensureGreen();

        // index simple data
        prepareIndex("test").setId("p1").setSource("join_field", "parent").get();
        Map<String, Object> source = new HashMap<>();
        source.put("field1", "red");
        Map<String, Object> joinField = new HashMap<>();
        joinField.put("name", "child");
        joinField.put("parent", "p1");
        source.put("join_field", joinField);
        prepareIndex("test").setId("c1").setSource(source).setRouting("p1").get();
        source = new HashMap<>();
        source.put("field1", "yellow");
        source.put("join_field", joinField);
        prepareIndex("test").setId("c2").setSource(source).setRouting("p1").get();
        refresh();
        verifyParentChild();
    }

    private void verifyParentChild() {
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("field1", "yellow"), ScoreMode.None)),
            searchResponse -> {
                assertHitCount(searchResponse, 1L);
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("p1"));
            }
        );

        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("field1", "yellow"), ScoreMode.None)),
            0L
        );

        // Perform the same checks, but using an alias for field1.
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("alias", "yellow"), ScoreMode.None)),
            searchResponse -> {
                assertHitCount(searchResponse, 1L);
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("p1"));
            }
        );

        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("alias", "yellow"), ScoreMode.None)),
            0L
        );
    }

    public void testUpdateApiIsBlocked() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value1").setRefreshPolicy(IMMEDIATE).get();

        // With field level security enabled the update is not allowed:
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareUpdate("test", "1")
                .setDoc(Requests.INDEX_CONTENT_TYPE, "field2", "value2")
                .get();
            fail("failed, because update request shouldn't be allowed if field level security is enabled");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(), equalTo("Can't execute an update request if field or document level security is enabled"));
        }
        assertThat(client().prepareGet("test", "1").get().getSource().get("field2").toString(), equalTo("value1"));

        // With no field level security enabled the update is allowed:
        client().prepareUpdate("test", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field2", "value2").get();
        assertThat(client().prepareGet("test", "1").get().getSource().get("field2").toString(), equalTo("value2"));

        // With field level security enabled the update in bulk is not allowed:
        BulkResponse bulkResponse = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareBulk().add(new UpdateRequest("test", "1").doc(Requests.INDEX_CONTENT_TYPE, "field2", "value3")).get();
        assertEquals(1, bulkResponse.getItems().length);
        BulkItemResponse bulkItem = bulkResponse.getItems()[0];
        assertTrue(bulkItem.isFailed());
        assertThat(bulkItem.getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        ElasticsearchSecurityException securityException = (ElasticsearchSecurityException) bulkItem.getFailure().getCause();
        assertThat(securityException.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            securityException.getMessage(),
            equalTo("Can't execute a bulk item request with update requests embedded if field or document level security is enabled")
        );

        assertThat(client().prepareGet("test", "1").get().getSource().get("field2").toString(), equalTo("value2"));

        client().prepareBulk().add(new UpdateRequest("test", "1").doc(Requests.INDEX_CONTENT_TYPE, "field2", "value3")).get();
        assertThat(client().prepareGet("test", "1").get().getSource().get("field2").toString(), equalTo("value3"));
    }

    public void testQuery_withRoleWithFieldWildcards() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        // user6 has access to all fields, so the query should match with the document:
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1")),
            response -> {
                assertHitCount(response, 1);
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertThat(source.size(), equalTo(2));
                assertThat(source.get("field1").toString(), equalTo("value1"));
                assertThat(source.get("field2").toString(), equalTo("value2"));
            }
        );

        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2")),
            response -> {
                Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
                assertHitCount(response, 1);
                assertThat(source.size(), equalTo(2));
                assertThat(source.get("field1").toString(), equalTo("value1"));
                assertThat(source.get("field2").toString(), equalTo("value2"));
            }
        );
    }

    public void testExistQuery() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text", "alias", "type=alias,path=field1")
        );

        prepareIndex("test").setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // user1 has access to field1, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field1")),
            1
        );
        // user1 has no access to field2, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field2")),
            0
        );
        // user2 has no access to field1, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field1")),
            0
        );
        // user2 has access to field2, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field2")),
            1
        );
        // user3 has access to field1 and field2, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field1")),
            1
        );
        // user3 has access to field1 and field2, so the query should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field2")),
            1
        );
        // user4 has access to no fields, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field1")),
            0
        );
        // user4 has access to no fields, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field2")),
            0
        );

        // user1 has access to field1, so a query on its alias should match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("alias")),
            1
        );
        // user2 has no access to field1, so the query should not match with the document:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("alias")),
            0
        );
    }

    public void testLookupRuntimeFields() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("hosts").setMapping("field1", "type=keyword", "field2", "type=text", "field3", "type=text")
        );
        prepareIndex("hosts").setId("1")
            .setSource("field1", "192.168.1.1", "field2", "windows", "field3", "canada")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("hosts").setId("2")
            .setSource("field1", "192.168.1.2", "field2", "macos", "field3", "us")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        assertAcked(
            indicesAdmin().prepareCreate("logs")
                .setMapping("field1", "type=keyword", "field2", "type=text", "field3", "type=date,format=yyyy-MM-dd")
        );

        prepareIndex("logs").setId("1")
            .setSource("field1", "192.168.1.1", "field2", "out of memory", "field3", "2021-01-20")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("logs").setId("2")
            .setSource("field1", "192.168.1.2", "field2", "authentication fails", "field3", "2021-01-21")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        Map<String, Object> lookupField = Map.of(
            "type",
            "lookup",
            "target_index",
            "hosts",
            "input_field",
            "field1",
            "target_field",
            "field1",
            "fetch_fields",
            List.of("field1", "field2", "field3")
        );
        SearchRequest request = new SearchRequest("logs").source(
            new SearchSourceBuilder().fetchSource(false)
                .fetchField("field1")
                .fetchField("field2")
                .fetchField("field3")
                .fetchField("host")
                .sort("field1")
                .runtimeMappings(Map.of("host", lookupField))
        );
        // user1 has access to field1
        assertResponse(
            client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))).search(request),
            response -> {
                assertHitCount(response, 2);
                {
                    SearchHit hit0 = response.getHits().getHits()[0];
                    assertThat(hit0.getDocumentFields().keySet(), equalTo(Set.of("field1", "host")));
                    assertThat(hit0.field("field1").getValues(), equalTo(List.of("192.168.1.1")));
                    assertThat(hit0.field("host").getValues(), equalTo(List.of(Map.of("field1", List.of("192.168.1.1")))));
                }
                {
                    SearchHit hit1 = response.getHits().getHits()[1];
                    assertThat(hit1.getDocumentFields().keySet(), equalTo(Set.of("field1", "host")));
                    assertThat(hit1.field("field1").getValues(), equalTo(List.of("192.168.1.2")));
                    assertThat(hit1.field("host").getValues(), equalTo(List.of(Map.of("field1", List.of("192.168.1.2")))));
                }
            }
        );
        // user3 has access to field1, field2
        assertResponse(
            client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))).search(request),
            response -> {
                assertHitCount(response, 2);
                {
                    SearchHit hit0 = response.getHits().getHits()[0];
                    assertThat(hit0.getDocumentFields().keySet(), equalTo(Set.of("field1", "field2", "host")));
                    assertThat(hit0.field("field1").getValues(), equalTo(List.of("192.168.1.1")));
                    assertThat(hit0.field("field2").getValues(), equalTo(List.of("out of memory")));
                    assertThat(
                        hit0.field("host").getValues(),
                        equalTo(List.of(Map.of("field1", List.of("192.168.1.1"), "field2", List.of("windows"))))
                    );
                }
                {
                    SearchHit hit1 = response.getHits().getHits()[1];
                    assertThat(hit1.getDocumentFields().keySet(), equalTo(Set.of("field1", "field2", "host")));
                    assertThat(hit1.field("field1").getValues(), equalTo(List.of("192.168.1.2")));
                    assertThat(hit1.field("field2").getValues(), equalTo(List.of("authentication fails")));
                    assertThat(
                        hit1.field("host").getValues(),
                        equalTo(List.of(Map.of("field1", List.of("192.168.1.2"), "field2", List.of("macos"))))
                    );
                }
            }
        );
        // user6 has access to field1, field2, and field3
        assertResponse(
            client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD))).search(request),
            response -> {
                assertHitCount(response, 2);
                {
                    SearchHit hit0 = response.getHits().getHits()[0];
                    assertThat(hit0.getDocumentFields().keySet(), equalTo(Set.of("field1", "field2", "field3", "host")));
                    assertThat(hit0.field("field1").getValues(), equalTo(List.of("192.168.1.1")));
                    assertThat(hit0.field("field2").getValues(), equalTo(List.of("out of memory")));
                    assertThat(hit0.field("field3").getValues(), equalTo(List.of("2021-01-20")));
                    assertThat(
                        hit0.field("host").getValues(),
                        equalTo(
                            List.of(Map.of("field1", List.of("192.168.1.1"), "field2", List.of("windows"), "field3", List.of("canada")))
                        )
                    );
                }
                {
                    SearchHit hit1 = response.getHits().getHits()[1];
                    assertThat(hit1.getDocumentFields().keySet(), equalTo(Set.of("field1", "field2", "field3", "host")));
                    assertThat(hit1.field("field1").getValues(), equalTo(List.of("192.168.1.2")));
                    assertThat(hit1.field("field2").getValues(), equalTo(List.of("authentication fails")));
                    assertThat(hit1.field("field3").getValues(), equalTo(List.of("2021-01-21")));
                    assertThat(
                        hit1.field("host").getValues(),
                        equalTo(List.of(Map.of("field1", List.of("192.168.1.2"), "field2", List.of("macos"), "field3", List.of("us"))))
                    );
                }
            }
        );
    }

    public void testSearchDifferentFieldsVisible() {
        String firstName = "partial1" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        String secondName = "partial2" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        indexPartial(firstName, secondName);
        SearchResponse response = client().filterWithHeader(
            Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_different_fields", USERS_PASSWD))
        ).prepareSearch("partial*").addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC)).get();
        try {
            assertMap(response.getHits().getAt(0).getSourceAsMap(), matchesMap().entry("value", 1).entry("partial", 2));
            assertMap(response.getHits().getAt(1).getSourceAsMap(), matchesMap().entry("value", 2));
        } finally {
            response.decRef();
        }
    }

    /**
     * The fields {@code partial} is only visible in one of the two backing indices and field caps should show it.
     */
    public void testFieldCapsDifferentFieldsVisible() {
        String firstName = "partial1_" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        String secondName = "partial2_" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        indexPartial(firstName, secondName);
        FieldCapabilitiesResponse response = client().filterWithHeader(
            Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("user_different_fields", USERS_PASSWD))
        ).prepareFieldCaps("partial*").setFields("value", "partial").get();
        try {
            assertThat(response.get().keySet(), equalTo(Set.of("value", "partial")));
            assertThat(response.getField("value").keySet(), equalTo(Set.of("long")));
            assertThat(response.getField("partial").keySet(), equalTo(Set.of("long")));
        } finally {
            response.decRef();
        }
    }

    private void indexPartial(String firstName, String secondName) {
        BulkResponse bulkResponse = client().prepareBulk()
            .add(client().prepareIndex(firstName).setSource("value", 1, "partial", 2))
            .add(client().prepareIndex(secondName).setSource("value", 2, "partial", 3))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        for (var i : bulkResponse.getItems()) {
            assertThat(i.getFailure(), nullValue());
            assertThat(i.status(), equalTo(RestStatus.CREATED));
        }
    }
}
