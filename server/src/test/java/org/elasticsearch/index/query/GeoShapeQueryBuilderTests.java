/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class GeoShapeQueryBuilderTests extends AbstractQueryTestCase<GeoShapeQueryBuilder> {

    protected static String indexedShapeId;
    protected static String indexedShapePath;
    protected static String indexedShapeIndex;
    protected static String indexedShapeRouting;
    protected static Geometry indexedShapeToReturn;

    protected abstract String fieldName();

    @Override
    protected GeoShapeQueryBuilder doCreateTestQueryBuilder() {
        return doCreateTestQueryBuilder(randomBoolean());
    }

    abstract GeoShapeQueryBuilder doCreateTestQueryBuilder(boolean indexedShape);

    @Override
    protected GetResponse executeGet(GetRequest getRequest) {

        assertThat(indexedShapeToReturn, notNullValue());
        assertThat(indexedShapeId, notNullValue());
        assertThat(getRequest.id(), equalTo(indexedShapeId));
        assertThat(getRequest.routing(), equalTo(indexedShapeRouting));
        String expectedShapeIndex = indexedShapeIndex == null ? GeoShapeQueryBuilder.DEFAULT_SHAPE_INDEX_NAME : indexedShapeIndex;
        assertThat(getRequest.index(), equalTo(expectedShapeIndex));
        String expectedShapePath = indexedShapePath == null ? GeoShapeQueryBuilder.DEFAULT_SHAPE_FIELD_NAME : indexedShapePath;

        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.field(expectedShapePath, WellKnownText.toWKT(indexedShapeToReturn));
            builder.field(randomAlphaOfLengthBetween(10, 20), "something");
            builder.endObject();
            json = Strings.toString(builder);
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
        return new GetResponse(new GetResult(indexedShapeIndex, indexedShapeId, 0, 1, 0, true, new BytesArray(json),
            null, null));
    }

    @After
    public void clearShapeFields() {
        indexedShapeToReturn = null;
        indexedShapeId = null;
        indexedShapePath = null;
        indexedShapeIndex = null;
        indexedShapeRouting = null;
    }

    @Override
    protected void doAssertLuceneQuery(GeoShapeQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        // Logic for doToQuery is complex and is hard to test here. Need to rely
        // on Integration tests to determine if created query is correct
        // TODO improve GeoShapeQueryBuilder.doToQuery() method to make it
        // easier to test here
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(ConstantScoreQuery.class)));
    }

    public void testNoFieldName() throws Exception {
        Geometry geometry = GeometryTestUtils.randomGeometry(false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoShapeQueryBuilder(null, geometry));
        assertEquals("fieldName is required", e.getMessage());
    }

    public void testNoShape() {
        expectThrows(IllegalArgumentException.class, () -> new GeoShapeQueryBuilder(fieldName(), (Geometry) null));
    }

    public void testNoIndexedShape() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new GeoShapeQueryBuilder(fieldName(), null, null));
        assertEquals("either shape or indexedShapeId is required", e.getMessage());
    }

    public void testNoRelation() {
        Geometry shape = GeometryTestUtils.randomGeometry(false);
        GeoShapeQueryBuilder builder = new GeoShapeQueryBuilder(fieldName(), shape);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.relation(null));
        assertEquals("No Shape Relation defined", e.getMessage());
    }

    // see #3878
    public void testThatXContentSerializationInsideOfArrayWorks() throws Exception {
        Rectangle rectangle = new Rectangle(0, 10, 10, 0);
        GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery("searchGeometry", rectangle);
        JsonXContent.contentBuilder().startArray().value(geoQuery).endArray();
    }

    public void testFromJson() throws IOException {
        String json =
            "{\n" +
                "  \"geo_shape\" : {\n" +
                "    \"location\" : {\n" +
                "      \"shape\" : {\n" +
                "        \"type\" : \"Envelope\",\n" +
                "        \"coordinates\" : [ [ 13.0, 53.0 ], [ 14.0, 52.0 ] ]\n" +
                "      },\n" +
                "      \"relation\" : \"intersects\"\n" +
                "    },\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 42.0\n" +
                "  }\n" +
                "}";
        GeoShapeQueryBuilder parsed = (GeoShapeQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 42.0, parsed.boost(), 0.0001);
    }

    @Override
    public void testMustRewrite() throws IOException {
        GeoShapeQueryBuilder query = doCreateTestQueryBuilder(true);

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> query.toQuery(createSearchExecutionContext()));
        assertEquals("query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = rewriteAndFetch(query, createSearchExecutionContext());
        GeoShapeQueryBuilder geoShapeQueryBuilder = new GeoShapeQueryBuilder(fieldName(), indexedShapeToReturn);
        geoShapeQueryBuilder.strategy(query.strategy());
        geoShapeQueryBuilder.relation(query.relation());
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testMultipleRewrite() {
        GeoShapeQueryBuilder shape = doCreateTestQueryBuilder(true);
        QueryBuilder builder = new BoolQueryBuilder()
            .should(shape)
            .should(shape);

        builder = rewriteAndFetch(builder, createSearchExecutionContext());
        GeoShapeQueryBuilder expectedShape = new GeoShapeQueryBuilder(fieldName(), indexedShapeToReturn);
        expectedShape.strategy(shape.strategy());
        expectedShape.relation(shape.relation());
        QueryBuilder expected = new BoolQueryBuilder()
            .should(expectedShape)
            .should(expectedShape);
        assertEquals(expected, builder);
    }

    public void testIgnoreUnmapped() throws IOException {
        Geometry geometry = GeometryTestUtils.randomGeometry(false);
        final GeoShapeQueryBuilder queryBuilder = new GeoShapeQueryBuilder("unmapped", geometry);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createSearchExecutionContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoShapeQueryBuilder failingQueryBuilder = new GeoShapeQueryBuilder("unmapped", geometry);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("failed to find type for field [unmapped]"));
    }

    public void testWrongFieldType() {
        Geometry geometry = GeometryTestUtils.randomGeometry(false);
        final GeoShapeQueryBuilder queryBuilder = new GeoShapeQueryBuilder(TEXT_FIELD_NAME, geometry);
        QueryShardException e = expectThrows(QueryShardException.class, () -> queryBuilder.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("Field [mapped_string] is of unsupported type [text] for [geo_shape] query"));
    }

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = doCreateTestQueryBuilder(true);
        QueryBuilder queryBuilder = Rewriteable.rewrite(builder, createSearchExecutionContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> queryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createSearchExecutionContext());
        builder.writeTo(new BytesStreamOutput(10));
    }

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        assertThat(query, instanceOf(GeoShapeQueryBuilder.class));
        return query;
    }

    @Override
    protected Map<String, String> getObjectsHoldingArbitraryContent() {
        // shape field can accept any element but expects a type
       return Collections.singletonMap("shape", "Required [type]");
    }
}
