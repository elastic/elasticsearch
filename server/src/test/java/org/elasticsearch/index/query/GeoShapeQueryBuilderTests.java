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
package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;
import org.junit.After;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

public abstract class GeoShapeQueryBuilderTests extends AbstractQueryTestCase<GeoShapeQueryBuilder> {

    protected static String indexedShapeId;
    protected static String indexedShapePath;
    protected static String indexedShapeIndex;
    protected static String indexedShapeRouting;
    protected static ShapeBuilder<?, ?, ?> indexedShapeToReturn;

    protected abstract String fieldName();

    @Override
    protected Settings createTestIndexSettings() {
        // force the new shape impl
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        return Settings.builder()
                .put(super.createTestIndexSettings())
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build();
    }

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
            builder.field(expectedShapePath, indexedShapeToReturn);
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
    protected void doAssertLuceneQuery(GeoShapeQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        // Logic for doToQuery is complex and is hard to test here. Need to rely
        // on Integration tests to determine if created query is correct
        // TODO improve GeoShapeQueryBuilder.doToQuery() method to make it
        // easier to test here
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(ConstantScoreQuery.class)));
    }

    public void testNoFieldName() throws Exception {
        ShapeBuilder<?, ?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoShapeQueryBuilder(null, shape));
        assertEquals("fieldName is required", e.getMessage());
    }

    public void testNoShape() throws IOException {
        expectThrows(IllegalArgumentException.class, () -> new GeoShapeQueryBuilder(fieldName(), (ShapeBuilder) null));
    }

    public void testNoIndexedShape() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new GeoShapeQueryBuilder(fieldName(), null, null));
        assertEquals("either shape or indexedShapeId is required", e.getMessage());
    }

    public void testNoRelation() throws IOException {
        ShapeBuilder<?, ?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null);
        GeoShapeQueryBuilder builder = new GeoShapeQueryBuilder(fieldName(), shape);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.relation(null));
        assertEquals("No Shape Relation defined", e.getMessage());
    }

    // see #3878
    public void testThatXContentSerializationInsideOfArrayWorks() throws Exception {
        EnvelopeBuilder envelopeBuilder = new EnvelopeBuilder(new Coordinate(0, 10), new Coordinate(10, 0));
        GeoShapeQueryBuilder geoQuery = randomBoolean() ?
            QueryBuilders.geoShapeQuery("searchGeometry", envelopeBuilder) :
            QueryBuilders.geoShapeQuery("searchGeometry", envelopeBuilder.buildGeometry());
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

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> query.toQuery(createShardContext()));
        assertEquals("query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = rewriteAndFetch(query, createShardContext());
        GeoShapeQueryBuilder geoShapeQueryBuilder = randomBoolean() ?
            new GeoShapeQueryBuilder(fieldName(), indexedShapeToReturn) :
            new GeoShapeQueryBuilder(fieldName(), indexedShapeToReturn.buildGeometry());
        geoShapeQueryBuilder.strategy(query.strategy());
        geoShapeQueryBuilder.relation(query.relation());
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testMultipleRewrite() throws IOException {
        GeoShapeQueryBuilder shape = doCreateTestQueryBuilder(true);
        QueryBuilder builder = new BoolQueryBuilder()
            .should(shape)
            .should(shape);

        builder = rewriteAndFetch(builder, createShardContext());

        GeoShapeQueryBuilder expectedShape = randomBoolean() ?
            new GeoShapeQueryBuilder(fieldName(), indexedShapeToReturn) :
            new GeoShapeQueryBuilder(fieldName(), indexedShapeToReturn.buildGeometry());
        expectedShape.strategy(shape.strategy());
        expectedShape.relation(shape.relation());
        QueryBuilder expected = new BoolQueryBuilder()
            .should(expectedShape)
            .should(expectedShape);
        assertEquals(expected, builder);
    }

    public void testIgnoreUnmapped() throws IOException {
        ShapeType shapeType = ShapeType.randomType(random());
        ShapeBuilder<?, ?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null, shapeType);
        final GeoShapeQueryBuilder queryBuilder = randomBoolean() ?
            new GeoShapeQueryBuilder("unmapped", shape) :
            new GeoShapeQueryBuilder("unmapped", shape.buildGeometry());
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoShapeQueryBuilder failingQueryBuilder = randomBoolean() ?
            new GeoShapeQueryBuilder("unmapped", shape) :
            new GeoShapeQueryBuilder("unmapped", shape.buildGeometry());
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), matchesPattern("failed to find .*geo_shape.* field \\[unmapped\\]"));
    }

    public void testWrongFieldType() throws IOException {
        ShapeType shapeType = ShapeType.randomType(random());
        ShapeBuilder<?, ?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null, shapeType);
        final GeoShapeQueryBuilder queryBuilder = randomBoolean() ?
            new GeoShapeQueryBuilder(TEXT_FIELD_NAME, shape) :
            new GeoShapeQueryBuilder(TEXT_FIELD_NAME, shape.buildGeometry());
        QueryShardException e = expectThrows(QueryShardException.class, () -> queryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), matchesPattern("Field \\[mapped_string\\] is of unsupported type \\[text\\]." +
            " \\[geo_shape\\] query supports the following types \\[.*geo_shape.*\\]"));
    }

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = doCreateTestQueryBuilder(true);
        QueryBuilder queryBuilder = Rewriteable.rewrite(builder, createShardContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> queryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createShardContext());
        builder.writeTo(new BytesStreamOutput(10));
    }

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        assertThat(query, instanceOf(GeoShapeQueryBuilder.class));
        return query;
    }
}
