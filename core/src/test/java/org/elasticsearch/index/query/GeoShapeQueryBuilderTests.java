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

import com.vividsolutions.jts.geom.Coordinate;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;
import org.junit.After;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class GeoShapeQueryBuilderTests extends AbstractQueryTestCase<GeoShapeQueryBuilder> {

    private static String indexedShapeId;
    private static String indexedShapeType;
    private static String indexedShapePath;
    private static String indexedShapeIndex;
    private static ShapeBuilder indexedShapeToReturn;

    @Override
    protected GeoShapeQueryBuilder doCreateTestQueryBuilder() {
        ShapeType shapeType = ShapeType.randomType(random());
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(random(), null, shapeType);

        GeoShapeQueryBuilder builder;
        clearShapeFields();
        if (randomBoolean()) {
            builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        } else {
            indexedShapeToReturn = shape;
            indexedShapeId = randomAsciiOfLengthBetween(3, 20);
            indexedShapeType = randomAsciiOfLengthBetween(3, 20);
            builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, indexedShapeId, indexedShapeType);
            if (randomBoolean()) {
                indexedShapeIndex = randomAsciiOfLengthBetween(3, 20);
                builder.indexedShapeIndex(indexedShapeIndex);
            }
            if (randomBoolean()) {
                indexedShapePath = randomAsciiOfLengthBetween(3, 20);
                builder.indexedShapePath(indexedShapePath);
            }
        }
        if (randomBoolean()) {
            SpatialStrategy strategy = randomFrom(SpatialStrategy.values());
            // ShapeType.MULTILINESTRING + SpatialStrategy.TERM can lead to large queries and will slow down tests, so
            // we try to avoid that combination
            while (shapeType == ShapeType.MULTILINESTRING && strategy == SpatialStrategy.TERM) {
                strategy = randomFrom(SpatialStrategy.values());
            }
            builder.strategy(strategy);
            if (strategy != SpatialStrategy.TERM) {
                builder.relation(randomFrom(ShapeRelation.values()));
            }
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }
        return builder;
    }

    @Override
    protected GetResponse executeGet(GetRequest getRequest) {
        assertThat(indexedShapeToReturn, notNullValue());
        assertThat(indexedShapeId, notNullValue());
        assertThat(indexedShapeType, notNullValue());
        assertThat(getRequest.id(), equalTo(indexedShapeId));
        assertThat(getRequest.type(), equalTo(indexedShapeType));
        String expectedShapeIndex = indexedShapeIndex == null ? GeoShapeQueryBuilder.DEFAULT_SHAPE_INDEX_NAME : indexedShapeIndex;
        assertThat(getRequest.index(), equalTo(expectedShapeIndex));
        String expectedShapePath = indexedShapePath == null ? GeoShapeQueryBuilder.DEFAULT_SHAPE_FIELD_NAME : indexedShapePath;
        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.field(expectedShapePath, indexedShapeToReturn);
            builder.endObject();
            json = builder.string();
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
        return new GetResponse(new GetResult(indexedShapeIndex, indexedShapeType, indexedShapeId, 0, true, new BytesArray(json), null));
    }

    @After
    public void clearShapeFields() {
        indexedShapeToReturn = null;
        indexedShapeId = null;
        indexedShapeType = null;
        indexedShapePath = null;
        indexedShapeIndex = null;
    }

    @Override
    protected void doAssertLuceneQuery(GeoShapeQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        // Logic for doToQuery is complex and is hard to test here. Need to rely
        // on Integration tests to determine if created query is correct
        // TODO improve GeoShapeQueryBuilder.doToQuery() method to make it
        // easier to test here
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(ConstantScoreQuery.class)));
    }

    /**
     * Overridden here to ensure the test is only run if at least one type is
     * present in the mappings. Geo queries do not execute if the field is not
     * explicitly mapped
     */
    @Override
    public void testToQuery() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        super.testToQuery();
    }

    public void testNoFieldName() throws Exception {
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(random(), null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoShapeQueryBuilder(null, shape));
        assertEquals("fieldName is required", e.getMessage());
    }

    public void testNoShape() throws IOException {
        expectThrows(IllegalArgumentException.class, () -> new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, null));
    }

    public void testNoIndexedShape() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, null, "type"));
        assertEquals("either shapeBytes or indexedShapeId and indexedShapeType are required", e.getMessage());
    }

    public void testNoIndexedShapeType() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, "id", null));
        assertEquals("indexedShapeType is required if indexedShapeId is specified", e.getMessage());
    }

    public void testNoRelation() throws IOException {
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(random(), null);
        GeoShapeQueryBuilder builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.relation(null));
        assertEquals("No Shape Relation defined", e.getMessage());
    }

    public void testInvalidRelation() throws IOException {
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(random(), null);
        GeoShapeQueryBuilder builder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        builder.strategy(SpatialStrategy.TERM);
        expectThrows(IllegalArgumentException.class, () -> builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.WITHIN)));
        GeoShapeQueryBuilder builder2 = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        builder2.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.WITHIN));
        expectThrows(IllegalArgumentException.class, () -> builder2.strategy(SpatialStrategy.TERM));
        GeoShapeQueryBuilder builder3 = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, shape);
        builder3.strategy(SpatialStrategy.TERM);
        expectThrows(IllegalArgumentException.class, () -> builder3.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.WITHIN)));
    }

    // see #3878
    public void testThatXContentSerializationInsideOfArrayWorks() throws Exception {
        EnvelopeBuilder envelopeBuilder = ShapeBuilders.newEnvelope(new Coordinate(0, 0), new Coordinate(10, 10));
        GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery("searchGeometry", envelopeBuilder);
        JsonXContent.contentBuilder().startArray().value(geoQuery).endArray();
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"geo_shape\" : {\n" +
                "    \"location\" : {\n" +
                "      \"shape\" : {\n" +
                "        \"type\" : \"envelope\",\n" +
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
        GeoShapeQueryBuilder sqb;
        do {
            sqb = doCreateTestQueryBuilder();
            // do this until we get one without a shape
        } while (sqb.shape() != null);

        GeoShapeQueryBuilder query = sqb;

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> query.toQuery(createShardContext()));
        assertEquals("query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = query.rewrite(createShardContext());
        GeoShapeQueryBuilder geoShapeQueryBuilder = new GeoShapeQueryBuilder(GEO_SHAPE_FIELD_NAME, indexedShapeToReturn);
        geoShapeQueryBuilder.strategy(query.strategy());
        geoShapeQueryBuilder.relation(query.relation());
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testIgnoreUnmapped() throws IOException {
        ShapeType shapeType = ShapeType.randomType(random());
        ShapeBuilder shape = RandomShapeGenerator.createShapeWithin(random(), null, shapeType);
        final GeoShapeQueryBuilder queryBuilder = new GeoShapeQueryBuilder("unmapped", shape);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoShapeQueryBuilder failingQueryBuilder = new GeoShapeQueryBuilder("unmapped", shape);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("failed to find geo_shape field [unmapped]"));
    }
}
