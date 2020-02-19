/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class ShapeQueryBuilderTests extends AbstractQueryTestCase<ShapeQueryBuilder> {

    protected static final String SHAPE_FIELD_NAME = "mapped_shape";

    private static String docType = "_doc";

    protected static String indexedShapeId;
    protected static String indexedShapePath;
    protected static String indexedShapeIndex;
    protected static String indexedShapeRouting;
    protected static Geometry indexedShapeToReturn;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(SpatialPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(docType, new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(
            fieldName(), "type=shape"))), MapperService.MergeReason.MAPPING_UPDATE);
    }

    protected String fieldName() {
        return SHAPE_FIELD_NAME;
    }

    @Override
    protected ShapeQueryBuilder doCreateTestQueryBuilder() {
        return doCreateTestQueryBuilder(randomBoolean());
    }

    protected ShapeQueryBuilder doCreateTestQueryBuilder(boolean indexedShape) {
        Geometry shape;
        // multipoint queries not (yet) supported
        do {
            shape = ShapeTestUtils.randomGeometry(false);
        } while (shape.type() == ShapeType.MULTIPOINT || shape.type() == ShapeType.GEOMETRYCOLLECTION);

        ShapeQueryBuilder builder;
        clearShapeFields();
        if (indexedShape == false) {
            builder = new ShapeQueryBuilder(fieldName(), shape);
        } else {
            indexedShapeToReturn = shape;
            indexedShapeId = randomAlphaOfLengthBetween(3, 20);
            builder = new ShapeQueryBuilder(fieldName(), indexedShapeId);
            if (randomBoolean()) {
                indexedShapeIndex = randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapeIndex(indexedShapeIndex);
            }
            if (randomBoolean()) {
                indexedShapePath = randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapePath(indexedShapePath);
            }
            if (randomBoolean()) {
                indexedShapeRouting = randomAlphaOfLengthBetween(3, 20);
                builder.indexedShapeRouting(indexedShapeRouting);
            }
        }

        if (shape.type() == ShapeType.LINESTRING || shape.type() == ShapeType.MULTILINESTRING) {
            builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS));
        } else {
            // XYShape does not support CONTAINS:
            builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.WITHIN));
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }
        return builder;
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
    protected void doAssertLuceneQuery(ShapeQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        // Logic for doToQuery is complex and is hard to test here. Need to rely
        // on Integration tests to determine if created query is correct
        // TODO improve ShapeQueryBuilder.doToQuery() method to make it
        // easier to test here
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(ConstantScoreQuery.class)));
    }

    public void testNoFieldName() {
        Geometry shape = ShapeTestUtils.randomGeometry(false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ShapeQueryBuilder(null, shape));
        assertEquals("fieldName is required", e.getMessage());
    }

    public void testNoShape() {
        expectThrows(IllegalArgumentException.class, () -> new ShapeQueryBuilder(fieldName(), (Geometry) null));
    }

    public void testNoIndexedShape() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new ShapeQueryBuilder(fieldName(), null, null));
        assertEquals("either shape or indexedShapeId is required", e.getMessage());
    }

    public void testNoRelation() {
        Geometry shape = ShapeTestUtils.randomGeometry(false);
        ShapeQueryBuilder builder = new ShapeQueryBuilder(fieldName(), shape);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.relation(null));
        assertEquals("No Shape Relation defined", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json =
            "{\n" +
                "  \"shape\" : {\n" +
                "    \"geometry\" : {\n" +
                "      \"shape\" : {\n" +
                "        \"type\" : \"envelope\",\n" +
                "        \"coordinates\" : [ [ 1300.0, 5300.0 ], [ 1400.0, 5200.0 ] ]\n" +
                "      },\n" +
                "      \"relation\" : \"intersects\"\n" +
                "    },\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 42.0\n" +
                "  }\n" +
                "}";
        ShapeQueryBuilder parsed = (ShapeQueryBuilder) parseQuery(json);
        checkGeneratedJson(json.replaceAll("envelope", "Envelope"), parsed);
        assertEquals(json, 42.0, parsed.boost(), 0.0001);
    }

    @Override
    public void testMustRewrite() {
        ShapeQueryBuilder query = doCreateTestQueryBuilder(true);

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> query.toQuery(createShardContext()));
        assertEquals("query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = rewriteAndFetch(query, createShardContext());
        ShapeQueryBuilder geoShapeQueryBuilder = new ShapeQueryBuilder(fieldName(), indexedShapeToReturn);
        geoShapeQueryBuilder.relation(query.relation());
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testMultipleRewrite() {
        ShapeQueryBuilder shape = doCreateTestQueryBuilder(true);
        QueryBuilder builder = new BoolQueryBuilder()
            .should(shape)
            .should(shape);

        builder = rewriteAndFetch(builder, createShardContext());

        ShapeQueryBuilder expectedShape = new ShapeQueryBuilder(fieldName(), indexedShapeToReturn);
        expectedShape.relation(shape.relation());
        QueryBuilder expected = new BoolQueryBuilder()
            .should(expectedShape)
            .should(expectedShape);
        assertEquals(expected, builder);
    }

    public void testIgnoreUnmapped() throws IOException {
        Geometry shape = ShapeTestUtils.randomGeometry(false);
        final ShapeQueryBuilder queryBuilder = new ShapeQueryBuilder("unmapped", shape);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final ShapeQueryBuilder failingQueryBuilder = new ShapeQueryBuilder("unmapped", shape);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("failed to find shape field [unmapped]"));
    }

    public void testWrongFieldType() {
        Geometry shape = ShapeTestUtils.randomGeometry(false);
        final ShapeQueryBuilder queryBuilder = new ShapeQueryBuilder(STRING_FIELD_NAME, shape);
        QueryShardException e = expectThrows(QueryShardException.class, () -> queryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("Field [mapped_string] is not of type [shape] but of type [text]"));
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
        assertThat(query, instanceOf(ShapeQueryBuilder.class));
        return query;
    }

    @Override
    protected GetResponse executeGet(GetRequest getRequest) {
        assertThat(indexedShapeToReturn, notNullValue());
        assertThat(indexedShapeId, notNullValue());
        assertThat(getRequest.id(), equalTo(indexedShapeId));
        assertThat(getRequest.routing(), equalTo(indexedShapeRouting));
        String expectedShapeIndex = indexedShapeIndex == null ? ShapeQueryBuilder.DEFAULT_SHAPE_INDEX_NAME : indexedShapeIndex;
        assertThat(getRequest.index(), equalTo(expectedShapeIndex));
        String expectedShapePath = indexedShapePath == null ? ShapeQueryBuilder.DEFAULT_SHAPE_FIELD_NAME : indexedShapePath;

        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.field(expectedShapePath, new ToXContentObject() {
                @Override
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    return GeoJson.toXContent(indexedShapeToReturn, builder, null);
                }
            });
            builder.field(randomAlphaOfLengthBetween(10, 20), "something");
            builder.endObject();
            json = Strings.toString(builder);
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
        return new GetResponse(new GetResult(indexedShapeIndex, indexedShapeId, 0, 1, 0, true, new BytesArray(json),
            null, null));
    }
}
