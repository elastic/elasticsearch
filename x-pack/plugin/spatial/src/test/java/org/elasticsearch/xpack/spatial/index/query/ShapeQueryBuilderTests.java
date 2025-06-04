/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public abstract class ShapeQueryBuilderTests extends AbstractQueryTestCase<ShapeQueryBuilder> {

    protected static final String SHAPE_FIELD_NAME = "mapped_shape";

    protected static String docType = "_doc";

    protected static String indexedShapeId;
    protected static String indexedShapePath;
    protected static String indexedShapeIndex;
    protected static String indexedShapeRouting;
    protected static Geometry indexedShapeToReturn;

    protected abstract ShapeRelation getShapeRelation(ShapeType type);

    protected abstract Geometry getGeometry();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateSpatialPlugin.class);
    }

    protected String fieldName() {
        return SHAPE_FIELD_NAME;
    }

    @Override
    protected ShapeQueryBuilder doCreateTestQueryBuilder() {
        return doCreateTestQueryBuilder(randomBoolean());
    }

    protected ShapeQueryBuilder doCreateTestQueryBuilder(boolean indexedShape) {
        Geometry shape = getGeometry();

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

        if (randomBoolean()) {
            builder.relation(getShapeRelation(shape.type()));
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
    protected void doAssertLuceneQuery(ShapeQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        // Logic for doToQuery is complex and is hard to test here. Need to rely
        // on Integration tests to determine if created query is correct
        // TODO improve ShapeQueryBuilder.doToQuery() method to make it
        // easier to test here
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(ConstantScoreQuery.class)));
    }

    public void testNoFieldName() {
        Geometry shape = getGeometry();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ShapeQueryBuilder(null, shape));
        assertEquals("fieldName is required", e.getMessage());
    }

    public void testNoShape() {
        expectThrows(IllegalArgumentException.class, () -> new ShapeQueryBuilder(fieldName(), (Geometry) null));
    }

    public void testNoIndexedShape() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ShapeQueryBuilder(fieldName(), null, null));
        assertEquals("either shape or indexedShapeId is required", e.getMessage());
    }

    public void testNoRelation() {
        Geometry shape = getGeometry();
        ShapeQueryBuilder builder = new ShapeQueryBuilder(fieldName(), shape);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.relation(null));
        assertEquals("No Shape Relation defined", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "shape" : {
                "geometry" : {
                  "shape" : {
                    "type" : "envelope",
                    "coordinates" : [ [ 1300.0, 5300.0 ], [ 1400.0, 5200.0 ] ]
                  },
                  "relation" : "intersects"
                },
                "ignore_unmapped" : false,
                "boost" : 42.0
              }
            }""";
        ShapeQueryBuilder parsed = (ShapeQueryBuilder) parseQuery(json);
        checkGeneratedJson(json.replaceAll("envelope", "Envelope"), parsed);
        assertEquals(json, 42.0, parsed.boost(), 0.0001);
    }

    @Override
    public void testMustRewrite() {
        ShapeQueryBuilder query = doCreateTestQueryBuilder(true);

        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> query.toQuery(createSearchExecutionContext())
        );
        assertEquals("query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = rewriteAndFetch(query, createSearchExecutionContext());
        ShapeQueryBuilder geoShapeQueryBuilder = new ShapeQueryBuilder(fieldName(), indexedShapeToReturn);
        geoShapeQueryBuilder.relation(query.relation());
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testMultipleRewrite() {
        ShapeQueryBuilder shape = doCreateTestQueryBuilder(true);
        QueryBuilder builder = new BoolQueryBuilder().should(shape).should(shape);

        builder = rewriteAndFetch(builder, createSearchExecutionContext());

        ShapeQueryBuilder expectedShape = new ShapeQueryBuilder(fieldName(), indexedShapeToReturn);
        expectedShape.relation(shape.relation());
        QueryBuilder expected = new BoolQueryBuilder().should(expectedShape).should(expectedShape);
        assertEquals(expected, builder);
    }

    public void testIgnoreUnmapped() throws IOException {
        Geometry shape = getGeometry();
        final ShapeQueryBuilder queryBuilder = new ShapeQueryBuilder("unmapped", shape);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createSearchExecutionContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final ShapeQueryBuilder failingQueryBuilder = new ShapeQueryBuilder("unmapped", shape);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("failed to find type for field [unmapped]"));
    }

    public void testWrongFieldType() {
        Geometry shape = getGeometry();
        final ShapeQueryBuilder queryBuilder = new ShapeQueryBuilder(TEXT_FIELD_NAME, shape);
        QueryShardException e = expectThrows(QueryShardException.class, () -> queryBuilder.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("Field [mapped_string] is of unsupported type [text] for [shape] query"));
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
        return new GetResponse(new GetResult(indexedShapeIndex, indexedShapeId, 0, 1, 0, true, new BytesArray(json), null, null));
    }

    @Override
    protected Map<String, String> getObjectsHoldingArbitraryContent() {
        // shape field can accept any element but expects a type
        return Collections.singletonMap("shape", "Required [type]");
    }
}
