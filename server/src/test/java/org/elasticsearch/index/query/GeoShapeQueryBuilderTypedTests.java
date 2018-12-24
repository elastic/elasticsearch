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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;
import org.junit.After;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class GeoShapeQueryBuilderTypedTests extends AbstractQueryTestCase<GeoShapeQueryBuilder> {

    protected static String indexedShapeId;
    protected static String indexedShapeType;
    protected static String indexedShapePath;
    protected static String indexedShapeIndex;
    protected static String indexedShapeRouting;
    protected static ShapeBuilder<?, ?> indexedShapeToReturn;

    protected String fieldName() {
        return GEO_SHAPE_FIELD_NAME;
    }

    @Override
    protected Settings createTestIndexSettings() {
        // force the new shape impl
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_6_0, Version.CURRENT);
        return Settings.builder()
                .put(super.createTestIndexSettings())
                .put(IndexMetaData.SETTING_VERSION_CREATED, version)
                .build();
    }

    @Override
    protected GeoShapeQueryBuilder doCreateTestQueryBuilder() {
        // LatLonShape does not support MultiPoint queries
        ShapeType shapeType = randomFrom(ShapeType.POINT, ShapeType.LINESTRING, ShapeType.MULTILINESTRING, ShapeType.POLYGON);
        ShapeBuilder<?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null, shapeType);
        GeoShapeQueryBuilder builder;
        clearShapeFields();

        indexedShapeToReturn = shape;
        indexedShapeId = randomAlphaOfLengthBetween(3, 20);
        indexedShapeType = randomAlphaOfLengthBetween(3, 20);
        builder = new GeoShapeQueryBuilder(fieldName(), indexedShapeId, indexedShapeType);
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

        if (randomBoolean()) {
            if (shapeType == ShapeType.LINESTRING || shapeType == ShapeType.MULTILINESTRING) {
                builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS));
            } else {
                // LatLonShape does not support CONTAINS:
                builder.relation(randomFrom(ShapeRelation.DISJOINT, ShapeRelation.INTERSECTS, ShapeRelation.WITHIN));
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
        return new GetResponse(new GetResult(indexedShapeIndex, indexedShapeType, indexedShapeId, 0, 1, 0, true, new BytesArray(json),
            null));
    }

    @After
    public void clearShapeFields() {
        indexedShapeToReturn = null;
        indexedShapeId = null;
        indexedShapeType = null;
        indexedShapePath = null;
        indexedShapeIndex = null;
        indexedShapeRouting = null;
    }

    @Override
    protected void doAssertLuceneQuery(GeoShapeQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        // Logic for doToQuery is complex and is hard to test here. Need to rely
        // on Integration tests to determine if created query is correct
        // TODO improve GeoShapeQueryBuilder.doToQuery() method to make it
        // easier to test here
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(ConstantScoreQuery.class)));
    }

    @Override
    public void testMustRewrite() throws IOException {
        GeoShapeQueryBuilder query = doCreateTestQueryBuilder();

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> query.toQuery(createShardContext()));
        assertEquals("query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = rewriteAndFetch(query, createShardContext());
        GeoShapeQueryBuilder geoShapeQueryBuilder = new GeoShapeQueryBuilder(fieldName(), indexedShapeToReturn);
        geoShapeQueryBuilder.strategy(query.strategy());
        geoShapeQueryBuilder.relation(query.relation());
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testMultipleRewrite() throws IOException {
        GeoShapeQueryBuilder shape = doCreateTestQueryBuilder();
        QueryBuilder builder = new BoolQueryBuilder()
            .should(shape)
            .should(shape);

        builder = rewriteAndFetch(builder, createShardContext());

        GeoShapeQueryBuilder expectedShape = new GeoShapeQueryBuilder(fieldName(), indexedShapeToReturn);
        expectedShape.strategy(shape.strategy());
        expectedShape.relation(shape.relation());
        QueryBuilder expected = new BoolQueryBuilder()
            .should(expectedShape)
            .should(expectedShape);
        assertEquals(expected, builder);
    }

    public void testIgnoreUnmapped() throws IOException {
        ShapeType shapeType = ShapeType.randomType(random());
        ShapeBuilder<?, ?> shape = RandomShapeGenerator.createShapeWithin(random(), null, shapeType);
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

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = doCreateTestQueryBuilder();
        QueryBuilder queryBuilder = Rewriteable.rewrite(builder, createShardContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> queryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createShardContext());
        builder.writeTo(new BytesStreamOutput(10));
    }

    @Override
    public void testFromXContent() throws IOException {
        super.testFromXContent();
        assertWarnings(QueryShardContext.TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public void testUnknownField() throws IOException {
        super.testUnknownField();
        assertWarnings(QueryShardContext.TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public void testValidOutput() throws IOException {
        super.testValidOutput();
        assertWarnings(QueryShardContext.TYPES_DEPRECATION_MESSAGE);
    }
}
