/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.enrich.MatchProcessorTests.str;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoMatchProcessorTests extends ESTestCase {

    public void testBasics() {
        // point
        Point expectedPoint = new Point(-122.084110, 37.386637);
        testBasicsForFieldValue(Map.of("lat", 37.386637, "lon", -122.084110), expectedPoint);
        testBasicsForFieldValue("37.386637, -122.084110", expectedPoint);
        testBasicsForFieldValue("POINT (-122.084110 37.386637)", expectedPoint);
        testBasicsForFieldValue(List.of(-122.084110, 37.386637), expectedPoint);
        testBasicsForFieldValue(Map.of("type", "Point", "coordinates", List.of(-122.084110, 37.386637)), expectedPoint);
        // line
        Line expectedLine = new Line(new double[] { 0, 1 }, new double[] { 0, 1 });
        testBasicsForFieldValue("LINESTRING(0 0, 1 1)", expectedLine);
        testBasicsForFieldValue(Map.of("type", "LineString", "coordinates", List.of(List.of(0, 0), List.of(1, 1))), expectedLine);
        // polygon
        Polygon expectedPolygon = new Polygon(new LinearRing(new double[] { 0, 1, 1, 0, 0 }, new double[] { 0, 0, 1, 1, 0 }));
        testBasicsForFieldValue("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", expectedPolygon);
        testBasicsForFieldValue(
            Map.of(
                "type",
                "Polygon",
                "coordinates",
                List.of(List.of(List.of(0, 0), List.of(1, 0), List.of(1, 1), List.of(0, 1), List.of(0, 0)))
            ),
            expectedPolygon
        );
        // geometry collection
        testBasicsForFieldValue(
            List.of(
                List.of(-122.084110, 37.386637),
                "37.386637, -122.084110",
                "POINT (-122.084110 37.386637)",
                Map.of("type", "Point", "coordinates", List.of(-122.084110, 37.386637)),
                Map.of("type", "LineString", "coordinates", List.of(List.of(0, 0), List.of(1, 1))),
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
            ),
            new GeometryCollection<>(List.of(expectedPoint, expectedPoint, expectedPoint, expectedPoint, expectedLine, expectedPolygon))
        );
        testBasicsForFieldValue("not a point", null);
    }

    private void testBasicsForFieldValue(Object fieldValue, Geometry expectedGeometry) {
        int maxMatches = randomIntBetween(1, 8);
        MockSearchFunction mockSearch = mockedSearchFunction(Map.of("key", Map.of("shape", "object", "zipcode", 94040)));
        GeoMatchProcessor processor = new GeoMatchProcessor(
            "_tag",
            null,
            mockSearch,
            "_name",
            str("location"),
            str("entry"),
            false,
            false,
            "shape",
            maxMatches,
            ShapeRelation.INTERSECTS,
            ShapeBuilder.Orientation.CCW
        );
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_id",
            "_routing",
            1L,
            VersionType.INTERNAL,
            Map.of("location", fieldValue)
        );
        // Run
        IngestDocument[] holder = new IngestDocument[1];
        processor.execute(ingestDocument, (result, e) -> holder[0] = result);
        if (expectedGeometry == null) {
            assertThat(holder[0], nullValue());
            return;
        } else {
            assertThat(holder[0], notNullValue());
        }
        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.indices().length, equalTo(1));
        assertThat(request.indices()[0], equalTo(".enrich-_name"));
        assertThat(request.preference(), equalTo(Preference.LOCAL.type()));
        assertThat(request.source().size(), equalTo(maxMatches));
        assertThat(request.source().trackScores(), equalTo(false));
        assertThat(request.source().fetchSource().fetchSource(), equalTo(true));
        assertThat(request.source().fetchSource().excludes(), emptyArray());
        assertThat(request.source().fetchSource().includes(), emptyArray());
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(GeoShapeQueryBuilder.class));
        GeoShapeQueryBuilder shapeQueryBuilder = (GeoShapeQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(shapeQueryBuilder.fieldName(), equalTo("shape"));
        assertThat(shapeQueryBuilder.shape(), equalTo(expectedGeometry));

        // Check result
        Map<?, ?> entry;
        if (maxMatches == 1) {
            entry = ingestDocument.getFieldValue("entry", Map.class);
        } else {
            List<?> entries = ingestDocument.getFieldValue("entry", List.class);
            entry = (Map<?, ?>) entries.get(0);
        }
        assertThat(entry.size(), equalTo(2));
        assertThat(entry.get("zipcode"), equalTo(94040));

    }

    private static final class MockSearchFunction implements BiConsumer<SearchRequest, BiConsumer<SearchResponse, Exception>> {
        private final SearchResponse mockResponse;
        private final SetOnce<SearchRequest> capturedRequest;
        private final Exception exception;

        MockSearchFunction(SearchResponse mockResponse) {
            this.mockResponse = mockResponse;
            this.exception = null;
            this.capturedRequest = new SetOnce<>();
        }

        MockSearchFunction(Exception exception) {
            this.mockResponse = null;
            this.exception = exception;
            this.capturedRequest = new SetOnce<>();
        }

        @Override
        public void accept(SearchRequest request, BiConsumer<SearchResponse, Exception> handler) {
            capturedRequest.set(request);
            if (exception != null) {
                handler.accept(null, exception);
            } else {
                handler.accept(mockResponse, null);
            }
        }

        SearchRequest getCapturedRequest() {
            return capturedRequest.get();
        }
    }

    public MockSearchFunction mockedSearchFunction() {
        return new MockSearchFunction(mockResponse(Collections.emptyMap()));
    }

    public MockSearchFunction mockedSearchFunction(Exception exception) {
        return new MockSearchFunction(exception);
    }

    public MockSearchFunction mockedSearchFunction(Map<String, Map<String, ?>> documents) {
        return new MockSearchFunction(mockResponse(documents));
    }

    public SearchResponse mockResponse(Map<String, Map<String, ?>> documents) {
        SearchHit[] searchHits = documents.entrySet().stream().map(e -> {
            SearchHit searchHit = new SearchHit(randomInt(100), e.getKey(), Collections.emptyMap(), Collections.emptyMap());
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.SMILE.xContent())) {
                builder.map(e.getValue());
                builder.flush();
                ByteArrayOutputStream outputStream = (ByteArrayOutputStream) builder.getOutputStream();
                searchHit.sourceRef(new BytesArray(outputStream.toByteArray()));
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
            return searchHit;
        }).toArray(SearchHit[]::new);
        return new SearchResponse(
            new SearchResponseSections(
                new SearchHits(searchHits, new TotalHits(documents.size(), TotalHits.Relation.EQUAL_TO), 1.0f),
                new Aggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                false,
                false,
                null,
                1
            ),
            null,
            1,
            1,
            0,
            1,
            ShardSearchFailure.EMPTY_ARRAY,
            new SearchResponse.Clusters(1, 1, 0)
        );
    }
}
