/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.geo.GeometryTestUtils.randomCircle;
import static org.elasticsearch.geo.GeometryTestUtils.randomGeometryCollection;
import static org.elasticsearch.geo.GeometryTestUtils.randomLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPoint;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPolygon;
import static org.elasticsearch.geo.GeometryTestUtils.randomPoint;
import static org.elasticsearch.geo.GeometryTestUtils.randomPolygon;
import static org.hamcrest.Matchers.equalTo;

public class GeoJsonSerializationTests extends ESTestCase {

    private static class GeometryWrapper implements ToXContentObject {

        private final Geometry geometry;

        GeometryWrapper(Geometry geometry) {
            this.geometry = geometry;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return GeoJson.toXContent(geometry, builder, params);
        }

        public static GeometryWrapper fromXContent(XContentParser parser) throws IOException {
            parser.nextToken();
            return new GeometryWrapper(GeoJson.fromXContent(GeographyValidator.instance(true), false, true, parser));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GeometryWrapper that = (GeometryWrapper) o;
            return Objects.equals(geometry, that.geometry);
        }

        @Override
        public int hashCode() {
            return Objects.hash(geometry);
        }
    }

    private void xContentTest(Supplier<Geometry> instanceSupplier) throws IOException {
        AbstractXContentTestCase.xContentTester(
            this::createParser,
            () -> new GeometryWrapper(instanceSupplier.get()),
            (geometryWrapper, xContentBuilder) -> {
                geometryWrapper.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            },
            GeometryWrapper::fromXContent
        ).supportsUnknownFields(true).test();
    }

    public void testPoint() throws IOException {
        xContentTest(() -> randomPoint(randomBoolean()));
    }

    public void testMultiPoint() throws IOException {
        xContentTest(() -> randomMultiPoint(randomBoolean()));
    }

    public void testLineString() throws IOException {
        xContentTest(() -> randomLine(randomBoolean()));
    }

    public void testMultiLineString() throws IOException {
        xContentTest(() -> randomMultiLine(randomBoolean()));
    }

    public void testPolygon() throws IOException {
        xContentTest(() -> randomPolygon(randomBoolean()));
    }

    public void testMultiPolygon() throws IOException {
        xContentTest(() -> randomMultiPolygon(randomBoolean()));
    }

    public void testEnvelope() throws IOException {
        xContentTest(GeometryTestUtils::randomRectangle);
    }

    public void testGeometryCollection() throws IOException {
        xContentTest(() -> randomGeometryCollection(randomBoolean()));
    }

    public void testCircle() throws IOException {
        xContentTest(() -> randomCircle(randomBoolean()));
    }

    public void testToMap() throws IOException {
        for (int i = 0; i < 10; i++) {
            Geometry geometry = GeometryTestUtils.randomGeometry(randomBoolean());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            GeoJson.toXContent(geometry, builder, ToXContent.EMPTY_PARAMS);
            StreamInput input = BytesReference.bytes(builder).streamInput();

            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, input)
            ) {
                Map<String, Object> map = GeoJson.toMap(geometry);
                assertThat(parser.map(), equalTo(map));
            }
        }
    }
}
