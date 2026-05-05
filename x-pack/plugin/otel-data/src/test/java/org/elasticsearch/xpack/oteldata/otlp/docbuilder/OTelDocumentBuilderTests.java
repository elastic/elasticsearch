/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class OTelDocumentBuilderTests extends ESTestCase {

    private final TestDocumentBuilder documentBuilder = new TestDocumentBuilder();

    public void testBuildAttributesWithGeoPoints() throws IOException {
        ObjectPath doc = buildDocumentWithGeoPoints(
            List.of(
                keyValue("geo.location.lon", 1.1),
                keyValue("geo.location.lat", 2.2),
                keyValue("foo.bar.geo.location.lon", 3.3),
                keyValue("foo.bar.geo.location.lat", 4.4),
                keyValue("a.geo.location.lon", 5.5),
                keyValue("b.geo.location.lat", 6.6),
                keyValue("unrelatedgeo.location.lon", 7.7),
                keyValue("unrelatedgeo.location.lat", 8.8),
                keyValue("d", 9.9),
                keyValue("e.geo.location.lon", "foo"),
                keyValue("e.geo.location.lat", "bar")
            )
        );

        assertThat(doc.evaluate("attributes.geo\\.location"), equalTo(List.of(1.1, 2.2)));
        assertThat(doc.evaluate("attributes.foo\\.bar\\.geo\\.location"), equalTo(List.of(3.3, 4.4)));
        assertThat(doc.evaluate("attributes.a\\.geo\\.location\\.lon"), equalTo(5.5));
        assertThat(doc.evaluate("attributes.b\\.geo\\.location\\.lat"), equalTo(6.6));
        assertThat(doc.evaluate("attributes.unrelatedgeo\\.location\\.lon"), equalTo(7.7));
        assertThat(doc.evaluate("attributes.unrelatedgeo\\.location\\.lat"), equalTo(8.8));
        assertThat(doc.evaluate("attributes.d"), equalTo(9.9));
        assertThat(doc.evaluate("attributes.e\\.geo\\.location\\.lon"), equalTo("foo"));
        assertThat(doc.evaluate("attributes.e\\.geo\\.location\\.lat"), equalTo("bar"));
    }

    public void testBuildAttributesDoesNotMergeGeoPointsByDefault() throws IOException {
        ObjectPath doc = buildDocument(List.of(keyValue("geo.location.lon", 1.1), keyValue("geo.location.lat", 2.2)));

        assertThat(doc.evaluate("attributes.geo\\.location"), nullValue());
        assertThat(doc.evaluate("attributes.geo\\.location\\.lon"), equalTo(1.1));
        assertThat(doc.evaluate("attributes.geo\\.location\\.lat"), equalTo(2.2));
    }

    private ObjectPath buildDocument(List<KeyValue> attributes) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        documentBuilder.buildAttributes(builder, attributes, 0);
        builder.endObject();
        return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
    }

    private ObjectPath buildDocumentWithGeoPoints(List<KeyValue> attributes) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        documentBuilder.buildAttributesWithGeoPoints(builder, attributes, 0);
        builder.endObject();
        return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
    }

    private static class TestDocumentBuilder extends OTelDocumentBuilder {
        TestDocumentBuilder() {
            super(new BufferedByteStringAccessor());
        }
    }
}
