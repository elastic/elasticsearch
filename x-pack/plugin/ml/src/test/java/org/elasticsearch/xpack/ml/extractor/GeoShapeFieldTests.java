/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class GeoShapeFieldTests extends ESTestCase {

    public void testObjectFormat() {
        double lat = 38.897676;
        double lon = -77.03653;
        String[] expected = new String[] { lat + "," + lon };

        SearchHit hit = new SearchHitBuilder(42).setSource("{\"geo\":{\"type\":\"point\", \"coordinates\": [" + lon + ", " + lat + "]}}")
            .build();

        ExtractedField geo = new GeoShapeField("geo");

        assertThat(geo.value(hit, new SourceSupplier(hit)), equalTo(expected));
        assertThat(geo.getName(), equalTo("geo"));
        assertThat(geo.getSearchField(), equalTo("geo"));
        assertThat(geo.getTypes(), contains("geo_shape"));
        assertThat(geo.getMethod(), equalTo(ExtractedField.Method.SOURCE));
        assertThat(geo.supportsFromSource(), is(true));
        assertThat(geo.newFromSource(), sameInstance(geo));
        expectThrows(UnsupportedOperationException.class, () -> geo.getDocValueFormat());
        assertThat(geo.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> geo.getParentField());
    }

    public void testWKTFormat() {
        double lat = 38.897676;
        double lon = -77.03653;
        String[] expected = new String[] { lat + "," + lon };

        SearchHit hit = new SearchHitBuilder(42).setSource("{\"geo\":\"POINT (" + lon + " " + lat + ")\"}").build();

        ExtractedField geo = new GeoShapeField("geo");

        assertThat(geo.value(hit, new SourceSupplier(hit)), equalTo(expected));
        assertThat(geo.getName(), equalTo("geo"));
        assertThat(geo.getSearchField(), equalTo("geo"));
        assertThat(geo.getTypes(), contains("geo_shape"));
        assertThat(geo.getMethod(), equalTo(ExtractedField.Method.SOURCE));
        assertThat(geo.supportsFromSource(), is(true));
        assertThat(geo.newFromSource(), sameInstance(geo));
        expectThrows(UnsupportedOperationException.class, () -> geo.getDocValueFormat());
        assertThat(geo.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> geo.getParentField());
    }

    public void testMissing() {
        SearchHit hit = new SearchHitBuilder(42).addField("a_keyword", "bar").build();

        ExtractedField geo = new GeoShapeField("missing");

        assertThat(geo.value(hit, new SourceSupplier(hit)), equalTo(new Object[0]));
    }

    public void testArray() {
        SearchHit hit = new SearchHitBuilder(42).setSource("{\"geo\":[1,2]}").build();

        ExtractedField geo = new GeoShapeField("geo");

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> geo.value(hit, new SourceSupplier(hit)));
        assertThat(e.getMessage(), equalTo("Unexpected values for a geo_shape field: [1, 2]"));
    }
}
