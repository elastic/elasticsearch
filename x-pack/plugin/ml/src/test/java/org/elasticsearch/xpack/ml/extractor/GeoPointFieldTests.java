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

import java.util.Arrays;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointFieldTests extends ESTestCase {

    public void testGivenGeoPoint() {
        double lat = 38.897676;
        double lon = -77.03653;
        String[] expected = new String[] { lat + "," + lon };
        SearchHit hit = new SearchHitBuilder(42).addField("geo", lat + ", " + lon).build();

        // doc_value field
        ExtractedField geo = new GeoPointField("geo");

        assertThat(geo.value(hit), equalTo(expected));
        assertThat(geo.getName(), equalTo("geo"));
        assertThat(geo.getSearchField(), equalTo("geo"));
        assertThat(geo.getMethod(), equalTo(ExtractedField.Method.DOC_VALUE));
        assertThat(geo.getTypes(), contains("geo_point"));
        assertThat(geo.getDocValueFormat(), is(nullValue()));
        assertThat(geo.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> geo.newFromSource());
        assertThat(geo.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> geo.getParentField());
    }

    public void testMissing() {
        SearchHit hit = new SearchHitBuilder(42).addField("a_keyword", "bar").build();

        ExtractedField geo = new GeoPointField("missing");

        assertThat(geo.value(hit), equalTo(new Object[0]));
    }

    public void testArray() {
        SearchHit hit = new SearchHitBuilder(42).addField("geo", Arrays.asList(1, 2)).build();

        ExtractedField geo = new GeoPointField("geo");

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> geo.value(hit));
        assertThat(e.getMessage(), equalTo("Unexpected values for a geo_point field: [1, 2]"));
    }
}
