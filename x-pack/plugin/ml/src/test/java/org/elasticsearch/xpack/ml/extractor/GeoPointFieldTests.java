/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointFieldTests extends ESTestCase {

    public void testGivenGeoPoint() {
        double lat = 38.897676;
        double lon = -77.03653;
        String[] expected = new String[] {lat + "," + lon};
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
}
