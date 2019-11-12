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
import static org.hamcrest.Matchers.startsWith;

public class TimeFieldTests extends ESTestCase {

    public void testDocValueWithStringValue() {
        long millis = randomLong();
        SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", Long.toString(millis)).build();

        ExtractedField timeField = new TimeField("time", ExtractedField.Method.DOC_VALUE);

        assertThat(timeField.value(hit), equalTo(new Object[] { millis }));
        assertThat(timeField.getName(), equalTo("time"));
        assertThat(timeField.getSearchField(), equalTo("time"));
        assertThat(timeField.getTypes(), contains("date"));
        assertThat(timeField.getMethod(), equalTo(ExtractedField.Method.DOC_VALUE));
        assertThat(timeField.getDocValueFormat(), equalTo("epoch_millis"));
        assertThat(timeField.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> timeField.newFromSource());
        assertThat(timeField.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> timeField.getParentField());
    }

    public void testScriptWithLongValue() {
        long millis = randomLong();
        SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", millis).build();

        ExtractedField timeField = new TimeField("time", ExtractedField.Method.SCRIPT_FIELD);

        assertThat(timeField.value(hit), equalTo(new Object[] { millis }));
        assertThat(timeField.getName(), equalTo("time"));
        assertThat(timeField.getSearchField(), equalTo("time"));
        assertThat(timeField.getTypes(), contains("date"));
        assertThat(timeField.getMethod(), equalTo(ExtractedField.Method.SCRIPT_FIELD));
        expectThrows(UnsupportedOperationException.class, () -> timeField.getDocValueFormat());
        assertThat(timeField.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> timeField.newFromSource());
        assertThat(timeField.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> timeField.getParentField());
    }

    public void testUnknownFormat() {
        final SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", new Object()).build();

        final ExtractedField timeField = new TimeField("time", ExtractedField.Method.DOC_VALUE);

        assertThat(expectThrows(IllegalStateException.class, () -> timeField.value(hit)).getMessage(),
            startsWith("Unexpected value for a time field"));
    }

    public void testSourceNotSupported() {
        expectThrows(IllegalArgumentException.class, () -> new TimeField("foo", ExtractedField.Method.SOURCE));
    }
}
