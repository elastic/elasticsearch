/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.time.Instant;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class TimeFieldTests extends ESTestCase {

    public void testDocValueWithWholeMillisecondStringValue() {
        long millis = randomNonNegativeLong();
        Instant time = Instant.ofEpochMilli(millis);
        DateFormatter formatter = DateFormatter.forPattern("epoch_millis");
        String timeAsString = formatter.format(time);
        SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", timeAsString).build();

        ExtractedField timeField = new TimeField("time", ExtractedField.Method.DOC_VALUE);

        assertThat(timeField.value(hit), equalTo(new Object[] { millis }));
        assertThat(timeField.getName(), equalTo("time"));
        assertThat(timeField.getSearchField(), equalTo("time"));
        assertThat(timeField.getTypes(), containsInAnyOrder("date", "date_nanos"));
        assertThat(timeField.getMethod(), equalTo(ExtractedField.Method.DOC_VALUE));
        assertThat(timeField.getDocValueFormat(), equalTo("epoch_millis"));
        assertThat(timeField.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, timeField::newFromSource);
        assertThat(timeField.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, timeField::getParentField);
    }

    public void testDocValueWithFractionalMillisecondStringValue() {
        long millis = randomNonNegativeLong();
        int extraNanos = randomIntBetween(1, 999999);
        Instant time = Instant.ofEpochMilli(millis).plusNanos(extraNanos);
        DateFormatter formatter = DateFormatter.forPattern("epoch_millis");
        String timeAsString = formatter.format(time);
        SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", timeAsString).build();

        ExtractedField timeField = new TimeField("time", ExtractedField.Method.DOC_VALUE);

        assertThat(timeField.value(hit), equalTo(new Object[] { millis }));
        assertThat(timeField.getName(), equalTo("time"));
        assertThat(timeField.getSearchField(), equalTo("time"));
        assertThat(timeField.getTypes(), containsInAnyOrder("date", "date_nanos"));
        assertThat(timeField.getMethod(), equalTo(ExtractedField.Method.DOC_VALUE));
        assertThat(timeField.getDocValueFormat(), equalTo("epoch_millis"));
        assertThat(timeField.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, timeField::newFromSource);
        assertThat(timeField.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, timeField::getParentField);
    }

    public void testScriptWithLongValue() {
        long millis = randomLong();
        SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", millis).build();

        ExtractedField timeField = new TimeField("time", ExtractedField.Method.SCRIPT_FIELD);

        assertThat(timeField.value(hit), equalTo(new Object[] { millis }));
        assertThat(timeField.getName(), equalTo("time"));
        assertThat(timeField.getSearchField(), equalTo("time"));
        assertThat(timeField.getTypes(), containsInAnyOrder("date", "date_nanos"));
        assertThat(timeField.getMethod(), equalTo(ExtractedField.Method.SCRIPT_FIELD));
        expectThrows(UnsupportedOperationException.class, timeField::getDocValueFormat);
        assertThat(timeField.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, timeField::newFromSource);
        assertThat(timeField.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, timeField::getParentField);
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
