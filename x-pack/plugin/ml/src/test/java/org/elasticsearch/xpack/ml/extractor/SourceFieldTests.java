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

import java.util.Collections;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class SourceFieldTests extends ESTestCase {

    public void testSingleValue() {
        SearchHit hit = new SearchHitBuilder(42).setSource("{\"single\":\"bar\"}").build();

        ExtractedField field = new SourceField("single", Collections.singleton("text"));

        assertThat(field.value(hit), equalTo(new String[] { "bar" }));
        assertThat(field.getName(), equalTo("single"));
        assertThat(field.getSearchField(), equalTo("single"));
        assertThat(field.getTypes(), contains("text"));
        assertThat(field.getMethod(), equalTo(ExtractedField.Method.SOURCE));
        assertThat(field.supportsFromSource(), is(true));
        assertThat(field.newFromSource(), sameInstance(field));
        assertThat(field.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> field.getParentField());
        expectThrows(UnsupportedOperationException.class, () -> field.getDocValueFormat());
    }

    public void testArray() {
        SearchHit hit = new SearchHitBuilder(42).setSource("{\"array\":[\"a\",\"b\"]}").build();

        ExtractedField field = new SourceField("array", Collections.singleton("text"));

        assertThat(field.value(hit), equalTo(new String[] { "a", "b" }));
        assertThat(field.getName(), equalTo("array"));
        assertThat(field.getSearchField(), equalTo("array"));
        assertThat(field.getTypes(), contains("text"));
        assertThat(field.getMethod(), equalTo(ExtractedField.Method.SOURCE));
        assertThat(field.supportsFromSource(), is(true));
        assertThat(field.newFromSource(), sameInstance(field));
        assertThat(field.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> field.getParentField());
        expectThrows(UnsupportedOperationException.class, () -> field.getDocValueFormat());
    }

    public void testMissing() {
        SearchHit hit = new SearchHitBuilder(42).setSource("{\"array\":[\"a\",\"b\"]}").build();

        ExtractedField missing = new SourceField("missing", Collections.singleton("text"));

        assertThat(missing.value(hit), equalTo(new Object[0]));
    }

    public void testValueGivenNested() {
        SearchHit hit = new SearchHitBuilder(42).setSource("{\"level_1\":{\"level_2\":{\"foo\":\"bar\"}}}").build();

        ExtractedField nested = new SourceField("level_1.level_2.foo", Collections.singleton("text"));

        assertThat(nested.value(hit), equalTo(new String[] { "bar" }));
    }

    public void testValueGivenNestedArray() {
        SearchHit hit = new SearchHitBuilder(42).setSource("{\"level_1\":{\"level_2\":[{\"foo\":\"bar\"}]}}").build();

        ExtractedField nested = new SourceField("level_1.level_2.foo", Collections.singleton("text"));

        assertThat(nested.value(hit), equalTo(new String[] { "bar" }));
    }
}
