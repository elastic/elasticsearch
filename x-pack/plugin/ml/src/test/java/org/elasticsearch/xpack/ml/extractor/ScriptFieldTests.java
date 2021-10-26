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
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ScriptFieldTests extends ESTestCase {

    public void testKeyword() {
        SearchHit hit = new SearchHitBuilder(42).addField("a_keyword", "bar").build();

        ExtractedField field = new ScriptField("a_keyword");

        assertThat(field.value(hit), equalTo(new String[] { "bar" }));
        assertThat(field.getName(), equalTo("a_keyword"));
        assertThat(field.getSearchField(), equalTo("a_keyword"));
        assertThat(field.getTypes().isEmpty(), is(true));
        expectThrows(UnsupportedOperationException.class, () -> field.getDocValueFormat());
        assertThat(field.getMethod(), equalTo(ExtractedField.Method.SCRIPT_FIELD));
        expectThrows(UnsupportedOperationException.class, () -> field.getParentField());
        assertThat(field.isMultiField(), is(false));
        assertThat(field.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> field.newFromSource());
    }

    public void testKeywordArray() {
        SearchHit hit = new SearchHitBuilder(42).addField("array", Arrays.asList("a", "b")).build();

        ExtractedField field = new ScriptField("array");

        assertThat(field.value(hit), equalTo(new String[] { "a", "b" }));
        assertThat(field.getName(), equalTo("array"));
        assertThat(field.getSearchField(), equalTo("array"));
        assertThat(field.getTypes().isEmpty(), is(true));
        expectThrows(UnsupportedOperationException.class, () -> field.getDocValueFormat());
        assertThat(field.getMethod(), equalTo(ExtractedField.Method.SCRIPT_FIELD));
        expectThrows(UnsupportedOperationException.class, () -> field.getParentField());
        assertThat(field.isMultiField(), is(false));
        assertThat(field.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> field.newFromSource());

        ExtractedField missing = new DocValueField("missing", Collections.singleton("keyword"));
        assertThat(missing.value(hit), equalTo(new Object[0]));
    }

    public void testMissing() {
        SearchHit hit = new SearchHitBuilder(42).addField("a_keyword", "bar").build();

        ExtractedField missing = new ScriptField("missing");

        assertThat(missing.value(hit), equalTo(new Object[0]));
    }
}
