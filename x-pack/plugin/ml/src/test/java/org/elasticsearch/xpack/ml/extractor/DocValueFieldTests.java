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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DocValueFieldTests extends ESTestCase {

    public void testKeyword() {
        SearchHit hit = new SearchHitBuilder(42).addField("a_keyword", "bar").build();

        ExtractedField field = new DocValueField("a_keyword", Collections.singleton("keyword"));

        assertThat(field.value(hit, new SourceSupplier(hit)), equalTo(new String[] { "bar" }));
        assertThat(field.getName(), equalTo("a_keyword"));
        assertThat(field.getSearchField(), equalTo("a_keyword"));
        assertThat(field.getTypes(), contains("keyword"));
        assertThat(field.getDocValueFormat(), is(nullValue()));
        assertThat(field.getMethod(), equalTo(ExtractedField.Method.DOC_VALUE));
        assertThat(field.supportsFromSource(), is(true));
        assertThat(field.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> field.getParentField());
    }

    public void testKeywordArray() {
        SearchHit hit = new SearchHitBuilder(42).addField("array", Arrays.asList("a", "b")).build();

        ExtractedField field = new DocValueField("array", Collections.singleton("keyword"));

        assertThat(field.value(hit, new SourceSupplier(hit)), equalTo(new String[] { "a", "b" }));
        assertThat(field.getName(), equalTo("array"));
        assertThat(field.getSearchField(), equalTo("array"));
        assertThat(field.getTypes(), contains("keyword"));
        assertThat(field.getDocValueFormat(), is(nullValue()));
        assertThat(field.getMethod(), equalTo(ExtractedField.Method.DOC_VALUE));
        assertThat(field.supportsFromSource(), is(true));
        assertThat(field.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> field.getParentField());

        ExtractedField missing = new DocValueField("missing", Collections.singleton("keyword"));
        assertThat(missing.value(hit, new SourceSupplier(hit)), equalTo(new Object[0]));
    }

    public void testMissing() {
        SearchHit hit = new SearchHitBuilder(42).addField("a_keyword", "bar").build();

        ExtractedField missing = new DocValueField("missing", Collections.singleton("keyword"));

        assertThat(missing.value(hit, new SourceSupplier(hit)), equalTo(new Object[0]));
    }

    public void testNewFromSource() {
        ExtractedField field = new DocValueField("foo", Collections.singleton("keyword"));

        ExtractedField fromSource = field.newFromSource();

        assertThat(fromSource.getName(), equalTo("foo"));
        assertThat(fromSource.getSearchField(), equalTo("foo"));
        assertThat(fromSource.getTypes(), contains("keyword"));
        expectThrows(UnsupportedOperationException.class, () -> fromSource.getDocValueFormat());
        assertThat(fromSource.getMethod(), equalTo(ExtractedField.Method.SOURCE));
        assertThat(fromSource.supportsFromSource(), is(true));
        assertThat(fromSource.isMultiField(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> fromSource.getParentField());
    }
}
