/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MultiFieldTests extends ESTestCase {

    public void testGivenSameSearchField() {
        SearchHit hit = new SearchHitBuilder(42).addField("a", 1).addField("a.b", 2).build();

        ExtractedField wrapped = new DocValueField("a.b", Collections.singleton("integer"));
        ExtractedField field = new MultiField("a", wrapped);

        assertThat(field.value(hit), equalTo(new Integer[] { 2 }));
        assertThat(field.getName(), equalTo("a.b"));
        assertThat(field.getSearchField(), equalTo("a.b"));
        assertThat(field.getMethod(), equalTo(ExtractedField.Method.DOC_VALUE));
        assertThat(field.isMultiField(), is(true));
        assertThat(field.getParentField(), equalTo("a"));
        assertThat(field.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> field.newFromSource());
    }

    public void testGivenDifferentSearchField() {
        SearchHit hit = new SearchHitBuilder(42).addField("a", 1).addField("a.b", 2).build();

        ExtractedField wrapped = new DocValueField("a", Collections.singleton("integer"));
        ExtractedField field = new MultiField("a.b", "a", "a", wrapped);

        assertThat(field.value(hit), equalTo(new Integer[] { 1 }));
        assertThat(field.getName(), equalTo("a.b"));
        assertThat(field.getSearchField(), equalTo("a"));
        assertThat(field.getMethod(), equalTo(ExtractedField.Method.DOC_VALUE));
        assertThat(field.isMultiField(), is(true));
        assertThat(field.getParentField(), equalTo("a"));
        assertThat(field.supportsFromSource(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> field.newFromSource());
    }
}
