/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SelectorResolverTests extends ESTestCase {

    public void testSplitSelectorExpression() {
        expectThrows(NullPointerException.class, () -> SelectorResolver.parseSelectorExpression(null));
        assertThat(SelectorResolver.parseSelectorExpression(""), is(equalTo(new Tuple<>("", null))));
        assertThat(SelectorResolver.parseSelectorExpression("a"), is(equalTo(new Tuple<>("a", null))));
        assertThat(SelectorResolver.parseSelectorExpression("*"), is(equalTo(new Tuple<>("*", null))));
        assertThat(SelectorResolver.parseSelectorExpression("index"), is(equalTo(new Tuple<>("index", null))));
        assertThat(SelectorResolver.parseSelectorExpression("cluster:index"), is(equalTo(new Tuple<>("cluster:index", null))));
        assertThat(SelectorResolver.parseSelectorExpression("*:index"), is(equalTo(new Tuple<>("*:index", null))));
        assertThat(SelectorResolver.parseSelectorExpression("cluster:*"), is(equalTo(new Tuple<>("cluster:*", null))));
        assertThat(SelectorResolver.parseSelectorExpression("*:*"), is(equalTo(new Tuple<>("*:*", null))));
        assertThat(SelectorResolver.parseSelectorExpression("*:*:*"), is(equalTo(new Tuple<>("*:*:*", null))));

        assertThat(SelectorResolver.parseSelectorExpression("a::data"), is(equalTo(new Tuple<>("a", IndexComponentSelector.DATA))));
        assertThat(SelectorResolver.parseSelectorExpression("a::failures"), is(equalTo(new Tuple<>("a", IndexComponentSelector.FAILURES))));
        assertThat(SelectorResolver.parseSelectorExpression("a::*"), is(equalTo(new Tuple<>("a", IndexComponentSelector.ALL_APPLICABLE))));
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.parseSelectorExpression("a::random"));
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.parseSelectorExpression("a::d*ta"));
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.parseSelectorExpression("a::*ailures"));
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.parseSelectorExpression("a::"));
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.parseSelectorExpression("a::**"));
        expectThrows(InvalidIndexNameException.class, () -> SelectorResolver.parseSelectorExpression("index::data::*"));
        assertThat(SelectorResolver.parseSelectorExpression("::*"), is(equalTo(new Tuple<>("", IndexComponentSelector.ALL_APPLICABLE))));
    }
}
