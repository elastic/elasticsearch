/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RuleScopeTests extends AbstractWireSerializingTestCase<RuleScope> {

    @Override
    protected RuleScope createTestInstance() {
        RuleScope.Builder scope = RuleScope.builder();
        int count = randomIntBetween(0, 3);
        for (int i = 0; i < count; ++i) {
            if (randomBoolean()) {
                scope.include(randomAlphaOfLength(20), randomAlphaOfLength(20));
            } else {
                scope.exclude(randomAlphaOfLength(20), randomAlphaOfLength(20));
            }
        }
        return scope.build();
    }

    @Override
    protected RuleScope mutateInstance(RuleScope instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<RuleScope> instanceReader() {
        return RuleScope::new;
    }

    public void testValidate_GivenEmpty() {
        RuleScope scope = RuleScope.builder().build();
        assertThat(scope.isEmpty(), is(true));

        scope.validate(Sets.newHashSet("a", "b"));
    }

    public void testValidate_GivenMultipleValidFields() {
        RuleScope scope = RuleScope.builder().include("foo", "filter1").exclude("bar", "filter2").include("foobar", "filter3").build();
        assertThat(scope.isEmpty(), is(false));

        scope.validate(Sets.newHashSet("foo", "bar", "foobar"));
    }

    public void testValidate_GivenNoAvailableFieldsForScope() {
        RuleScope scope = RuleScope.builder().include("foo", "filter1").build();
        assertThat(scope.isEmpty(), is(false));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> scope.validate(Collections.emptySet()));
        assertThat(
            e.getMessage(),
            equalTo("Invalid detector rule: scope field 'foo' is invalid; " + "detector has no available fields for scoping")
        );
    }

    public void testValidate_GivenMultipleFieldsIncludingInvalid() {
        RuleScope scope = RuleScope.builder().include("foo", "filter1").exclude("bar", "filter2").include("foobar", "filter3").build();
        assertThat(scope.isEmpty(), is(false));

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> scope.validate(new LinkedHashSet<>(Arrays.asList("foo", "foobar")))
        );
        assertThat(e.getMessage(), equalTo("Invalid detector rule: scope field 'bar' is invalid; select from [foo, foobar]"));
    }

    public void testGetReferencedFilters_GivenEmpty() {
        assertThat(RuleScope.builder().build().getReferencedFilters().isEmpty(), is(true));
    }

    public void testGetReferencedFilters_GivenMultipleFields() {
        RuleScope scope = RuleScope.builder().include("foo", "filter1").exclude("bar", "filter2").include("foobar", "filter3").build();
        assertThat(scope.getReferencedFilters(), contains("filter1", "filter2", "filter3"));
    }
}
