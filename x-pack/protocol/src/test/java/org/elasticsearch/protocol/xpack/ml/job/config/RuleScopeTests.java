/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.protocol.xpack.ml.job.config;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

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
    protected Writeable.Reader<RuleScope> instanceReader() {
        return RuleScope::new;
    }

    public void testValidate_GivenEmpty() {
        RuleScope scope = RuleScope.builder().build();
        assertThat(scope.isEmpty(), is(true));

        scope.validate(Sets.newHashSet("a", "b"));
    }

    public void testValidate_GivenMultipleValidFields() {
        RuleScope scope = RuleScope.builder()
                .include("foo", "filter1")
                .exclude("bar", "filter2")
                .include("foobar", "filter3")
                .build();
        assertThat(scope.isEmpty(), is(false));

        scope.validate(Sets.newHashSet("foo", "bar", "foobar"));
    }

    public void testValidate_GivenMultipleFieldsIncludingInvalid() {
        RuleScope scope = RuleScope.builder()
                .include("foo", "filter1")
                .exclude("bar", "filter2")
                .include("foobar", "filter3")
                .build();
        assertThat(scope.isEmpty(), is(false));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> scope.validate(Sets.newHashSet("foo", "foobar")));
        assertThat(e.getMessage(), equalTo("Invalid detector rule: scope field 'bar' is invalid; select from [foo, foobar]"));
    }

    public void testGetReferencedFilters_GivenEmpty() {
        assertThat(RuleScope.builder().build().getReferencedFilters().isEmpty(), is(true));
    }

    public void testGetReferencedFilters_GivenMultipleFields() {
        RuleScope scope = RuleScope.builder()
                .include("foo", "filter1")
                .exclude("bar", "filter2")
                .include("foobar", "filter3")
                .build();
        assertThat(scope.getReferencedFilters(), contains("filter1", "filter2", "filter3"));
    }
}
