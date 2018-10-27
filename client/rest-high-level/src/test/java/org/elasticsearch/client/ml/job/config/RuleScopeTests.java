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
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.contains;

public class RuleScopeTests extends AbstractXContentTestCase<RuleScope> {

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

    public void testGetReferencedFilters_GivenEmpty() {
        assertTrue(RuleScope.builder().build().getReferencedFilters().isEmpty());
    }

    public void testGetReferencedFilters_GivenMultipleFields() {
        RuleScope scope = RuleScope.builder()
                .include("foo", "filter1")
                .exclude("bar", "filter2")
                .include("foobar", "filter3")
                .build();
        assertThat(scope.getReferencedFilters(), contains("filter1", "filter2", "filter3"));
    }

    @Override
    protected RuleScope doParseInstance(XContentParser parser) throws IOException {
        return RuleScope.parser().parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
