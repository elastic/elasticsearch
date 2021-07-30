/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
