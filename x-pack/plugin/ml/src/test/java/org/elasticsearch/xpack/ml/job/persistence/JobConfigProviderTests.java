/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isOneOf;

public class JobConfigProviderTests extends ESTestCase {

    public void testMatchingJobIds() {
        LinkedList<JobConfigProvider.IdMatcher> requiredMatches = JobConfigProvider.requiredMatches(new String[] {"*"}, false);
        assertThat(requiredMatches, hasSize(1));
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.singletonList("foo"));
        assertThat(requiredMatches, empty());

        requiredMatches = JobConfigProvider.requiredMatches(JobConfigProvider.tokenizeExpression(""), false);
        assertThat(requiredMatches, hasSize(1));
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.singletonList("foo"));
        assertThat(requiredMatches, empty());

        requiredMatches = JobConfigProvider.requiredMatches(JobConfigProvider.tokenizeExpression(null), false);
        assertThat(requiredMatches, hasSize(1));
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.singletonList("foo"));
        assertThat(requiredMatches, empty());

        requiredMatches = JobConfigProvider.requiredMatches(JobConfigProvider.tokenizeExpression(null), false);
        assertThat(requiredMatches, hasSize(1));
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.emptyList());
        assertThat(requiredMatches, hasSize(1));
        assertThat(requiredMatches.get(0).getId(), equalTo("*"));

        requiredMatches = JobConfigProvider.requiredMatches(JobConfigProvider.tokenizeExpression("_all"), false);
        assertThat(requiredMatches, hasSize(1));
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.singletonList("foo"));
        assertThat(requiredMatches, empty());

        requiredMatches = JobConfigProvider.requiredMatches(new String[] {"foo*"}, false);
        assertThat(requiredMatches, hasSize(1));
        JobConfigProvider.filterMatchedIds(requiredMatches, Arrays.asList("foo1","foo2"));
        assertThat(requiredMatches, empty());

        requiredMatches = JobConfigProvider.requiredMatches(new String[] {"foo*","bar"}, false);
        assertThat(requiredMatches, hasSize(2));
        JobConfigProvider.filterMatchedIds(requiredMatches, Arrays.asList("foo1","foo2"));
        assertThat(requiredMatches, hasSize(1));
        assertEquals("bar", requiredMatches.get(0).getId());

        requiredMatches = JobConfigProvider.requiredMatches(new String[] {"foo*","bar"}, false);
        assertThat(requiredMatches, hasSize(2));
        JobConfigProvider.filterMatchedIds(requiredMatches, Arrays.asList("foo1","bar"));
        assertThat(requiredMatches, empty());

        requiredMatches = JobConfigProvider.requiredMatches(new String[] {"foo*","bar"}, false);
        assertThat(requiredMatches, hasSize(2));
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.singletonList("bar"));
        assertThat(requiredMatches, hasSize(1));
        assertEquals("foo*", requiredMatches.get(0).getId());

        requiredMatches = JobConfigProvider.requiredMatches(JobConfigProvider.tokenizeExpression("foo,bar,baz,wild*"), false);
        assertThat(requiredMatches, hasSize(4));
        JobConfigProvider.filterMatchedIds(requiredMatches, Arrays.asList("foo","baz"));
        assertThat(requiredMatches, hasSize(2));
        assertThat(requiredMatches.get(0).getId(), isOneOf("bar", "wild*"));
        assertThat(requiredMatches.get(1).getId(), isOneOf("bar", "wild*"));
    }

    public void testMatchingJobIds_allowNoJobs() {
        // wildcard all with allow no jobs
        LinkedList<JobConfigProvider.IdMatcher> requiredMatches = JobConfigProvider.requiredMatches(new String[] {"*"}, true);
        assertThat(requiredMatches, empty());
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.emptyList());
        assertThat(requiredMatches, empty());

        requiredMatches = JobConfigProvider.requiredMatches(new String[] {"foo*","bar"}, true);
        assertThat(requiredMatches, hasSize(1));
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.singletonList("bar"));
        assertThat(requiredMatches, empty());

        requiredMatches = JobConfigProvider.requiredMatches(new String[] {"foo*","bar"}, true);
        assertThat(requiredMatches, hasSize(1));
        JobConfigProvider.filterMatchedIds(requiredMatches, Collections.emptyList());
        assertThat(requiredMatches, hasSize(1));
        assertEquals("bar", requiredMatches.get(0).getId());
    }
}
