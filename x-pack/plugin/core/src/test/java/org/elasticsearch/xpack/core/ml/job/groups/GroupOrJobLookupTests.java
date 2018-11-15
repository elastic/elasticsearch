/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.groups;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupOrJobLookupTests extends ESTestCase {

    public void testEmptyLookup() {
        GroupOrJobLookup lookup = new GroupOrJobLookup(Collections.emptyList());

        assertThat(lookup.expandJobIds("_all").isEmpty(), is(true));
        assertThat(lookup.expandJobIds("*").isEmpty(), is(true));
        assertThat(lookup.expandJobIds("foo*").isEmpty(), is(true));
        assertThat(lookup.expandJobIds("foo").isEmpty(), is(true));
    }

    public void testAllIsNotExpandedInCommaSeparatedExpression() {
        GroupOrJobLookup lookup = new GroupOrJobLookup(Collections.emptyList());
        assertThat(lookup.expandJobIds("foo*,_all").isEmpty(), is(true));
    }

    public void testConstructor_GivenJobWithSameIdAsPreviousGroupName() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(mockJob("foo", Arrays.asList("foo-group")));
        jobs.add(mockJob("foo-group", Collections.emptyList()));
        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class, () -> new GroupOrJobLookup(jobs));
        assertThat(e.getMessage(),
                equalTo("job and group names must be unique but job [foo-group] and group [foo-group] have the same name"));
    }

    public void testConstructor_GivenGroupWithSameNameAsPreviousJobId() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(mockJob("foo", Collections.emptyList()));
        jobs.add(mockJob("foo-2", Arrays.asList("foo")));
        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class, () -> new GroupOrJobLookup(jobs));
        assertThat(e.getMessage(),
                equalTo("job and group names must be unique but job [foo] and group [foo] have the same name"));
    }

    public void testLookup() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(mockJob("foo-1", Arrays.asList("foo-group", "ones")));
        jobs.add(mockJob("foo-2", Arrays.asList("foo-group", "twos")));
        jobs.add(mockJob("bar-1", Arrays.asList("bar-group", "ones")));
        jobs.add(mockJob("bar-2", Arrays.asList("bar-group", "twos")));
        jobs.add(mockJob("nogroup", Collections.emptyList()));
        GroupOrJobLookup groupOrJobLookup = new GroupOrJobLookup(jobs);

        assertThat(groupOrJobLookup.expandJobIds("_all"), contains("bar-1", "bar-2", "foo-1", "foo-2", "nogroup"));
        assertThat(groupOrJobLookup.expandJobIds("*"), contains("bar-1", "bar-2", "foo-1", "foo-2", "nogroup"));
        assertThat(groupOrJobLookup.expandJobIds("bar-1"), contains("bar-1"));
        assertThat(groupOrJobLookup.expandJobIds("foo-1"), contains("foo-1"));
        assertThat(groupOrJobLookup.expandJobIds("foo-2, bar-1"), contains("bar-1", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("foo-group"), contains("foo-1", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("bar-group"), contains("bar-1", "bar-2"));
        assertThat(groupOrJobLookup.expandJobIds("ones"), contains("bar-1", "foo-1"));
        assertThat(groupOrJobLookup.expandJobIds("twos"), contains("bar-2", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("foo-group, nogroup"), contains("foo-1", "foo-2", "nogroup"));
        assertThat(groupOrJobLookup.expandJobIds("*-group"), contains("bar-1", "bar-2", "foo-1", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("foo-group,foo-1,foo-2"), contains("foo-1", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("foo-group,*-2"), contains("bar-2", "foo-1", "foo-2"));
    }

    public void testIsGroupOrJob() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(mockJob("foo-1", Arrays.asList("foo-group", "ones")));
        jobs.add(mockJob("foo-2", Arrays.asList("foo-group", "twos")));
        jobs.add(mockJob("bar-1", Arrays.asList("bar-group", "ones")));
        jobs.add(mockJob("nogroup", Collections.emptyList()));
        GroupOrJobLookup groupOrJobLookup = new GroupOrJobLookup(jobs);

        assertTrue(groupOrJobLookup.isGroupOrJob("foo-1"));
        assertTrue(groupOrJobLookup.isGroupOrJob("twos"));
        assertTrue(groupOrJobLookup.isGroupOrJob("nogroup"));
        assertFalse(groupOrJobLookup.isGroupOrJob("missing"));
    }

    public void testExpandGroupIds() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(mockJob("foo-1", Arrays.asList("foo-group")));
        jobs.add(mockJob("foo-2", Arrays.asList("foo-group")));
        jobs.add(mockJob("bar-1", Arrays.asList("bar-group")));
        jobs.add(mockJob("nogroup", Collections.emptyList()));

        GroupOrJobLookup groupOrJobLookup = new GroupOrJobLookup(jobs);
        assertThat(groupOrJobLookup.expandGroupIds("foo*"), contains("foo-group"));
        assertThat(groupOrJobLookup.expandGroupIds("bar-group,nogroup"), contains("bar-group"));
        assertThat(groupOrJobLookup.expandGroupIds("*"), contains("bar-group", "foo-group"));
        assertThat(groupOrJobLookup.expandGroupIds("foo-group"), contains("foo-group"));
        assertThat(groupOrJobLookup.expandGroupIds("no-group"), empty());
    }

    private static Job mockJob(String jobId, List<String> groups) {
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(jobId);
        when(job.getGroups()).thenReturn(groups);
        return job;
    }
}
