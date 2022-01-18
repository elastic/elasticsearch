/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.groups;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupOrJobLookupTests extends ESTestCase {

    public void testEmptyLookup_GivenAllowNoMatch() {
        GroupOrJobLookup lookup = new GroupOrJobLookup(Collections.emptyList());

        assertThat(lookup.expandJobIds("_all", true).isEmpty(), is(true));
        assertThat(lookup.expandJobIds("*", true).isEmpty(), is(true));
        assertThat(lookup.expandJobIds("foo*", true).isEmpty(), is(true));
        expectThrows(ResourceNotFoundException.class, () -> lookup.expandJobIds("foo", true));
    }

    public void testEmptyLookup_GivenNotAllowNoMatch() {
        GroupOrJobLookup lookup = new GroupOrJobLookup(Collections.emptyList());

        expectThrows(ResourceNotFoundException.class, () -> lookup.expandJobIds("_all", false));
        expectThrows(ResourceNotFoundException.class, () -> lookup.expandJobIds("*", false));
        expectThrows(ResourceNotFoundException.class, () -> lookup.expandJobIds("foo*", false));
        expectThrows(ResourceNotFoundException.class, () -> lookup.expandJobIds("foo", true));
    }

    public void testAllIsNotExpandedInCommaSeparatedExpression() {
        GroupOrJobLookup lookup = new GroupOrJobLookup(Collections.emptyList());
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> lookup.expandJobIds("foo-*,_all", true));
        assertThat(e.getMessage(), equalTo("No known job with id '_all'"));
    }

    public void testConstructor_GivenJobWithSameIdAsPreviousGroupName() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(mockJob("foo", Arrays.asList("foo-group")));
        jobs.add(mockJob("foo-group", Collections.emptyList()));
        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class, () -> new GroupOrJobLookup(jobs));
        assertThat(
            e.getMessage(),
            equalTo("job and group names must be unique but job [foo-group] and group [foo-group] have the same name")
        );
    }

    public void testConstructor_GivenGroupWithSameNameAsPreviousJobId() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(mockJob("foo", Collections.emptyList()));
        jobs.add(mockJob("foo-2", Arrays.asList("foo")));
        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class, () -> new GroupOrJobLookup(jobs));
        assertThat(e.getMessage(), equalTo("job and group names must be unique but job [foo] and group [foo] have the same name"));
    }

    public void testLookup() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(mockJob("foo-1", Arrays.asList("foo-group", "ones")));
        jobs.add(mockJob("foo-2", Arrays.asList("foo-group", "twos")));
        jobs.add(mockJob("bar-1", Arrays.asList("bar-group", "ones")));
        jobs.add(mockJob("bar-2", Arrays.asList("bar-group", "twos")));
        jobs.add(mockJob("nogroup", Collections.emptyList()));
        GroupOrJobLookup groupOrJobLookup = new GroupOrJobLookup(jobs);

        assertThat(groupOrJobLookup.expandJobIds("_all", false), contains("bar-1", "bar-2", "foo-1", "foo-2", "nogroup"));
        assertThat(groupOrJobLookup.expandJobIds("*", false), contains("bar-1", "bar-2", "foo-1", "foo-2", "nogroup"));
        assertThat(groupOrJobLookup.expandJobIds("bar-1", false), contains("bar-1"));
        assertThat(groupOrJobLookup.expandJobIds("foo-1", false), contains("foo-1"));
        assertThat(groupOrJobLookup.expandJobIds("foo-2, bar-1", false), contains("bar-1", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("foo-group", false), contains("foo-1", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("bar-group", false), contains("bar-1", "bar-2"));
        assertThat(groupOrJobLookup.expandJobIds("ones", false), contains("bar-1", "foo-1"));
        assertThat(groupOrJobLookup.expandJobIds("twos", false), contains("bar-2", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("foo-group, nogroup", false), contains("foo-1", "foo-2", "nogroup"));
        assertThat(groupOrJobLookup.expandJobIds("*-group", false), contains("bar-1", "bar-2", "foo-1", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("foo-group,foo-1,foo-2", false), contains("foo-1", "foo-2"));
        assertThat(groupOrJobLookup.expandJobIds("foo-group,*-2", false), contains("bar-2", "foo-1", "foo-2"));
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

    private static Job mockJob(String jobId, List<String> groups) {
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(jobId);
        when(job.getGroups()).thenReturn(groups);
        return job;
    }
}
