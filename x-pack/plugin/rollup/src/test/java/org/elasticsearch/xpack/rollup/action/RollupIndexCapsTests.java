/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class RollupIndexCapsTests extends ESTestCase {

    public void testSetEmptyJobs() {
        Exception e = expectThrows(NullPointerException.class, () -> new RollupIndexCaps(ESTestCase.randomAlphaOfLength(10), null));
        assertThat(e.getMessage(), equalTo("List of Rollup Jobs cannot be null"));

        RollupIndexCaps caps = new RollupIndexCaps(ESTestCase.randomAlphaOfLength(10), Collections.emptyList());
        assertFalse(caps.hasCaps());
    }

    public void testGetAllJobs() {
        List<RollupJobConfig> jobs = new ArrayList<>(2);
        jobs.add(ConfigTestHelpers.randomRollupJobConfig(random(), "foo"));
        jobs.add(ConfigTestHelpers.randomRollupJobConfig(random(), "bar"));
        RollupIndexCaps caps = new RollupIndexCaps(ESTestCase.randomAlphaOfLength(10), jobs);
        assertTrue(caps.hasCaps());

        List<String> jobCaps = caps.getJobCapsByIndexPattern(Metadata.ALL).stream()
                .map(RollupJobCaps::getJobID)
                .collect(Collectors.toList());
        assertThat(jobCaps.size(), equalTo(2));
        assertTrue(jobCaps.contains("foo"));
        assertTrue(jobCaps.contains("bar"));
    }

    public void testFilterGetJobs() {
        List<RollupJobConfig> jobs = new ArrayList<>(2);
        jobs.add(ConfigTestHelpers.randomRollupJobConfig(random(), "foo", "foo_index_pattern"));
        jobs.add(ConfigTestHelpers.randomRollupJobConfig(random(), "bar"));
        RollupIndexCaps caps = new RollupIndexCaps(ESTestCase.randomAlphaOfLength(10), jobs);
        assertTrue(caps.hasCaps());

        List<String> jobCaps = caps.getJobCapsByIndexPattern("foo_index_pattern").stream()
                .map(RollupJobCaps::getJobID)
                .collect(Collectors.toList());
        assertThat(jobCaps.size(), equalTo(1));
        assertTrue(jobCaps.contains("foo"));
        assertFalse(jobCaps.contains("bar"));
    }
}
