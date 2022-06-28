/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.groups;

import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An interface to represent either a job or a group of jobs
 */
interface GroupOrJob {

    boolean isGroup();

    List<Job> jobs();

    final class Group implements GroupOrJob {

        private final List<Job> jobs;

        Group(List<Job> jobs) {
            this.jobs = Collections.unmodifiableList(jobs);
        }

        @Override
        public boolean isGroup() {
            return true;
        }

        @Override
        public List<Job> jobs() {
            return jobs;
        }
    }

    final class SingleJob implements GroupOrJob {

        private final Job job;

        SingleJob(Job job) {
            this.job = Objects.requireNonNull(job);
        }

        @Override
        public boolean isGroup() {
            return false;
        }

        @Override
        public List<Job> jobs() {
            return Collections.singletonList(job);
        }
    }
}
