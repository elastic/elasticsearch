/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.groups;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NameResolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * A lookup that allows expanding expressions that may consist of job
 * IDs, job group names, wildcard patterns or a comma separated combination
 * of the aforementioned to the matching job IDs.
 * The lookup is immutable.
 */
public class GroupOrJobLookup {

    private final SortedMap<String, GroupOrJob> groupOrJobLookup;

    public GroupOrJobLookup(Collection<Job> jobs) {
        groupOrJobLookup = new TreeMap<>();
        jobs.forEach(this::put);
    }

    private void put(Job job) {
        if (groupOrJobLookup.containsKey(job.getId())) {
            throw new ResourceAlreadyExistsException(Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, job.getId()));
        }
        groupOrJobLookup.put(job.getId(), new GroupOrJob.SingleJob(job));
        for (String groupName : job.getGroups()) {
            GroupOrJob oldGroup = groupOrJobLookup.get(groupName);
            if (oldGroup == null) {
                groupOrJobLookup.put(groupName, new GroupOrJob.Group(Collections.singletonList(job)));
            } else {
                if (oldGroup.isGroup() == false) {
                    throw new ResourceAlreadyExistsException(Messages.getMessage(Messages.JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE, groupName));
                }
                List<Job> groupJobs = new ArrayList<>(oldGroup.jobs());
                groupJobs.add(job);
                groupOrJobLookup.put(groupName, new GroupOrJob.Group(groupJobs));
            }
        }
    }

    public Set<String> expandJobIds(String expression, boolean allowNoMatch) {
        return new GroupOrJobResolver().expand(expression, allowNoMatch);
    }

    public boolean isGroupOrJob(String id) {
        return groupOrJobLookup.containsKey(id);
    }

    private class GroupOrJobResolver extends NameResolver {

        private GroupOrJobResolver() {
            super(ExceptionsHelper::missingJobException);
        }

        @Override
        protected Set<String> keys() {
            return groupOrJobLookup.keySet();
        }

        @Override
        protected Set<String> nameSet() {
            return groupOrJobLookup.values().stream()
                    .filter(groupOrJob -> groupOrJob.isGroup() == false)
                    .map(groupOrJob -> groupOrJob.jobs().get(0).getId())
                    .collect(Collectors.toSet());
        }

        @Override
        protected List<String> lookup(String key) {
            GroupOrJob groupOrJob = groupOrJobLookup.get(key);
            return groupOrJob == null ? Collections.emptyList() : groupOrJob.jobs().stream().map(Job::getId).collect(Collectors.toList());
        }
    }
}
