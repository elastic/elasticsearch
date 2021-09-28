/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun;

import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.testing.Test;

import javax.inject.Inject;

/**
 * Allows configuring test rerun mechanics.
 * <p>
 * This extension is added with the name 'rerun' to all {@link Test} tasks.
 */
public class TestRerunTaskExtension {

    /**
     * The default number of reruns we allow for a test task.
     */
    public static final Integer DEFAULT_MAX_RERUNS = 1;

    /**
     * The name of the extension added to each test task.
     */
    public static String NAME = "rerun";

    private final Property<Integer> maxReruns;

    private final Property<Boolean> didRerun;

    @Inject
    public TestRerunTaskExtension(ObjectFactory objects) {
        this.maxReruns = objects.property(Integer.class).convention(DEFAULT_MAX_RERUNS);
        this.didRerun = objects.property(Boolean.class).convention(Boolean.FALSE);
    }

    /**
     * The maximum number of times to rerun all tests.
     * <p>
     * This setting defaults to {@code 0}, which results in no retries.
     * Any value less than 1 disables rerunning.
     *
     * @return the maximum number of times to rerun all tests of a task
     */
    public Property<Integer> getMaxReruns() {
        return maxReruns;
    }

    /**
     /**
     * @return whether tests tests have been rerun or not. Defaults to false.
     */
    public Property<Boolean> getDidRerun() {
        return didRerun;
    }

}
