/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask;
import org.gradle.api.tasks.CacheableTask;

/**
 * Sub typed version of {@link StandaloneRestIntegTestTask}  that is used to differentiate between plain standalone
 * integ test tasks based on {@link StandaloneRestIntegTestTask} and
 * conventional configured tasks of {@link RestIntegTestTask}
 */
@CacheableTask
public class RestIntegTestTask extends StandaloneRestIntegTestTask {}
