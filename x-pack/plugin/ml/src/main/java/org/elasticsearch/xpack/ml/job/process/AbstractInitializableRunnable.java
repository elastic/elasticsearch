/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;

/**
 * Abstract runnable that has an `init` function that can be called when passed to an executor
 */
public abstract class AbstractInitializableRunnable extends AbstractRunnable {
    public abstract void init();
}
