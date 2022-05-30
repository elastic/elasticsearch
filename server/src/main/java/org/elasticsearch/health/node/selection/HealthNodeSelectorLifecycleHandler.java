/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;

import java.io.IOException;

/**
 * This handler ensures that the {@link HealthNodeSelector} is bootstrapped on a clean cluster upon node startup and
 * that it is released when the selected health node shuts down.
 */
public class HealthNodeSelectorLifecycleHandler extends AbstractLifecycleComponent {

    private final HealthNodeSelectorTaskExecutor executor;

    @Inject
    public HealthNodeSelectorLifecycleHandler(HealthNodeSelectorTaskExecutor executor) {
        this.executor = executor;
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        executor.abortTaskIfApplicable();
    }

    @Override
    protected void doClose() throws IOException {

    }
}
