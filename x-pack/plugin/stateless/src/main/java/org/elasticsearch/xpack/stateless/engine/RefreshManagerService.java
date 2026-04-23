/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.util.function.Consumer;

public abstract class RefreshManagerService extends AbstractLifecycleComponent {

    public abstract RefreshManager createRefreshManager(IndexSettings indexSettings, Consumer<RefreshManager.Request> refresh);

    /**
     * A no-op implementation that always returns Noop RefreshManager instances.
     */
    public static class Noop extends RefreshManagerService {

        @Override
        public RefreshManager createRefreshManager(IndexSettings indexSettings, Consumer<RefreshManager.Request> refresh) {
            return new RefreshManager.Noop(refresh);
        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() throws IOException {}
    }
}
