/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
