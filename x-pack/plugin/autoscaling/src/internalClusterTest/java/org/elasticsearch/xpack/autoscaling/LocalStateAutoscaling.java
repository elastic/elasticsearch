/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LocalStateAutoscaling extends LocalStateCompositeXPackPlugin {

    private final AutoscalingTestPlugin testPlugin;

    public LocalStateAutoscaling(final Settings settings) {
        super(settings, null);
        testPlugin = new AutoscalingTestPlugin();
        plugins.add(testPlugin);
    }

    public AutoscalingTestPlugin testPlugin() {
        return testPlugin;
    }

    public static class AutoscalingTestPlugin extends Autoscaling {
        private final AutoscalingSyncTestDeciderService syncDeciderService = new AutoscalingSyncTestDeciderService();

        private AutoscalingTestPlugin() {
            super(new AutoscalingLicenseChecker(() -> true));
        }

        @Override
        public Set<AutoscalingDeciderService> createDeciderServices() {
            Set<AutoscalingDeciderService> deciderServices = new HashSet<>(super.createDeciderServices());
            deciderServices.add(syncDeciderService);
            deciderServices.add(new AutoscalingCountTestDeciderService());
            return deciderServices;
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return CollectionUtils.appendToCopy(
                super.getNamedWriteables(),
                new NamedWriteableRegistry.Entry(
                    AutoscalingDeciderResult.Reason.class,
                    AutoscalingCountTestDeciderService.NAME,
                    AutoscalingCountTestDeciderService.CountReason::new
                )
            );
        }

        /**
         * Sync with decider service, running the runnable when we are sure that an active capacity response is blocked in the sync decider.
         * @param run
         */
        public <E extends Exception> void syncWithDeciderService(CheckedRunnable<E> run) throws E {
            syncDeciderService.sync(run);
        }
    }
}
