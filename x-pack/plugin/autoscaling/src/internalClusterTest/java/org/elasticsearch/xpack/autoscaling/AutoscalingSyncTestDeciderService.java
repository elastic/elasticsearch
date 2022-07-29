/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class AutoscalingSyncTestDeciderService implements AutoscalingDeciderService {

    // ! ensures this decider is always first in tests.
    public static final String NAME = "!sync";

    public static final Setting<Boolean> CHECK_FOR_CANCEL = Setting.boolSetting("check_for_cancel", false);

    private final CyclicBarrier syncBarrier = new CyclicBarrier(2);

    public AutoscalingSyncTestDeciderService() {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        internalSync();
        internalSync();
        if (CHECK_FOR_CANCEL.get(configuration)) {
            context.ensureNotCancelled();
            assert false;
        }
        return null;
    }

    private void internalSync() {
        try {
            syncBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            assert false : e;
        }
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(CHECK_FOR_CANCEL);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return DiscoveryNodeRole.roles().stream().toList();
    }

    @Override
    public boolean defaultOn() {
        return false;
    }

    public <E extends Exception> void sync(CheckedRunnable<E> run) throws E {
        internalSync();
        try {
            run.run();
        } finally {
            internalSync();
        }
    }
}
