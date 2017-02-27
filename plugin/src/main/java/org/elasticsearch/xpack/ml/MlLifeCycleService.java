/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.NativeControllerHolder;

import java.io.IOException;

public class MlLifeCycleService extends AbstractComponent {

    public MlLifeCycleService(Settings settings, ClusterService clusterService) {
        super(settings);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop();
            }
        });
    }

    public synchronized void stop() {
        try {
            NativeController nativeController = NativeControllerHolder.getNativeController(settings);
            if (nativeController != null) {
                nativeController.stop();
            }
        } catch (IOException e) {
            // We're stopping anyway, so don't let this complicate the shutdown sequence
        }
    }
}
