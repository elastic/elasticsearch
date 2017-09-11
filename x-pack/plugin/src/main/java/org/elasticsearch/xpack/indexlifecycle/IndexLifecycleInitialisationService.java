/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;

public class IndexLifecycleInitialisationService extends AbstractComponent implements ClusterStateListener {

    public IndexLifecycleInitialisationService(Settings settings, ClusterService clusterService) {
        super(settings);
        clusterService.addListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                super.beforeStop();
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN").error("cluster state changed: " + event.source());
    }

}
