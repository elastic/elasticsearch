/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.init;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

/**
 * A service to lazy initialize {@link InitializingService.Initializable} constructs.
 */
public class InitializingService extends AbstractLifecycleComponent {

    private final Injector injector;
    private final Set<Initializable> initializables;

    @Inject
    public InitializingService(Settings settings, Injector injector, Set<Initializable> initializables) {
        super(settings);
        this.injector = injector;
        this.initializables = initializables;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        for (Initializable initializable : initializables) {
            initializable.init(injector);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    public static interface Initializable {

        void init(Injector injector);
    }
}
