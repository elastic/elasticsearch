/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.init;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

/**
 * A service to lazy initialize {@link LazyInitializable} constructs.
 */
public class LazyInitializationService extends AbstractLifecycleComponent {

    private final Injector injector;
    private final Set<LazyInitializable> initializables;

    @Inject
    public LazyInitializationService(Settings settings, Injector injector, Set<LazyInitializable> initializables) {
        super(settings);
        this.injector = injector;
        this.initializables = initializables;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        for (LazyInitializable initializable : initializables) {
            logger.trace("lazy initialization of [{}]", initializable);
            initializable.init(injector);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
