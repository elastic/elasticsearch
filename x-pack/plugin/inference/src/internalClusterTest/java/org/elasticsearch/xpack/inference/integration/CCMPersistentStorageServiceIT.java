/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMPersistentStorageService;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

public class CCMPersistentStorageServiceIT extends CCMSingleNodeIT {
    private static final AtomicReference<CCMPersistentStorageService> ccmPersistentStorageService = new AtomicReference<>();

    public CCMPersistentStorageServiceIT() {
        super(new Provider() {
            @Override
            public void store(CCMModel ccmModel, ActionListener<Void> listener) {
                ccmPersistentStorageService.get().store(ccmModel, listener);
            }

            @Override
            public void get(ActionListener<CCMModel> listener) {
                ccmPersistentStorageService.get().get(listener);
            }

            @Override
            public void delete(ActionListener<Void> listener) {
                ccmPersistentStorageService.get().delete(listener);
            }
        });
    }

    @Before
    public void createComponents() {
        ccmPersistentStorageService.set(node().injector().getInstance(CCMPersistentStorageService.class));
    }
}
