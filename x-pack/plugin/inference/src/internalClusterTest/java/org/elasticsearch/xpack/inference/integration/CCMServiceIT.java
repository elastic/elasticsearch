/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

public class CCMServiceIT extends CCMSingleNodeIT {
    private static final AtomicReference<CCMService> ccmService = new AtomicReference<>();

    public CCMServiceIT() {
        super(new Provider() {
            @Override
            public void store(CCMModel ccmModel, ActionListener<Void> listener) {
                ccmService.get().storeConfiguration(ccmModel, listener);
            }

            @Override
            public void get(ActionListener<CCMModel> listener) {
                ccmService.get().getConfiguration(listener);
            }

            @Override
            public void delete(ActionListener<Void> listener) {
                ccmService.get().disableCCM(listener);
            }
        });
    }

    @Before
    public void createComponents() {
        ccmService.set(node().injector().getInstance(CCMService.class));
    }

    public void testIsEnabled_ReturnsFalse_WhenNoCCMConfigurationStored() {
        var listener = new PlainActionFuture<Boolean>();
        ccmService.get().isEnabled(listener);

        assertFalse(listener.actionGet(TimeValue.THIRTY_SECONDS));
    }

    public void testIsEnabled_ReturnsTrue_WhenCCMConfigurationIsPresent() {
        assertStoreCCMConfiguration();

        var listener = new PlainActionFuture<Boolean>();
        ccmService.get().isEnabled(listener);

        assertTrue(listener.actionGet(TimeValue.THIRTY_SECONDS));
    }
}
