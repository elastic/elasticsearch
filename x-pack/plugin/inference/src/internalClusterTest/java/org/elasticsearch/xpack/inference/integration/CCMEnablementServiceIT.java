/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMEnablementService;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeatureFlag;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collection;

import static org.hamcrest.Matchers.is;

public class CCMEnablementServiceIT extends ESSingleNodeTestCase {

    private CCMEnablementService ccmEnablementService;

    @BeforeClass
    public static void classSetup() {
        assumeTrue("CCM is behind a feature flag and snapshot only right now", CCMFeatureFlag.FEATURE_FLAG.isEnabled());
    }

    @Before
    public void createComponents() {
        ccmEnablementService = node().injector().getInstance(CCMEnablementService.class);
    }

    // Ensure we have a node that doesn't contain any enablement cluster state
    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(CCMSettings.CCM_SUPPORTED_ENVIRONMENT.getKey(), true)
            // Disable the authorization task so we don't get errors about inconsistent state while we're
            // changing enablement
            .put(ElasticInferenceServiceSettings.AUTHORIZATION_ENABLED.getKey(), false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testSetEnabled() {
        assertCCMDisabled();

        enabledCCM();
        assertCCMEnabled();
    }

    public void testIsEnabled() {
        assertCCMDisabled();

        enabledCCM();
        assertCCMEnabled();

        disabledCCM();
        assertCCMDisabled();
    }

    private void enabledCCM() {
        setCCMState(true);
    }

    private void disabledCCM() {
        setCCMState(false);
    }

    private void setCCMState(boolean enabled) {
        var listener = new TestPlainActionFuture<AcknowledgedResponse>();
        ccmEnablementService.setEnabled(ProjectId.DEFAULT, enabled, listener);
        assertThat(listener.actionGet(TimeValue.THIRTY_SECONDS), is(AcknowledgedResponse.TRUE));
    }

    private void assertCCMEnabled() {
        assertCCMState(true);
    }

    private void assertCCMDisabled() {
        assertCCMState(false);
    }

    private void assertCCMState(boolean expectedEnabled) {
        assertThat(expectedEnabled, is(ccmEnablementService.isEnabled(ProjectId.DEFAULT)));
    }
}
