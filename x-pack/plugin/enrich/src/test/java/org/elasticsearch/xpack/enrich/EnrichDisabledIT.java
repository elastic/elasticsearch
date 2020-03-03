/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoAction;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class EnrichDisabledIT extends ESSingleNodeTestCase {

    public void testEnrichAvailableButNotEnabled() {
        ensureGreen();

        XPackInfoRequest infoRequest = new XPackInfoRequest();
        infoRequest.setCategories(EnumSet.of(XPackInfoRequest.Category.FEATURES));
        XPackInfoResponse infoResponse = client().execute(XPackInfoAction.INSTANCE, infoRequest).actionGet();
        assertThat(infoResponse.getFeatureSetsInfo().getFeatureSets().get(XPackField.ENRICH).available(), is(true));
        assertThat(infoResponse.getFeatureSetsInfo().getFeatureSets().get(XPackField.ENRICH).enabled(), is(false));
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(XPackSettings.ENRICH_ENABLED_SETTING.getKey(), false).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateEnrich.class);
    }
}
