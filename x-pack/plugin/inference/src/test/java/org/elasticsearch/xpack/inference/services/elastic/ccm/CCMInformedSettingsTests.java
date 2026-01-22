/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMInformedSettings.DEFAULT_CCM_URL;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CCMInformedSettingsTests extends ESTestCase {
    public void testGetElasticInferenceServiceUrl_ReturnsSettingUrl_WhenConfiguringCCMIsPermitted_ButSettingUrlExists() {
        var url = "http://custom-url.com";
        var settings = Settings.builder().put(ELASTIC_INFERENCE_SERVICE_URL.getKey(), url).build();
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);

        var informedSettings = new CCMInformedSettings(settings, ccmFeature);

        assertThat(informedSettings.getElasticInferenceServiceUrl(), is(url));
    }

    public void testGetElasticInferenceServiceUrl_ReturnsCCMDefault_WhenConfiguringCCMIsPermitted_ButSettingUrlIsEmpty() {
        var settings = Settings.builder().put(ELASTIC_INFERENCE_SERVICE_URL.getKey(), "").build();
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);

        var informedSettings = new CCMInformedSettings(settings, ccmFeature);

        assertThat(informedSettings.getElasticInferenceServiceUrl(), is(DEFAULT_CCM_URL));
    }

    public void testGetElasticInferenceServiceUrl_ReturnsCCMDefault_WhenConfiguringCCMIsPermitted_ButSettingUrlIsNull() {
        var settings = Settings.builder().put(ELASTIC_INFERENCE_SERVICE_URL.getKey(), (String) null).build();
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);

        var informedSettings = new CCMInformedSettings(settings, ccmFeature);

        assertThat(informedSettings.getElasticInferenceServiceUrl(), is(DEFAULT_CCM_URL));
    }

    public void testGetElasticInferenceServiceUrl_ReturnsCCMDefault_WhenConfiguringCCMIsPermitted_ButSettingUrlIsAbsent() {
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);

        var informedSettings = new CCMInformedSettings(Settings.EMPTY, ccmFeature);

        assertThat(informedSettings.getElasticInferenceServiceUrl(), is(DEFAULT_CCM_URL));
    }

    public void testGetElasticInferenceServiceUrl_ReturnsSettingUrl_WhenConfiguringCCMIsNotPermitted() {
        var url = "http://custom-url.com";
        var settings = Settings.builder().put(ELASTIC_INFERENCE_SERVICE_URL.getKey(), url).build();
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(false);

        var informedSettings = new CCMInformedSettings(settings, ccmFeature);

        assertThat(informedSettings.getElasticInferenceServiceUrl(), is(url));
    }

    public void testGetElasticInferenceServiceUrl_ReturnsEmpty_WhenConfiguringCCMIsNotPermitted_AndSettingUrlIsEmpty() {
        var url = "";
        var settings = Settings.builder().put(ELASTIC_INFERENCE_SERVICE_URL.getKey(), url).build();
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(false);

        var informedSettings = new CCMInformedSettings(settings, ccmFeature);

        assertThat(informedSettings.getElasticInferenceServiceUrl(), is(url));
    }

    public void testGetElasticInferenceServiceUrl_ReturnsEmpty_WhenConfiguringCCMIsNotPermitted_AndSettingUrlIsNull() {
        var settings = Settings.builder().put(ELASTIC_INFERENCE_SERVICE_URL.getKey(), (String) null).build();
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(false);

        var informedSettings = new CCMInformedSettings(settings, ccmFeature);

        assertThat(informedSettings.getElasticInferenceServiceUrl(), is(""));
    }

    public void testGetElasticInferenceServiceUrl_ReturnsEmpty_WhenConfiguringCCMIsNotPermitted_AndSettingUrlAbsent() {
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(false);

        var informedSettings = new CCMInformedSettings(Settings.EMPTY, ccmFeature);

        assertThat(informedSettings.getElasticInferenceServiceUrl(), is(""));
    }
}
