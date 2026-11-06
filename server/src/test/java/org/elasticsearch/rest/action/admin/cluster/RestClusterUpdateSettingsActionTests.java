/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class RestClusterUpdateSettingsActionTests extends RestActionTestCase {

    private static final Setting<SecureString> SECURE = SecureSetting.secureString("test.secure", null, Setting.Property.Dynamic);
    private static final Setting<Integer> REGULAR = Setting.intSetting(
        "test.regular",
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private RestClusterUpdateSettingsAction action;
    private boolean clusterSupportsFeature;

    @BeforeClass
    public static void checkFeatureFlagEnabled() {
        assumeTrue(
            "secure_settings_api feature flag must be enabled",
            UpdateSecureSettingsAction.SECURE_SETTINGS_API_FEATURE_FLAG.isEnabled()
        );
    }

    @Before
    public void createAction() {
        clusterSupportsFeature = true;
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.<Setting<?>>of(SECURE, REGULAR));
        action = new RestClusterUpdateSettingsAction(clusterSettings, feature -> clusterSupportsFeature);
    }

    public void testSecureSettingIsSentToSecureSettingsAction() throws Exception {
        AtomicReference<UpdateSecureSettingsRequest> captured = new AtomicReference<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(actionType, equalTo(UpdateSecureSettingsAction.INSTANCE));
            captured.set((UpdateSecureSettingsRequest) request);
            return AcknowledgedResponse.TRUE;
        });

        RestRequest request = request("https", """
            {"persistent": {"test.secure": "s3cr3t"}}""");
        FakeRestChannel channel = new FakeRestChannel(request, true);
        action.handleRequest(request, channel, verifyingClient);

        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        assertThat(channel.capturedResponse().content().utf8ToString(), not(containsString("s3cr3t")));
        assertThat(captured.get().settings().get("test.secure").toString(), equalTo("s3cr3t"));
    }

    public void testNullRemovesSecureSetting() throws Exception {
        AtomicReference<UpdateSecureSettingsRequest> captured = new AtomicReference<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            captured.set((UpdateSecureSettingsRequest) request);
            return AcknowledgedResponse.TRUE;
        });

        RestRequest request = request("https", """
            {"persistent": {"test.secure": null}}""");
        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);

        assertTrue(captured.get().settings().containsKey("test.secure"));
        assertThat(captured.get().settings().get("test.secure"), nullValue());
    }

    public void testRejectsPlaintextHttp() {
        assertBadRequest(request("http", """
            {"persistent": {"test.secure": "s3cr3t"}}"""), "HTTPS");
    }

    public void testRejectsTransientSecureSetting() {
        assertBadRequest(request("https", """
            {"transient": {"test.secure": "s3cr3t"}}"""), "persistent");
    }

    public void testRejectsMixedRequest() {
        assertBadRequest(request("https", """
            {"persistent": {"test.secure": "s3cr3t", "test.regular": 2}}"""), "cannot also set regular settings");
    }

    public void testRejectsNonObjectSection() {
        assertBadRequest(request("https", """
            {"persistent": "test.secure"}"""), "[persistent] must be an object");
    }

    public void testRejectsWhenClusterDoesNotSupportFeature() {
        clusterSupportsFeature = false;
        assertBadRequest(request("https", """
            {"persistent": {"test.secure": "s3cr3t"}}"""), "all nodes");
    }

    public void testRegularSettingsUseClusterSettingsAction() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(actionType, equalTo(ClusterUpdateSettingsAction.INSTANCE));
            assertThat(request, instanceOf(ClusterUpdateSettingsRequest.class));
            return new ClusterUpdateSettingsResponse(true, Settings.EMPTY, Settings.EMPTY);
        });

        RestRequest request = request("http", """
            {"persistent": {"test.regular": 2}}""");
        FakeRestChannel channel = new FakeRestChannel(request, true);
        action.handleRequest(request, channel, verifyingClient);
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
    }

    public void testAuditViewOmitsBodyWithSecureSettings() {
        RestRequest secure = request("https", """
            {"persistent": {"test.secure": "s3cr3t"}}""");
        assertThat(action.getFilteredRequest(secure).content().utf8ToString(), not(containsString("s3cr3t")));

        RestRequest regular = request("https", """
            {"persistent": {"test.regular": 2}}""");
        assertThat(action.getFilteredRequest(regular).content().utf8ToString(), containsString("test.regular"));

        RestRequest malformed = request("https", "{\"persistent\": {\"test.secure\": \"s3cr3t\"");
        assertThat(action.getFilteredRequest(malformed).content().utf8ToString(), not(containsString("s3cr3t")));
    }

    private void assertBadRequest(RestRequest request, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
        assertThat(e.getMessage(), containsString(message));
    }

    private RestRequest request(String scheme, String body) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_cluster/settings")
            .withScheme(scheme)
            .withContent(new BytesArray(body), XContentType.JSON)
            .build();
    }
}
