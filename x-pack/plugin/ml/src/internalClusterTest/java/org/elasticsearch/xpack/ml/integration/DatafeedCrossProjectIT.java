/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class DatafeedCrossProjectIT extends MlSingleNodeTestCase {

    private DatafeedConfigProvider datafeedConfigProvider;
    private String dummyAuthenticationHeader;

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("serverless.cross_project.enabled", true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(CpsPlugin.class)).toList();
    }

    public static class CpsPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(Setting.boolSetting("serverless.cross_project.enabled", false, Setting.Property.NodeScope));
        }
    }

    @Before
    public void createComponents() throws Exception {
        datafeedConfigProvider = new DatafeedConfigProvider(client(), xContentRegistry(), getInstanceFromNode(ClusterService.class));
        waitForMlTemplates();
        dummyAuthenticationHeader = Authentication.newRealmAuthentication(
            new org.elasticsearch.xpack.core.security.user.User("dummy"),
            new Authentication.RealmRef("name", "type", "node")
        ).encode();
    }

    public void testGetDatafeedWithProjectRouting() throws Exception {
        String datafeedId = "datafeed_with_project_routing";
        String jobId = "job_for_datafeed_project_routing";
        String expectedProjectRouting = "_alias:prod-*";

        // Create datafeed with project_routing
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedBuilder.setIndices(List.of("logs-*"));
        datafeedBuilder.setProjectRouting(expectedProjectRouting);

        AtomicReference<Tuple<DatafeedConfig, DocWriteResponse>> putResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            actionListener -> datafeedConfigProvider.putDatafeedConfig(datafeedBuilder.build(), createSecurityHeader(), actionListener),
            putResponseHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertThat(putResponseHolder.get().v2().status(), equalTo(RestStatus.CREATED));

        // Get datafeed and verify project_routing is returned
        AtomicReference<DatafeedConfig.Builder> getResponseHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.getDatafeedConfig(datafeedId, null, actionListener),
            getResponseHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());

        DatafeedConfig retrievedDatafeed = getResponseHolder.get().build();
        assertThat(retrievedDatafeed.getProjectRouting(), equalTo(expectedProjectRouting));
    }

    private Map<String, String> createSecurityHeader() {
        Map<String, String> headers = new HashMap<>();
        // Only security headers are updated, grab the first one
        String securityHeader = ClientHelper.SECURITY_HEADER_FILTERS.iterator().next();
        if (Set.of(
            AuthenticationField.AUTHENTICATION_KEY,
            org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication.THREAD_CTX_KEY
        ).contains(securityHeader)) {
            headers.put(securityHeader, dummyAuthenticationHeader);
        } else {
            headers.put(securityHeader, "SECURITY_");
        }
        return headers;
    }
}
