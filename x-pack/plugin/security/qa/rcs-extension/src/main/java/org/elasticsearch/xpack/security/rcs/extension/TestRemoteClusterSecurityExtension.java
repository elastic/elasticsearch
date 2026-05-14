/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rcs.extension;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.security.authc.RemoteClusterAuthenticationService;
import org.elasticsearch.xpack.security.transport.RemoteClusterTransportInterceptor;
import org.elasticsearch.xpack.security.transport.ServerTransportFilter;
import org.elasticsearch.xpack.security.transport.extension.RemoteClusterSecurityExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TestRemoteClusterSecurityExtension implements RemoteClusterSecurityExtension {

    public static final Setting<String> DUMMY_EXTENSION_SETTING = Setting.simpleString(
        "xpack.test.rcs.extension.setting",
        "default-rcs-extension-setting-value",
        Setting.Property.NodeScope
    );

    private final Components components;

    public TestRemoteClusterSecurityExtension(Components components) {
        this.components = components;
    }

    @Override
    public RemoteClusterTransportInterceptor getTransportInterceptor() {
        return new RemoteClusterTransportInterceptor() {

            @Override
            public TransportInterceptor.AsyncSender interceptSender(TransportInterceptor.AsyncSender sender) {
                return sender;
            }

            @Override
            public boolean isRemoteClusterConnection(Transport.Connection connection) {
                return false;
            }

            @Override
            public Optional<ServerTransportFilter> getRemoteProfileTransportFilter(
                SslProfile sslProfile,
                DestructiveOperations destructiveOperations
            ) {
                return Optional.empty();
            }

            public boolean hasRemoteClusterAccessHeadersInContext(SecurityContext securityContext) {
                return false;
            }

        };
    }

    @Override
    public RemoteClusterAuthenticationService getAuthenticationService() {
        return new RemoteClusterAuthenticationService() {

            @Override
            public void authenticate(String action, TransportRequest request, ActionListener<Authentication> listener) {
                listener.onFailure(new IllegalStateException("not relevant for this test"));
            }

            @Override
            public void authenticateHeaders(Map<String, String> headers, ActionListener<Void> listener) {
                listener.onResponse(null);
            }
        };
    }

    public static class Provider implements RemoteClusterSecurityExtension.Provider {

        @Override
        public RemoteClusterSecurityExtension getExtension(Components components) {
            return new TestRemoteClusterSecurityExtension(components);
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(DUMMY_EXTENSION_SETTING);
        }
    }

}
