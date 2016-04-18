/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.license.plugin.LicensingClient;
import org.elasticsearch.marvel.client.MonitoringClient;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.client.SecurityClient;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.xpack.action.XPackInfoAction;
import org.elasticsearch.xpack.action.XPackInfoRequest;
import org.elasticsearch.xpack.action.XPackInfoRequestBuilder;
import org.elasticsearch.xpack.action.XPackInfoResponse;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

/**
 *
 */
public class XPackClient {

    private final Client client;

    private final LicensingClient licensingClient;
    private final MonitoringClient monitoringClient;
    private final SecurityClient securityClient;
    private final WatcherClient watcherClient;

    public XPackClient(Client client) {
        this.client = client;
        this.licensingClient = new LicensingClient(client);
        this.monitoringClient = new MonitoringClient(client);
        this.securityClient = new SecurityClient(client);
        this.watcherClient = new WatcherClient(client);
    }

    public Client es() {
        return client;
    }

    public LicensingClient licensing() {
        return licensingClient;
    }

    public MonitoringClient monitoring() {
        return monitoringClient;
    }

    public SecurityClient security() {
        return securityClient;
    }

    public WatcherClient watcher() {
        return watcherClient;
    }

    public XPackClient withHeaders(Map<String, String> headers) {
        return new XPackClient(client.filterWithHeader(headers));
    }

    /**
     * Returns a client that will call xpack APIs on behalf of the given user.
     *
     * @param username The username of the user
     * @param passwd    The password of the user. This char array can be cleared after calling this method.
     */
    public XPackClient withAuth(String username, char[] passwd) {
        return withHeaders(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(username, new SecuredString(passwd))));
    }

    public XPackInfoRequestBuilder prepareInfo() {
        return new XPackInfoRequestBuilder(client);
    }

    public void info(XPackInfoRequest request, ActionListener<XPackInfoResponse> listener) {
        client.execute(XPackInfoAction.INSTANCE, request, listener);
    }
}
