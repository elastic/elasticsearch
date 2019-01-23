/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;

import static java.util.Arrays.asList;

public class ClientReference extends Plugin {

    static final ClientInvocationHandler HANDLER = new ClientInvocationHandler();
    private static final Client CLIENT =
        (Client) Proxy.newProxyInstance(ClientReference.class.getClassLoader(), new Class[]{Client.class}, HANDLER);
    private static final XPackLicenseState LICENSE = new XPackLicenseState(Settings.EMPTY);

    @Override
    public Collection<Module> createGuiceModules() {
        return asList(b -> b.bind(Client.class).toInstance(CLIENT), b -> b.bind(XPackLicenseState.class).toInstance(LICENSE));
    }

    static class ClientInvocationHandler implements InvocationHandler {

        volatile Client actualClient;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return method.invoke(actualClient, args);
        }
    }
}
