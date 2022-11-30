/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 *  [2020] Elasticsearch Incorporated. All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Elasticsearch Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Elasticsearch Incorporated.
 */

package org.elasticsearch.xpack.idp.privileges;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequest;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

public class ApplicationActionsResolver extends AbstractLifecycleComponent {

    private static final int CACHE_SIZE_DEFAULT = 100;
    private static final TimeValue CACHE_TTL_DEFAULT = TimeValue.timeValueMinutes(90);

    public static final Setting<Integer> CACHE_SIZE = Setting.intSetting(
        "xpack.idp.privileges.cache.size",
        CACHE_SIZE_DEFAULT,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CACHE_TTL = Setting.timeSetting(
        "xpack.idp.privileges.cache.ttl",
        CACHE_TTL_DEFAULT,
        Setting.Property.NodeScope
    );

    private final Logger logger = LogManager.getLogger(ApplicationActionsResolver.class);

    private final ServiceProviderDefaults defaults;
    private final Client client;
    private final Cache<String, Set<String>> cache;

    public ApplicationActionsResolver(Settings settings, ServiceProviderDefaults defaults, Client client) {
        this.defaults = defaults;
        this.client = new OriginSettingClient(client, ClientHelper.IDP_ORIGIN);

        final TimeValue cacheTtl = CACHE_TTL.get(settings);
        this.cache = CacheBuilder.<String, Set<String>>builder()
            .setMaximumWeight(CACHE_SIZE.get(settings))
            .setExpireAfterWrite(cacheTtl)
            .build();

        // Preload the cache at 2/3 of its expiry time (TTL). This means that we should never have an empty cache, but if for some reason
        // the preload thread stops running, we will still automatically refresh the cache on access.
        final TimeValue preloadInterval = TimeValue.timeValueMillis(cacheTtl.millis() * 2 / 3);
        client.threadPool().scheduleWithFixedDelay(this::loadPrivilegesForDefaultApplication, preloadInterval, ThreadPool.Names.GENERIC);
    }

    public static Collection<? extends Setting<?>> getSettings() {
        return List.of(CACHE_SIZE, CACHE_TTL);
    }

    @Override
    protected void doStart() {
        loadPrivilegesForDefaultApplication();
    }

    private void loadPrivilegesForDefaultApplication() {
        loadActions(
            defaults.applicationName,
            ActionListener.wrap(
                actions -> logger.info(
                    "Found actions [{}] defined within application privileges for [{}]",
                    actions,
                    defaults.applicationName
                ),
                ex -> logger.warn(
                    () -> format("Failed to load application privileges actions for application [%s]", defaults.applicationName),
                    ex
                )
            )
        );
    }

    @Override
    protected void doStop() {
        // no-op
    }

    @Override
    protected void doClose() throws IOException {
        // no-op
    }

    public void getActions(String application, ActionListener<Set<String>> listener) {
        final Set<String> actions = this.cache.get(application);
        if (actions == null || actions.isEmpty()) {
            loadActions(application, listener);
        } else {
            listener.onResponse(actions);
        }
    }

    private void loadActions(String applicationName, ActionListener<Set<String>> listener) {
        final GetPrivilegesRequest request = new GetPrivilegesRequest();
        request.application(applicationName);
        this.client.execute(GetPrivilegesAction.INSTANCE, request, ActionListener.wrap(response -> {
            final Set<String> fixedActions = Stream.of(response.privileges())
                .map(p -> p.getActions())
                .flatMap(Collection::stream)
                .filter(s -> s.indexOf('*') == -1)
                .collect(Collectors.toUnmodifiableSet());
            cache.put(applicationName, fixedActions);
            listener.onResponse(fixedActions);
        }, listener::onFailure));
    }
}
