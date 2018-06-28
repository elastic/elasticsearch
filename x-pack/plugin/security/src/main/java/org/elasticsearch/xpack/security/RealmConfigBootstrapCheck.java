/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.security.authc.InternalRealms;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Performs bootstrap checks required across security realms using realm
 * configuration.<br>
 * The checks also run for realms provided by security extensions.<br>
 * Not all checks could be performed with bootstrap check, the kind of checks
 * performed are like invalid/missing types, allowed no of instances for realm
 * type or lookup realms validation.
 */
public class RealmConfigBootstrapCheck implements BootstrapCheck {
    private static final Logger LOGGER = Loggers.getLogger(RealmConfigBootstrapCheck.class);

    private final List<SecurityExtension> securityExtensions;

    public RealmConfigBootstrapCheck(final List<SecurityExtension> securityExtensions) {
        this.securityExtensions = Collections.unmodifiableList(securityExtensions);
    }

    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        BootstrapCheckResult result = BootstrapCheckResult.success();
        final ThreadPool threadPool = new ThreadPool(context.settings);
        try (ResourceWatcherService watcherService = new ResourceWatcherService(context.settings, threadPool)) {
            final Map<String, Realm.Factory> extensionsRealmTypes = new HashMap<>();
            securityExtensions.stream().forEach(e -> extensionsRealmTypes.putAll(e.getRealms(watcherService)));

            final Settings realmsSettings = RealmSettings.get(context.settings);
            final Map<String, RealmConfig> realmConfigByName = new HashMap<>();
            final Set<String> registeredRealmTypes = new HashSet<>();
            final Set<String> internalRealmTypes = new HashSet<>();
            final Set<String> realmNamesUsedAsLookups = new HashSet<>();
            final Set<String> candidateLookupRealms = new HashSet<>();
            for (String name : realmsSettings.names()) {
                final RealmConfig config = createRealmConfig(extensionsRealmTypes, realmsSettings, name, context, internalRealmTypes,
                        registeredRealmTypes, threadPool);
                if (config == null) {
                    continue;
                }
                realmConfigByName.put(name, config);
                if (config.lookupRealms().isEmpty()) {
                    candidateLookupRealms.add(config.name());
                }
                realmNamesUsedAsLookups.addAll(config.lookupRealms());
            }

            realmNamesUsedAsLookups.stream().forEach(realmName -> {
                if (realmConfigByName.containsKey(realmName) == false) {
                    throw new IllegalArgumentException(realmName + " used as lookup realm does not exist or is disabled");
                }
                if (candidateLookupRealms.contains(realmName) == false) {
                    throw new IllegalArgumentException(realmName + " used as lookup realm is itself dependent on other realms for lookup");
                }
            });
        } catch (Exception e) {
            result = BootstrapCheckResult.failure(e.getMessage());
        } finally {
            try {
                threadPool.close();
            } catch (IOException e) {
                LOGGER.debug("Exception occurred while closing thread pool", e);
            }
            threadPool.shutdown();
            boolean cleanShutDown = false;
            try {
                cleanShutDown = threadPool.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.debug("Timeout while waiting for thread pool clean shutdown", e);
            }
            if (cleanShutDown == false) {
                threadPool.shutdownNow();
            }
        }
        return result;
    }

    private RealmConfig createRealmConfig(final Map<String, Realm.Factory> extensionsRealmTypes, final Settings realmsSettings,
            final String name, final BootstrapContext context, final Set<String> internalRealmTypes, final Set<String> registeredRealmTypes,
            ThreadPool threadPool) throws Exception {
        final Settings realmSettings = realmsSettings.getAsSettings(name);
        final String type = realmSettings.get("type");
        if (Strings.isNullOrEmpty(type)) {
            throw new IllegalArgumentException("missing realm type for [" + name + "] realm");
        }
        final boolean isInternalRealmType = InternalRealms.TYPE_REALM_CLASS_REGISTRY.get(type) != null;
        if (isInternalRealmType == false && extensionsRealmTypes.get(type) == null) {
            throw new IllegalArgumentException("unknown realm type [" + type + "] set for realm [" + name + "]");
        }
        final RealmConfig config = new RealmConfig(name, realmSettings, context.settings, null, threadPool.getThreadContext());
        if (!config.enabled()) {
            LOGGER.debug("realm [{}/{}] is disabled", type, name);
            return null;
        }
        if (FileRealmSettings.TYPE.equals(type) || NativeRealmSettings.TYPE.equals(type)) {
            // this is an internal realm factory, let's make sure we didn't already
            // registered one (there can only be one instance of an internal realm)
            if (internalRealmTypes.contains(type)) {
                throw new IllegalArgumentException("multiple [" + type + "] realms are configured. [" + type
                        + "] is an internal realm and therefore there can only be one such realm configured");
            }
            internalRealmTypes.add(type);
        }
        registeredRealmTypes.add(type);

        return config;
    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }
}
