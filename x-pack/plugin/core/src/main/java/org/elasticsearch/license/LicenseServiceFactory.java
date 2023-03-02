/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LicenseServiceFactory {

    private static final Logger logger = LogManager.getLogger(LicenseServiceFactory.class);

    public static synchronized LicenseService create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        Clock clock,
        XPackLicenseState xPacklicenseState
    ) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Map<ClassLoader, Set<String>> possible = PluginsService.getSPIImplementationClassNames(LicenseService.class);
        Map<String, ClassLoader> flatPossible = new HashMap<>(possible.values().size());

        possible.forEach((k, v) -> {
            v.forEach(implementation -> {
                if (flatPossible.containsKey(implementation)) {
                    // should never happen
                    throw new IllegalStateException("LicenseService does not support multiple implementations with same FQN");
                }
                flatPossible.put(implementation, k);
            });
        });

        // We only support at max 2 implementations, one of which is required to be ClusterStateLicenseService.
        // We will always require the implementation that is not ClusterStateLicenseService if there is more than 1 implementation
        if (flatPossible.size() <= 0) {
            throw new IllegalStateException("Could not find any SPI definitions for LicenseService");
        }
        if (flatPossible.size() > 2) {
            throw new IllegalStateException("Found too many SPI definitions for LicenseService");
        }

        if (flatPossible.size() == 2) {
            boolean removed = flatPossible.remove(ClusterStateLicenseService.class.getCanonicalName()) != null;
            if (removed == false || flatPossible.size() != 1) {
                // should never happen
                throw new IllegalStateException("Unexpected SPI definition for LicenseService. This is likely a bug.");
            }
        }
        assert flatPossible.size() == 1;
        Map.Entry<String, ClassLoader> entry = flatPossible.entrySet().iterator().next();
        String canonicalName = entry.getKey();
        ClassLoader classLoader = entry.getValue();
        logger.info("Constructing implementation " + canonicalName + " for interface " + LicenseService.class.getCanonicalName());// TODO:
                                                                                                                                  // use
                                                                                                                                  // debug
        @SuppressWarnings("rawtypes")
        Constructor constructor = Class.forName(canonicalName, true, classLoader)
            .getConstructor(Settings.class, ThreadPool.class, ClusterService.class, Clock.class, XPackLicenseState.class);

        return (LicenseService) constructor.newInstance(settings, threadPool, clusterService, clock, xPacklicenseState);
    }
}
