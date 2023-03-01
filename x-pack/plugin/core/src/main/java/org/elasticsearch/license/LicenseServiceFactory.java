/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.spi.SPIClassIterator;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

public class LicenseServiceFactory {

    public static synchronized LicenseService create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        Clock clock,
        XPackLicenseState xPacklicenseState
    ) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        SPIClassIterator<LicenseService> classIterator = SPIClassIterator.get(LicenseService.class, LicenseService.class.getClassLoader());
        Set<Class<? extends LicenseService>> clazzSet = new HashSet<>(2);
        while (classIterator.hasNext()) {
            clazzSet.add(classIterator.next());
        }
        // We only support at max 2 implementations, one of which is required to be ClusterStateLicenseService.
        // We will always require the implementation that is not ClusterStateLicenseService if there is more than 1 implementation
        if (clazzSet.size() <= 0) {
            throw new IllegalStateException("Could not find any SPI definitions for LicenseService");
        }
        if (clazzSet.size() > 2) {
            throw new IllegalStateException("Found too many SPI definitions for LicenseService");
        }

        if (clazzSet.size() == 2) {
            boolean removed = clazzSet.remove(ClusterStateLicenseService.class);
            if (removed == false || clazzSet.size() != 1) {
                // should never happen
                throw new IllegalStateException("Unexpected SPI definition for LicenseService. This is likely a bug.");
            }
        }
        assert clazzSet.size() == 1;
        String canonicalName = clazzSet.iterator().next().getCanonicalName();
        @SuppressWarnings("rawtypes")
        Constructor constructor = Class.forName(canonicalName)
            .getConstructor(Settings.class, ThreadPool.class, ClusterService.class, Clock.class, XPackLicenseState.class);

        return (LicenseService) constructor.newInstance(settings, threadPool, clusterService, clock, xPacklicenseState);
    }
}
