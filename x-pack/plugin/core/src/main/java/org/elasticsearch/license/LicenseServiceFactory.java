/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;

public class LicenseServiceFactory {

    public static SetOnce<LicenseService> instance = new SetOnce<>();

    public static synchronized LicenseService create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        Clock clock,
        XPackLicenseState xPacklicenseState
    ) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        if (instance.get() != null) {
            return instance.get();
        }

        @SuppressWarnings("rawtypes")
        Constructor constructor = Class.forName("org.elasticsearch.license.ClusterStateLicenseService")
            .getConstructor(Settings.class, ThreadPool.class, ClusterService.class, Clock.class, XPackLicenseState.class);

        LicenseService licenseService = (LicenseService) constructor.newInstance(
            settings,
            threadPool,
            clusterService,
            clock,
            xPacklicenseState
        );
        instance.set(licenseService);
        return licenseService;
    }
}
