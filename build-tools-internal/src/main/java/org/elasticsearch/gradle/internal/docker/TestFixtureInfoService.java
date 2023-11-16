/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.docker;

import com.avast.gradle.dockercompose.ServiceInfo;

import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.util.HashMap;
import java.util.Map;

public abstract class TestFixtureInfoService implements BuildService<BuildServiceParameters.None> {

    Map<String, ServiceInfo> serviceInfo = new HashMap<>();
    public void registerServiceInfo(String name, ServiceInfo info) {
        serviceInfo.put(name, info);
        System.out.println("DockerComposeInfoService.registerServiceInfo");
        System.out.println("name = " + name);
        System.out.println("info = " + info);
    }
    
    public Map<String, ServiceInfo> getServiceInfo() {
        return serviceInfo;
    }
}
