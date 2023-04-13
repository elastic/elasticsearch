/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.jdk.ModuleQualifiedExportsService;

public class XPackCoreModuleExportsService extends ModuleQualifiedExportsService {
    @Override
    protected void addExports(String pkg, Module target) {
        selfModule.addExports(pkg, target);
    }

    @Override
    protected void addOpens(String pkg, Module target) {
        selfModule.addOpens(pkg, target);
    }
}
