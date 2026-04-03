/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.runtime.registry.InstrumentationInfo;

import java.util.Map;

public class InstrumentationServiceImpl implements InstrumentationService {
    public Instrumenter newInstrumenter(Class<?> clazz, Map<MethodKey, InstrumentationInfo> methods) {
        return InstrumenterImpl.create(clazz, methods);
    }
}
