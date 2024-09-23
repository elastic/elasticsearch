/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.agent;

import org.elasticsearch.entitlement.runtime.api.EntitlementChecks;

import java.lang.instrument.Instrumentation;

public class EntitlementAgent {

    public static void premain(String agentArgs, Instrumentation inst) throws Exception {
        EntitlementChecks.setAgentBooted();
    }
}
