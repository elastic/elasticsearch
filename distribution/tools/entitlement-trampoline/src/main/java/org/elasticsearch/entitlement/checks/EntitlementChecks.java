/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.checks;

import java.io.InputStream;
import java.io.PrintStream;

public interface EntitlementChecks {
    @CheckBefore(method = "exit")
    void checkSystemExit(Class<?> callerClass, System system, int status);

    @CheckBefore(method = "setIn")
    void checkSystemSetIn(Class<?> callerClass, System system, InputStream in);

    @CheckBefore(method = "setOut")
    void checkSystemSetOut(Class<?> callerClass, System system, PrintStream out);

    @CheckBefore(method = "setErr")
    void checkSystemSetErr(Class<?> callerClass, System system, PrintStream err);
}
