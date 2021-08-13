/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import java.net.URL;
import java.nio.file.Path;
import java.security.Policy;
import java.util.Set;

public class PluginPolicyInfo {
    public final Path file;
    public final Set<URL> jars;
    public final Policy policy;

    PluginPolicyInfo(Path file, Set<URL> jars, Policy policy) {
        this.file = file;
        this.jars = jars;
        this.policy = policy;
    }
}
