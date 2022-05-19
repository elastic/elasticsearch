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

public record PluginPolicyInfo(Path file, Set<URL> jars, Policy policy) {}
