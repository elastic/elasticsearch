/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner.spi;

import org.elasticsearch.plugin.scanner.StablePluginRegistry;

import java.nio.file.Path;
import java.util.stream.Stream;

public interface StablePluginRegistryProvider {
    StablePluginRegistry getInstance(Stream<Path> pathStream);
}
