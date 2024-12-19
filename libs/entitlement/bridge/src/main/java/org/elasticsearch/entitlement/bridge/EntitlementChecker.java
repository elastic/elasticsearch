/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.util.List;

public interface EntitlementChecker {

    // Exit the JVM process
    void check$$exit(Class<?> callerClass, Runtime runtime, int status);

    void check$$halt(Class<?> callerClass, Runtime runtime, int status);

    // URLClassLoader ctor
    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory);

    void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory);

    // Process creation
    void check$$start(Class<?> callerClass, ProcessBuilder that, ProcessBuilder.Redirect[] redirects);

    void check$java_lang_ProcessBuilder$startPipeline(Class<?> callerClass, List<ProcessBuilder> builders);

}
