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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

@SuppressWarnings("unused") // Called from instrumentation code inserted by the Entitlements agent
public interface EntitlementChecker {

    ////////////////////
    //
    // Exit the JVM process
    //

    void check$java_lang_Runtime$exit(Class<?> callerClass, Runtime runtime, int status);

    void check$java_lang_Runtime$halt(Class<?> callerClass, Runtime runtime, int status);

    ////////////////////
    //
    // ClassLoader ctor
    //

    void check$java_lang_ClassLoader$(Class<?> callerClass);

    void check$java_lang_ClassLoader$(Class<?> callerClass, ClassLoader parent);

    void check$java_lang_ClassLoader$(Class<?> callerClass, String name, ClassLoader parent);

    ////////////////////
    //
    // SecureClassLoader ctor
    //

    void check$java_security_SecureClassLoader$(Class<?> callerClass);

    void check$java_security_SecureClassLoader$(Class<?> callerClass, ClassLoader parent);

    void check$java_security_SecureClassLoader$(Class<?> callerClass, String name, ClassLoader parent);

    ////////////////////
    //
    // URLClassLoader constructors
    //

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory);

    void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory);

    ////////////////////
    //
    // "setFactory" methods
    //

    void check$javax_net_ssl_HttpsURLConnection$setSSLSocketFactory(Class<?> callerClass, HttpsURLConnection conn, SSLSocketFactory sf);

    void check$javax_net_ssl_HttpsURLConnection$$setDefaultSSLSocketFactory(Class<?> callerClass, SSLSocketFactory sf);

    void check$javax_net_ssl_HttpsURLConnection$$setDefaultHostnameVerifier(Class<?> callerClass, HostnameVerifier hv);

    void check$javax_net_ssl_SSLContext$$setDefault(Class<?> callerClass, SSLContext context);

    ////////////////////
    //
    // Process creation
    //

    void check$java_lang_ProcessBuilder$start(Class<?> callerClass, ProcessBuilder that);

    void check$java_lang_ProcessBuilder$$startPipeline(Class<?> callerClass, List<ProcessBuilder> builders);

}
