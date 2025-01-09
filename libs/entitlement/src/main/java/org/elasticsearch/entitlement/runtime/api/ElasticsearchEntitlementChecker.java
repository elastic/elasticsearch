/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;

import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.ContentHandlerFactory;
import java.net.DatagramSocketImplFactory;
import java.net.FileNameMap;
import java.net.SocketImplFactory;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

/**
 * Implementation of the {@link EntitlementChecker} interface, providing additional
 * API methods for managing the checks.
 * The trampoline module loads this object via SPI.
 */
public class ElasticsearchEntitlementChecker implements EntitlementChecker {

    private final PolicyManager policyManager;

    public ElasticsearchEntitlementChecker(PolicyManager policyManager) {
        this.policyManager = policyManager;
    }

    @Override
    public void check$java_lang_Runtime$exit(Class<?> callerClass, Runtime runtime, int status) {
        policyManager.checkExitVM(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$halt(Class<?> callerClass, Runtime runtime, int status) {
        policyManager.checkExitVM(callerClass);
    }

    @Override
    public void check$java_lang_System$$exit(Class<?> callerClass, int status) {
        policyManager.checkExitVM(callerClass);
    }

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass, String name, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass, String name, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(
        Class<?> callerClass,
        String name,
        URL[] urls,
        ClassLoader parent,
        URLStreamHandlerFactory factory
    ) {
        policyManager.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_lang_ProcessBuilder$start(Class<?> callerClass, ProcessBuilder processBuilder) {
        policyManager.checkStartProcess(callerClass);
    }

    @Override
    public void check$java_lang_ProcessBuilder$$startPipeline(Class<?> callerClass, List<ProcessBuilder> builders) {
        policyManager.checkStartProcess(callerClass);
    }

    @Override
    public void check$java_lang_System$$setIn(Class<?> callerClass, InputStream in) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$setOut(Class<?> callerClass, PrintStream out) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$setErr(Class<?> callerClass, PrintStream err) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$addShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$removeShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_tools_jlink_internal_Jlink$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_tools_jlink_internal_Main$$run(Class<?> callerClass, PrintWriter out, PrintWriter err, String... args) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_JVMCIServiceLocator$$getProviders(Class<?> callerClass, Class<?> service) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_Services$$load(Class<?> callerClass, Class<?> service) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_Services$$loadSingle(Class<?> callerClass, Class<?> service, boolean required) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$com_sun_tools_jdi_VirtualMachineManagerImpl$$virtualMachineManager(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Thread$$setDefaultUncaughtExceptionHandler(Class<?> callerClass, Thread.UncaughtExceptionHandler ueh) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_LocaleServiceProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_BreakIteratorProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_CollatorProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DateFormatProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DateFormatSymbolsProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DecimalFormatSymbolsProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_NumberFormatProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CalendarDataProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CalendarNameProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CurrencyNameProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_LocaleNameProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_TimeZoneNameProvider$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_logging_LogManager$(Class<?> callerClass) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$$setDatagramSocketImplFactory(Class<?> callerClass, DatagramSocketImplFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_HttpURLConnection$$setFollowRedirects(Class<?> callerClass, boolean set) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$$setSocketFactory(Class<?> callerClass, SocketImplFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_Socket$$setSocketImplFactory(Class<?> callerClass, SocketImplFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URL$$setURLStreamHandlerFactory(Class<?> callerClass, URLStreamHandlerFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URLConnection$$setFileNameMap(Class<?> callerClass, FileNameMap map) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URLConnection$$setContentHandlerFactory(Class<?> callerClass, ContentHandlerFactory fac) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$setSSLSocketFactory(
        Class<?> callerClass,
        HttpsURLConnection connection,
        SSLSocketFactory sf
    ) {
        policyManager.checkSetHttpsConnectionProperties(callerClass);
    }

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$$setDefaultSSLSocketFactory(Class<?> callerClass, SSLSocketFactory sf) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$$setDefaultHostnameVerifier(Class<?> callerClass, HostnameVerifier hv) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$javax_net_ssl_SSLContext$$setDefault(Class<?> callerClass, SSLContext context) {
        policyManager.checkChangeJVMGlobalState(callerClass);
    }
}
