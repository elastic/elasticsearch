/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import jdk.nio.Channels;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.ContentHandlerFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.DatagramSocketImplFactory;
import java.net.FileNameMap;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.JarURLConnection;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.ResponseCache;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketImplFactory;
import java.net.URI;
import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.Charset;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitor;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.spi.FileSystemProvider;
import java.security.KeyStore;
import java.security.Provider;
import java.security.cert.CertStoreParameters;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;
import java.util.logging.FileHandler;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

/**
 * Implementation of the {@link EntitlementChecker} interface
 * with each method implemented as a one-liner call into {@link PolicyChecker}.
 * In effect, for each instrumented, this indicates the kind of check to perform;
 * the actual checking logic is in {@link PolicyChecker}.
 * The bridge module loads this object via SPI.
 */
@SuppressForbidden(reason = "Explicitly checking APIs that are forbidden")
public class ElasticsearchEntitlementChecker implements EntitlementChecker {

    protected final PolicyChecker policyChecker;

    public ElasticsearchEntitlementChecker(PolicyChecker policyChecker) {
        this.policyChecker = policyChecker;
    }

    /// /////////////////
    //
    // Exit the JVM process
    //

    @Override
    public void check$java_lang_Runtime$exit(Class<?> callerClass, Runtime runtime, int status) {
        policyChecker.checkExitVM(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$halt(Class<?> callerClass, Runtime runtime, int status) {
        policyChecker.checkExitVM(callerClass);
    }

    @Override
    public void check$java_lang_System$$exit(Class<?> callerClass, int status) {
        policyChecker.checkExitVM(callerClass);
    }

    /// /////////////////
    //
    // create class loaders
    //

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass, ClassLoader parent) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_lang_ClassLoader$(Class<?> callerClass, String name, ClassLoader parent) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_net_URLClassLoader$(
        Class<?> callerClass,
        String name,
        URL[] urls,
        ClassLoader parent,
        URLStreamHandlerFactory factory
    ) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass, ClassLoader parent) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    @Override
    public void check$java_security_SecureClassLoader$(Class<?> callerClass, String name, ClassLoader parent) {
        policyChecker.checkCreateClassLoader(callerClass);
    }

    /// /////////////////
    //
    // "setFactory" methods
    //

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$setSSLSocketFactory(
        Class<?> callerClass,
        HttpsURLConnection connection,
        SSLSocketFactory sf
    ) {
        policyChecker.checkSetHttpsConnectionProperties(callerClass);
    }

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$$setDefaultSSLSocketFactory(Class<?> callerClass, SSLSocketFactory sf) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$javax_net_ssl_HttpsURLConnection$$setDefaultHostnameVerifier(Class<?> callerClass, HostnameVerifier hv) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$javax_net_ssl_SSLContext$$setDefault(Class<?> callerClass, SSLContext context) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    /// /////////////////
    //
    // Process creation
    //

    @Override
    public void check$java_lang_ProcessBuilder$start(Class<?> callerClass, ProcessBuilder processBuilder) {
        policyChecker.checkStartProcess(callerClass);
    }

    @Override
    public void check$java_lang_ProcessBuilder$$startPipeline(Class<?> callerClass, List<ProcessBuilder> builders) {
        policyChecker.checkStartProcess(callerClass);
    }

    /// /////////////////
    //
    // System Properties and similar
    //

    @Override
    public void check$java_lang_System$$clearProperty(Class<?> callerClass, String key) {
        policyChecker.checkWriteProperty(callerClass, key);
    }

    @Override
    public void check$java_lang_System$$setProperties(Class<?> callerClass, Properties props) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$setProperty(Class<?> callerClass, String key, String value) {
        policyChecker.checkWriteProperty(callerClass, key);
    }

    /// /////////////////
    //
    // JVM-wide state changes
    //

    @Override
    public void check$java_lang_System$$setIn(Class<?> callerClass, InputStream in) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$setOut(Class<?> callerClass, PrintStream out) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_System$$setErr(Class<?> callerClass, PrintStream err) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$addShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$removeShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_tools_jlink_internal_Jlink$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_tools_jlink_internal_Main$$run(Class<?> callerClass, PrintWriter out, PrintWriter err, String... args) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_JVMCIServiceLocator$$getProviders(Class<?> callerClass, Class<?> service) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_Services$$load(Class<?> callerClass, Class<?> service) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$jdk_vm_ci_services_Services$$loadSingle(Class<?> callerClass, Class<?> service, boolean required) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_nio_charset_spi_CharsetProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$com_sun_tools_jdi_VirtualMachineManagerImpl$$virtualMachineManager(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_lang_Thread$$setDefaultUncaughtExceptionHandler(Class<?> callerClass, Thread.UncaughtExceptionHandler ueh) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_LocaleServiceProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_BreakIteratorProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_CollatorProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DateFormatProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DateFormatSymbolsProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_DecimalFormatSymbolsProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_text_spi_NumberFormatProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CalendarDataProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CalendarNameProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_CurrencyNameProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_LocaleNameProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_spi_TimeZoneNameProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_logging_LogManager$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_Locale$$setDefault(Class<?> callerClass, Locale.Category category, Locale locale) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_Locale$$setDefault(Class<?> callerClass, Locale locale) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_TimeZone$$setDefault(Class<?> callerClass, TimeZone zone) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$$setDatagramSocketImplFactory(Class<?> callerClass, DatagramSocketImplFactory fac) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_HttpURLConnection$$setFollowRedirects(Class<?> callerClass, boolean set) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$$setSocketFactory(Class<?> callerClass, SocketImplFactory fac) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_Socket$$setSocketImplFactory(Class<?> callerClass, SocketImplFactory fac) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URL$$setURLStreamHandlerFactory(Class<?> callerClass, URLStreamHandlerFactory fac) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URLConnection$$setFileNameMap(Class<?> callerClass, FileNameMap map) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_net_URLConnection$$setContentHandlerFactory(Class<?> callerClass, ContentHandlerFactory fac) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    /// /////////////////
    //
    // Network access
    //

    @Override
    public void check$java_net_ProxySelector$$setDefault(Class<?> callerClass, ProxySelector ps) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_ResponseCache$$setDefault(Class<?> callerClass, ResponseCache rc) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_spi_InetAddressResolverProvider$(Class<?> callerClass) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_spi_URLStreamHandlerProvider$(Class<?> callerClass) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_URL$(Class<?> callerClass, String protocol, String host, int port, String file, URLStreamHandler handler) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_URL$(Class<?> callerClass, URL context, String spec, URLStreamHandler handler) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$bind(Class<?> callerClass, DatagramSocket that, SocketAddress addr) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$connect(Class<?> callerClass, DatagramSocket that, InetAddress addr) {
        policyChecker.checkAllNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$connect(Class<?> callerClass, DatagramSocket that, SocketAddress addr) {
        policyChecker.checkAllNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$send(Class<?> callerClass, DatagramSocket that, DatagramPacket p) {
        if (p.getAddress().isMulticastAddress()) {
            policyChecker.checkAllNetworkAccess(callerClass);
        } else {
            policyChecker.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$java_net_DatagramSocket$receive(Class<?> callerClass, DatagramSocket that, DatagramPacket p) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_DatagramSocket$joinGroup(Class<?> caller, DatagramSocket that, SocketAddress addr, NetworkInterface ni) {
        policyChecker.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_DatagramSocket$leaveGroup(Class<?> caller, DatagramSocket that, SocketAddress addr, NetworkInterface ni) {
        policyChecker.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$joinGroup(Class<?> caller, MulticastSocket that, InetAddress addr) {
        policyChecker.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$joinGroup(Class<?> caller, MulticastSocket that, SocketAddress addr, NetworkInterface ni) {
        policyChecker.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$leaveGroup(Class<?> caller, MulticastSocket that, InetAddress addr) {
        policyChecker.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$leaveGroup(Class<?> caller, MulticastSocket that, SocketAddress addr, NetworkInterface ni) {
        policyChecker.checkAllNetworkAccess(caller);
    }

    @Override
    public void check$java_net_MulticastSocket$send(Class<?> callerClass, MulticastSocket that, DatagramPacket p, byte ttl) {
        policyChecker.checkAllNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$(Class<?> callerClass, int port) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$(Class<?> callerClass, int port, int backlog) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$(Class<?> callerClass, int port, int backlog, InetAddress bindAddr) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$accept(Class<?> callerClass, ServerSocket that) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$implAccept(Class<?> callerClass, ServerSocket that, Socket s) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$bind(Class<?> callerClass, ServerSocket that, SocketAddress endpoint) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_ServerSocket$bind(Class<?> callerClass, ServerSocket that, SocketAddress endpoint, int backlog) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, Proxy proxy) {
        if (proxy.type() == Proxy.Type.SOCKS || proxy.type() == Proxy.Type.HTTP) {
            policyChecker.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, String host, int port) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, InetAddress address, int port) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, String host, int port, InetAddress localAddr, int localPort) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, InetAddress address, int port, InetAddress localAddr, int localPort) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, String host, int port, boolean stream) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$(Class<?> callerClass, InetAddress host, int port, boolean stream) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$bind(Class<?> callerClass, Socket that, SocketAddress endpoint) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$connect(Class<?> callerClass, Socket that, SocketAddress endpoint) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_Socket$connect(Class<?> callerClass, Socket that, SocketAddress endpoint, int backlog) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_URL$openConnection(Class<?> callerClass, java.net.URL that) {
        policyChecker.checkEntitlementForUrl(callerClass, that);
    }

    @Override
    public void check$java_net_URL$openConnection(Class<?> callerClass, URL that, Proxy proxy) {
        if (proxy.type() != Proxy.Type.DIRECT) {
            policyChecker.checkOutboundNetworkAccess(callerClass);
        }
        policyChecker.checkEntitlementForUrl(callerClass, that);
    }

    @Override
    public void check$java_net_URL$openStream(Class<?> callerClass, java.net.URL that) {
        policyChecker.checkEntitlementForUrl(callerClass, that);
    }

    @Override
    public void check$java_net_URL$getContent(Class<?> callerClass, java.net.URL that) {
        policyChecker.checkEntitlementForUrl(callerClass, that);
    }

    @Override
    public void check$java_net_URL$getContent(Class<?> callerClass, java.net.URL that, Class<?>[] classes) {
        policyChecker.checkEntitlementForUrl(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getContentLength(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getContentLengthLong(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getContentType(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getContentEncoding(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getExpiration(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getDate(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getLastModified(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getHeaderFieldInt(
        Class<?> callerClass,
        java.net.URLConnection that,
        String name,
        int defaultValue
    ) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getHeaderFieldLong(
        Class<?> callerClass,
        java.net.URLConnection that,
        String name,
        long defaultValue
    ) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getHeaderFieldDate(
        Class<?> callerClass,
        java.net.URLConnection that,
        String name,
        long defaultValue
    ) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getContent(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_URLConnection$getContent(Class<?> callerClass, java.net.URLConnection that, Class<?>[] classes) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$java_net_HttpURLConnection$getResponseCode(Class<?> callerClass, java.net.HttpURLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_HttpURLConnection$getResponseMessage(Class<?> callerClass, java.net.HttpURLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_net_HttpURLConnection$getHeaderFieldDate(
        Class<?> callerClass,
        java.net.HttpURLConnection that,
        String name,
        long defaultValue
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    // Using java.net.URLConnection for "that" as sun.net.www.URLConnection is not exported
    @Override
    public void check$sun_net_www_URLConnection$getHeaderField(Class<?> callerClass, java.net.URLConnection that, String name) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$sun_net_www_URLConnection$getHeaderFields(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$sun_net_www_URLConnection$getHeaderFieldKey(Class<?> callerClass, java.net.URLConnection that, int n) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$sun_net_www_URLConnection$getHeaderField(Class<?> callerClass, java.net.URLConnection that, int n) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$sun_net_www_URLConnection$getContentType(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$sun_net_www_URLConnection$getContentLength(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkEntitlementForURLConnection(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_ftp_FtpURLConnection$connect(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_ftp_FtpURLConnection$getInputStream(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_ftp_FtpURLConnection$getOutputStream(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$$openConnectionCheckRedirects(
        Class<?> callerClass,
        java.net.URLConnection c
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$connect(Class<?> callerClass, java.net.HttpURLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$getOutputStream(Class<?> callerClass, java.net.HttpURLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$getInputStream(Class<?> callerClass, java.net.HttpURLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$getErrorStream(Class<?> callerClass, java.net.HttpURLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$getHeaderField(
        Class<?> callerClass,
        java.net.HttpURLConnection that,
        String name
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$getHeaderFields(Class<?> callerClass, java.net.HttpURLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$getHeaderField(
        Class<?> callerClass,
        java.net.HttpURLConnection that,
        int n
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_http_HttpURLConnection$getHeaderFieldKey(
        Class<?> callerClass,
        java.net.HttpURLConnection that,
        int n
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$connect(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getOutputStream(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getInputStream(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getErrorStream(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderField(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        String name
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFields(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderField(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        int n
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFieldKey(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        int n
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getResponseCode(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getResponseMessage(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContentLength(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContentLengthLong(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContentType(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContentEncoding(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getExpiration(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getDate(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getLastModified(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFieldInt(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        String name,
        int defaultValue
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFieldLong(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        String name,
        long defaultValue
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFieldDate(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        String name,
        long defaultValue
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContent(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContent(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        Class<?>[] classes
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_https_AbstractDelegateHttpsURLConnection$connect(
        Class<?> callerClass,
        java.net.HttpURLConnection that
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_mailto_MailToURLConnection$connect(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_net_www_protocol_mailto_MailToURLConnection$getOutputStream(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientImpl$send(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest request,
        HttpResponse.BodyHandler<?> responseBodyHandler
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientImpl$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientImpl$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler,
        HttpResponse.PushPromiseHandler<?> pushPromiseHandler
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientFacade$send(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest request,
        HttpResponse.BodyHandler<?> responseBodyHandler
    ) {
        check$jdk_internal_net_http_HttpClientImpl$send(callerClass, that, request, responseBodyHandler);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientFacade$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler
    ) {
        check$jdk_internal_net_http_HttpClientImpl$sendAsync(callerClass, that, userRequest, responseHandler);
    }

    @Override
    public void check$jdk_internal_net_http_HttpClientFacade$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler,
        HttpResponse.PushPromiseHandler<?> pushPromiseHandler
    ) {
        check$jdk_internal_net_http_HttpClientImpl$sendAsync(callerClass, that, userRequest, responseHandler, pushPromiseHandler);
    }

    @Override
    public void check$java_security_cert_CertStore$$getInstance(Class<?> callerClass, String type, CertStoreParameters params) {
        // We need to check "just" the LDAPCertStore instantiation: this is the CertStore that will try to perform a network operation
        // (connect to an LDAP server). But LDAPCertStore is internal (created via SPI), so we instrument the general factory instead and
        // then do the check only for the path that leads to sensitive code (by looking at the `type` parameter).
        if ("LDAP".equals(type)) {
            policyChecker.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$java_nio_channels_spi_AbstractSelectableChannel$register(
        Class<?> callerClass,
        SelectableChannel that,
        Selector sel,
        int ops,
        Object att
    ) {
        check$java_nio_channels_SelectableChannel$register(callerClass, that, sel, ops);
    }

    @Override
    public void check$java_nio_channels_SelectableChannel$register(Class<?> callerClass, SelectableChannel that, Selector sel, int ops) {
        if ((ops & SelectionKey.OP_CONNECT) != 0) {
            policyChecker.checkOutboundNetworkAccess(callerClass);
        }
        if ((ops & SelectionKey.OP_ACCEPT) != 0) {
            policyChecker.checkInboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$java_nio_channels_AsynchronousServerSocketChannel$bind(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        SocketAddress local
    ) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$bind(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        SocketAddress local,
        int backlog
    ) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousSocketChannelImpl$bind(
        Class<?> callerClass,
        AsynchronousSocketChannel that,
        SocketAddress local
    ) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_DatagramChannelImpl$bind(Class<?> callerClass, DatagramChannel that, SocketAddress local) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_nio_channels_ServerSocketChannel$bind(Class<?> callerClass, ServerSocketChannel that, SocketAddress local) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_ServerSocketChannelImpl$bind(
        Class<?> callerClass,
        ServerSocketChannel that,
        SocketAddress local,
        int backlog
    ) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_nio_channels_SocketChannel$$open(Class<?> callerClass) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_nio_channels_SocketChannel$$open(Class<?> callerClass, ProtocolFamily family) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_nio_channels_SocketChannel$$open(Class<?> callerClass, SocketAddress remote) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_SocketChannelImpl$bind(Class<?> callerClass, SocketChannel that, SocketAddress local) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_SocketChannelImpl$connect(Class<?> callerClass, SocketChannel that, SocketAddress remote) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousSocketChannelImpl$connect(
        Class<?> callerClass,
        AsynchronousSocketChannel that,
        SocketAddress remote
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousSocketChannelImpl$connect(
        Class<?> callerClass,
        AsynchronousSocketChannel that,
        SocketAddress remote,
        Object attachment,
        CompletionHandler<Void, Object> handler
    ) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_DatagramChannelImpl$connect(Class<?> callerClass, DatagramChannel that, SocketAddress remote) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_ServerSocketChannelImpl$accept(Class<?> callerClass, ServerSocketChannel that) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$accept(Class<?> callerClass, AsynchronousServerSocketChannel that) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$accept(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        Object attachment,
        CompletionHandler<AsynchronousSocketChannel, Object> handler
    ) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$sun_nio_ch_DatagramChannelImpl$send(
        Class<?> callerClass,
        DatagramChannel that,
        ByteBuffer src,
        SocketAddress target
    ) {
        if (target instanceof InetSocketAddress isa && isa.getAddress().isMulticastAddress()) {
            policyChecker.checkAllNetworkAccess(callerClass);
        } else {
            policyChecker.checkOutboundNetworkAccess(callerClass);
        }
    }

    @Override
    public void check$sun_nio_ch_DatagramChannelImpl$receive(Class<?> callerClass, DatagramChannel that, ByteBuffer dst) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_nio_channels_spi_SelectorProvider$(Class<?> callerClass) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void check$java_nio_channels_spi_AsynchronousChannelProvider$(Class<?> callerClass) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void checkSelectorProviderInheritedChannel(Class<?> callerClass, SelectorProvider that) {
        policyChecker.checkChangeNetworkHandling(callerClass);
    }

    @Override
    public void checkSelectorProviderOpenDatagramChannel(Class<?> callerClass, SelectorProvider that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void checkSelectorProviderOpenDatagramChannel(Class<?> callerClass, SelectorProvider that, ProtocolFamily family) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void checkSelectorProviderOpenServerSocketChannel(Class<?> callerClass, SelectorProvider that) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void checkSelectorProviderOpenServerSocketChannel(Class<?> callerClass, SelectorProvider that, ProtocolFamily family) {
        policyChecker.checkInboundNetworkAccess(callerClass);
    }

    @Override
    public void checkSelectorProviderOpenSocketChannel(Class<?> callerClass, SelectorProvider that) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void checkSelectorProviderOpenSocketChannel(Class<?> callerClass, SelectorProvider that, ProtocolFamily family) {
        policyChecker.checkOutboundNetworkAccess(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$load(Class<?> callerClass, Runtime that, String filename) {
        policyChecker.checkFileRead(callerClass, Path.of(filename));
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_Runtime$loadLibrary(Class<?> callerClass, Runtime that, String libname) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_System$$load(Class<?> callerClass, String filename) {
        policyChecker.checkFileRead(callerClass, Path.of(filename));
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_System$$loadLibrary(Class<?> callerClass, String libname) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_ModuleLayer$Controller$enableNativeAccess(
        Class<?> callerClass,
        ModuleLayer.Controller that,
        Module target
    ) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    /// /////////////////
    //
    // File access
    //

    // old io (ie File)

    @Override
    public void check$java_io_File$canExecute(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$canRead(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$canWrite(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$createNewFile(Class<?> callerClass, File file) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$$createTempFile(Class<?> callerClass, String prefix, String suffix, File directory) {
        policyChecker.checkFileWrite(callerClass, directory);
    }

    @Override
    public void check$java_io_File$delete(Class<?> callerClass, File file) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$deleteOnExit(Class<?> callerClass, File file) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$exists(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$isDirectory(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$isFile(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$isHidden(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$lastModified(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$length(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$list(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$list(Class<?> callerClass, File file, FilenameFilter filter) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$listFiles(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$listFiles(Class<?> callerClass, File file, FileFilter filter) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$listFiles(Class<?> callerClass, File file, FilenameFilter filter) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_File$mkdir(Class<?> callerClass, File file) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$mkdirs(Class<?> callerClass, File file) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$renameTo(Class<?> callerClass, File file, File dest) {
        policyChecker.checkFileRead(callerClass, file);
        policyChecker.checkFileWrite(callerClass, dest);
    }

    @Override
    public void check$java_io_File$setExecutable(Class<?> callerClass, File file, boolean executable) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setExecutable(Class<?> callerClass, File file, boolean executable, boolean ownerOnly) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setLastModified(Class<?> callerClass, File file, long time) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setReadable(Class<?> callerClass, File file, boolean readable) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setReadable(Class<?> callerClass, File file, boolean readable, boolean ownerOnly) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setReadOnly(Class<?> callerClass, File file) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setWritable(Class<?> callerClass, File file, boolean writable) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_File$setWritable(Class<?> callerClass, File file, boolean writable, boolean ownerOnly) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileInputStream$(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_FileInputStream$(Class<?> callerClass, FileDescriptor fd) {
        policyChecker.checkFileDescriptorRead(callerClass);
    }

    @Override
    public void check$java_io_FileInputStream$(Class<?> callerClass, String name) {
        policyChecker.checkFileRead(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, String name) {
        policyChecker.checkFileWrite(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, String name, boolean append) {
        policyChecker.checkFileWrite(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, File file) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, File file, boolean append) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileOutputStream$(Class<?> callerClass, FileDescriptor fd) {
        policyChecker.checkFileDescriptorWrite(callerClass);
    }

    @Override
    public void check$java_io_FileReader$(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_FileReader$(Class<?> callerClass, File file, Charset charset) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_io_FileReader$(Class<?> callerClass, FileDescriptor fd) {
        policyChecker.checkFileDescriptorRead(callerClass);
    }

    @Override
    public void check$java_io_FileReader$(Class<?> callerClass, String name) {
        policyChecker.checkFileRead(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileReader$(Class<?> callerClass, String name, Charset charset) {
        policyChecker.checkFileRead(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, File file) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, File file, boolean append) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, File file, Charset charset) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, File file, Charset charset, boolean append) {
        policyChecker.checkFileWrite(callerClass, file);
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, FileDescriptor fd) {
        policyChecker.checkFileDescriptorWrite(callerClass);
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, String name) {
        policyChecker.checkFileWrite(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, String name, boolean append) {
        policyChecker.checkFileWrite(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, String name, Charset charset) {
        policyChecker.checkFileWrite(callerClass, new File(name));
    }

    @Override
    public void check$java_io_FileWriter$(Class<?> callerClass, String name, Charset charset, boolean append) {
        policyChecker.checkFileWrite(callerClass, new File(name));
    }

    @Override
    public void check$java_io_RandomAccessFile$(Class<?> callerClass, String name, String mode) {
        if (mode.equals("r")) {
            policyChecker.checkFileRead(callerClass, new File(name));
        } else {
            policyChecker.checkFileWrite(callerClass, new File(name));
        }
    }

    @Override
    public void check$java_io_RandomAccessFile$(Class<?> callerClass, File file, String mode) {
        if (mode.equals("r")) {
            policyChecker.checkFileRead(callerClass, file);
        } else {
            policyChecker.checkFileWrite(callerClass, file);
        }
    }

    @Override
    public void check$java_security_KeyStore$$getInstance(Class<?> callerClass, File file, char[] password) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_security_KeyStore$$getInstance(Class<?> callerClass, File file, KeyStore.LoadStoreParameter param) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_security_KeyStore$Builder$$newInstance(
        Class<?> callerClass,
        File file,
        KeyStore.ProtectionParameter protection
    ) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_security_KeyStore$Builder$$newInstance(
        Class<?> callerClass,
        String type,
        Provider provider,
        File file,
        KeyStore.ProtectionParameter protection
    ) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_util_Scanner$(Class<?> callerClass, File source) {
        policyChecker.checkFileRead(callerClass, source);
    }

    @Override
    public void check$java_util_Scanner$(Class<?> callerClass, File source, String charsetName) {
        policyChecker.checkFileRead(callerClass, source);
    }

    @Override
    public void check$java_util_Scanner$(Class<?> callerClass, File source, Charset charset) {
        policyChecker.checkFileRead(callerClass, source);
    }

    @Override
    public void check$java_util_jar_JarFile$(Class<?> callerClass, String name) {
        policyChecker.checkFileRead(callerClass, new File(name));
    }

    @Override
    public void check$java_util_jar_JarFile$(Class<?> callerClass, String name, boolean verify) {
        policyChecker.checkFileRead(callerClass, new File(name));
    }

    @Override
    public void check$java_util_jar_JarFile$(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_util_jar_JarFile$(Class<?> callerClass, File file, boolean verify) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_util_jar_JarFile$(Class<?> callerClass, File file, boolean verify, int mode) {
        policyChecker.checkFileWithZipMode(callerClass, file, mode);
    }

    @Override
    public void check$java_util_jar_JarFile$(Class<?> callerClass, File file, boolean verify, int mode, Runtime.Version version) {
        policyChecker.checkFileWithZipMode(callerClass, file, mode);
    }

    @Override
    public void check$java_util_zip_ZipFile$(Class<?> callerClass, String name) {
        policyChecker.checkFileRead(callerClass, new File(name));
    }

    @Override
    public void check$java_util_zip_ZipFile$(Class<?> callerClass, String name, Charset charset) {
        policyChecker.checkFileRead(callerClass, new File(name));
    }

    @Override
    public void check$java_util_zip_ZipFile$(Class<?> callerClass, File file) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_util_zip_ZipFile$(Class<?> callerClass, File file, int mode) {
        policyChecker.checkFileWithZipMode(callerClass, file, mode);
    }

    @Override
    public void check$java_util_zip_ZipFile$(Class<?> callerClass, File file, Charset charset) {
        policyChecker.checkFileRead(callerClass, file);
    }

    @Override
    public void check$java_util_zip_ZipFile$(Class<?> callerClass, File file, int mode, Charset charset) {
        policyChecker.checkFileWithZipMode(callerClass, file, mode);
    }

    // nio

    @Override
    public void check$java_nio_channels_FileChannel$(Class<?> callerClass) {
        policyChecker.checkChangeFilesHandling(callerClass);
    }

    @Override
    public void check$java_nio_channels_FileChannel$$open(
        Class<?> callerClass,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void check$java_nio_channels_FileChannel$$open(Class<?> callerClass, Path path, OpenOption... options) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void check$java_nio_channels_AsynchronousFileChannel$(Class<?> callerClass) {
        policyChecker.checkChangeFilesHandling(callerClass);
    }

    @Override
    public void check$java_nio_channels_AsynchronousFileChannel$$open(
        Class<?> callerClass,
        Path path,
        Set<? extends OpenOption> options,
        ExecutorService executor,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void check$java_nio_channels_AsynchronousFileChannel$$open(Class<?> callerClass, Path path, OpenOption... options) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void check$jdk_nio_Channels$$readWriteSelectableChannel(
        Class<?> callerClass,
        FileDescriptor fd,
        Channels.SelectableChannelCloser closer
    ) {
        policyChecker.checkFileDescriptorWrite(callerClass);
    }

    @Override
    public void check$java_nio_file_Files$$getOwner(Class<?> callerClass, Path path, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$probeContentType(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$setOwner(Class<?> callerClass, Path path, UserPrincipal principal) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$newInputStream(Class<?> callerClass, Path path, OpenOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$newOutputStream(Class<?> callerClass, Path path, OpenOption... options) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$newByteChannel(
        Class<?> callerClass,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void check$java_nio_file_Files$$newByteChannel(Class<?> callerClass, Path path, OpenOption... options) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void check$java_nio_file_Files$$newDirectoryStream(Class<?> callerClass, Path dir) {
        policyChecker.checkFileRead(callerClass, dir);
    }

    @Override
    public void check$java_nio_file_Files$$newDirectoryStream(Class<?> callerClass, Path dir, String glob) {
        policyChecker.checkFileRead(callerClass, dir);
    }

    @Override
    public void check$java_nio_file_Files$$newDirectoryStream(Class<?> callerClass, Path dir, DirectoryStream.Filter<? super Path> filter) {
        policyChecker.checkFileRead(callerClass, dir);
    }

    @Override
    public void check$java_nio_file_Files$$createFile(Class<?> callerClass, Path path, FileAttribute<?>... attrs) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$createDirectory(Class<?> callerClass, Path dir, FileAttribute<?>... attrs) {
        policyChecker.checkFileWrite(callerClass, dir);
    }

    @Override
    public void check$java_nio_file_Files$$createDirectories(Class<?> callerClass, Path dir, FileAttribute<?>... attrs) {
        policyChecker.checkFileWrite(callerClass, dir);
    }

    @Override
    public void check$java_nio_file_Files$$createTempFile(
        Class<?> callerClass,
        Path dir,
        String prefix,
        String suffix,
        FileAttribute<?>... attrs
    ) {
        policyChecker.checkFileWrite(callerClass, dir);
    }

    @Override
    public void check$java_nio_file_Files$$createTempFile(Class<?> callerClass, String prefix, String suffix, FileAttribute<?>... attrs) {
        policyChecker.checkCreateTempFile(callerClass);
    }

    @Override
    public void check$java_nio_file_Files$$createTempDirectory(Class<?> callerClass, Path dir, String prefix, FileAttribute<?>... attrs) {
        policyChecker.checkFileWrite(callerClass, dir);
    }

    @Override
    public void check$java_nio_file_Files$$createTempDirectory(Class<?> callerClass, String prefix, FileAttribute<?>... attrs) {
        policyChecker.checkCreateTempFile(callerClass);
    }

    private static Path resolveLinkTarget(Path path, Path target) {
        var parent = path.getParent();
        return parent == null ? target : parent.resolve(target);
    }

    @Override
    public void check$java_nio_file_Files$$createSymbolicLink(Class<?> callerClass, Path link, Path target, FileAttribute<?>... attrs) {
        policyChecker.checkFileWrite(callerClass, link);
        policyChecker.checkFileRead(callerClass, resolveLinkTarget(link, target));
    }

    @Override
    public void check$java_nio_file_Files$$createLink(Class<?> callerClass, Path link, Path existing) {
        policyChecker.checkFileWrite(callerClass, link);
        policyChecker.checkFileRead(callerClass, resolveLinkTarget(link, existing));
    }

    @Override
    public void check$java_nio_file_Files$$delete(Class<?> callerClass, Path path) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$deleteIfExists(Class<?> callerClass, Path path) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$copy(Class<?> callerClass, Path source, Path target, CopyOption... options) {
        policyChecker.checkFileRead(callerClass, source);
        policyChecker.checkFileWrite(callerClass, target);
    }

    @Override
    public void check$java_nio_file_Files$$move(Class<?> callerClass, Path source, Path target, CopyOption... options) {
        policyChecker.checkFileWrite(callerClass, source);
        policyChecker.checkFileWrite(callerClass, target);
    }

    @Override
    public void check$java_nio_file_Files$$readSymbolicLink(Class<?> callerClass, Path link) {
        policyChecker.checkFileRead(callerClass, link);
    }

    @Override
    public void check$java_nio_file_Files$$getFileStore(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$isSameFile(Class<?> callerClass, Path path, Path path2) {
        policyChecker.checkFileRead(callerClass, path);
        policyChecker.checkFileRead(callerClass, path2);
    }

    @Override
    public void check$java_nio_file_Files$$mismatch(Class<?> callerClass, Path path, Path path2) {
        policyChecker.checkFileRead(callerClass, path);
        policyChecker.checkFileRead(callerClass, path2);
    }

    @Override
    public void check$java_nio_file_Files$$isHidden(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$getFileAttributeView(
        Class<?> callerClass,
        Path path,
        Class<? extends FileAttributeView> type,
        LinkOption... options
    ) {
        policyChecker.checkGetFileAttributeView(callerClass);
    }

    @Override
    public void check$java_nio_file_Files$$readAttributes(
        Class<?> callerClass,
        Path path,
        Class<? extends BasicFileAttributes> type,
        LinkOption... options
    ) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$setAttribute(
        Class<?> callerClass,
        Path path,
        String attribute,
        Object value,
        LinkOption... options
    ) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$getAttribute(Class<?> callerClass, Path path, String attribute, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$readAttributes(Class<?> callerClass, Path path, String attributes, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$getPosixFilePermissions(Class<?> callerClass, Path path, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$setPosixFilePermissions(Class<?> callerClass, Path path, Set<PosixFilePermission> perms) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$isSymbolicLink(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$isDirectory(Class<?> callerClass, Path path, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$isRegularFile(Class<?> callerClass, Path path, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$getLastModifiedTime(Class<?> callerClass, Path path, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$setLastModifiedTime(Class<?> callerClass, Path path, FileTime time) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$size(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$exists(Class<?> callerClass, Path path, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$notExists(Class<?> callerClass, Path path, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$isReadable(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$isWritable(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$isExecutable(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$walkFileTree(
        Class<?> callerClass,
        Path start,
        Set<FileVisitOption> options,
        int maxDepth,
        FileVisitor<? super Path> visitor
    ) {
        policyChecker.checkFileRead(callerClass, start);
    }

    @Override
    public void check$java_nio_file_Files$$walkFileTree(Class<?> callerClass, Path start, FileVisitor<? super Path> visitor) {
        policyChecker.checkFileRead(callerClass, start);
    }

    @Override
    public void check$java_nio_file_Files$$newBufferedReader(Class<?> callerClass, Path path, Charset cs) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$newBufferedReader(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$newBufferedWriter(Class<?> callerClass, Path path, Charset cs, OpenOption... options) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$newBufferedWriter(Class<?> callerClass, Path path, OpenOption... options) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$copy(Class<?> callerClass, InputStream in, Path target, CopyOption... options) {
        policyChecker.checkFileWrite(callerClass, target);
    }

    @Override
    public void check$java_nio_file_Files$$copy(Class<?> callerClass, Path source, OutputStream out) {
        policyChecker.checkFileRead(callerClass, source);
    }

    @Override
    public void check$java_nio_file_Files$$readAllBytes(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$readString(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$readString(Class<?> callerClass, Path path, Charset cs) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$readAllLines(Class<?> callerClass, Path path, Charset cs) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$readAllLines(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$write(Class<?> callerClass, Path path, byte[] bytes, OpenOption... options) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$write(
        Class<?> callerClass,
        Path path,
        Iterable<? extends CharSequence> lines,
        Charset cs,
        OpenOption... options
    ) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$write(
        Class<?> callerClass,
        Path path,
        Iterable<? extends CharSequence> lines,
        OpenOption... options
    ) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$writeString(Class<?> callerClass, Path path, CharSequence csq, OpenOption... options) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$writeString(
        Class<?> callerClass,
        Path path,
        CharSequence csq,
        Charset cs,
        OpenOption... options
    ) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$list(Class<?> callerClass, Path dir) {
        policyChecker.checkFileRead(callerClass, dir);
    }

    @Override
    public void check$java_nio_file_Files$$walk(Class<?> callerClass, Path start, int maxDepth, FileVisitOption... options) {
        policyChecker.checkFileRead(callerClass, start);
    }

    @Override
    public void check$java_nio_file_Files$$walk(Class<?> callerClass, Path start, FileVisitOption... options) {
        policyChecker.checkFileRead(callerClass, start);
    }

    @Override
    public void check$java_nio_file_Files$$find(
        Class<?> callerClass,
        Path start,
        int maxDepth,
        BiPredicate<Path, BasicFileAttributes> matcher,
        FileVisitOption... options
    ) {
        policyChecker.checkFileRead(callerClass, start);
    }

    @Override
    public void check$java_nio_file_Files$$lines(Class<?> callerClass, Path path, Charset cs) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_nio_file_Files$$lines(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    // file system providers

    @Override
    public void check$java_nio_file_spi_FileSystemProvider$(Class<?> callerClass) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void check$java_util_logging_FileHandler$(Class<?> callerClass) {
        policyChecker.checkLoggingFileHandler(callerClass);
    }

    @Override
    public void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern) {
        policyChecker.checkLoggingFileHandler(callerClass);
    }

    @Override
    public void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern, boolean append) {
        policyChecker.checkLoggingFileHandler(callerClass);
    }

    @Override
    public void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern, int limit, int count) {
        policyChecker.checkLoggingFileHandler(callerClass);
    }

    @Override
    public void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern, int limit, int count, boolean append) {
        policyChecker.checkLoggingFileHandler(callerClass);
    }

    @Override
    public void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern, long limit, int count, boolean append) {
        policyChecker.checkLoggingFileHandler(callerClass);
    }

    @Override
    public void check$java_util_logging_FileHandler$close(Class<?> callerClass, FileHandler that) {
        // Note that there's no IT test for this one, because there's no way to create
        // a FileHandler. However, we have this check just in case someone does manage
        // to get their hands on a FileHandler and uses close() to cause its lock file to be deleted.
        policyChecker.checkLoggingFileHandler(callerClass);
    }

    @Override
    public void check$java_net_http_HttpRequest$BodyPublishers$$ofFile(Class<?> callerClass, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void check$java_net_http_HttpResponse$BodyHandlers$$ofFile(Class<?> callerClass, Path path) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_net_http_HttpResponse$BodyHandlers$$ofFile(Class<?> callerClass, Path path, OpenOption... options) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void check$java_net_http_HttpResponse$BodyHandlers$$ofFileDownload(
        Class<?> callerClass,
        Path directory,
        OpenOption... openOptions
    ) {
        policyChecker.checkFileWrite(callerClass, directory);
    }

    @Override
    public void check$java_net_http_HttpResponse$BodySubscribers$$ofFile(Class<?> callerClass, Path directory) {
        policyChecker.checkFileWrite(callerClass, directory);
    }

    @Override
    public void check$java_net_http_HttpResponse$BodySubscribers$$ofFile(Class<?> callerClass, Path directory, OpenOption... openOptions) {
        policyChecker.checkFileWrite(callerClass, directory);
    }

    @Override
    public void checkNewFileSystem(Class<?> callerClass, FileSystemProvider that, URI uri, Map<String, ?> env) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void checkNewFileSystem(Class<?> callerClass, FileSystemProvider that, Path path, Map<String, ?> env) {
        policyChecker.checkChangeJVMGlobalState(callerClass);
    }

    @Override
    public void checkNewInputStream(Class<?> callerClass, FileSystemProvider that, Path path, OpenOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void checkNewOutputStream(Class<?> callerClass, FileSystemProvider that, Path path, OpenOption... options) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    private static boolean isOpenForWrite(Set<? extends OpenOption> options) {
        return options.contains(StandardOpenOption.WRITE)
            || options.contains(StandardOpenOption.APPEND)
            || options.contains(StandardOpenOption.CREATE)
            || options.contains(StandardOpenOption.CREATE_NEW)
            || options.contains(StandardOpenOption.DELETE_ON_CLOSE);
    }

    private static boolean isOpenForWrite(OpenOption... options) {
        return Arrays.stream(options)
            .anyMatch(
                o -> o.equals(StandardOpenOption.WRITE)
                    || o.equals(StandardOpenOption.APPEND)
                    || o.equals(StandardOpenOption.CREATE)
                    || o.equals(StandardOpenOption.CREATE_NEW)
                    || o.equals(StandardOpenOption.DELETE_ON_CLOSE)
            );
    }

    @Override
    public void checkNewFileChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void checkNewAsynchronousFileChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        ExecutorService executor,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void checkNewByteChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    ) {
        if (isOpenForWrite(options)) {
            policyChecker.checkFileWrite(callerClass, path);
        } else {
            policyChecker.checkFileRead(callerClass, path);
        }
    }

    @Override
    public void checkNewDirectoryStream(
        Class<?> callerClass,
        FileSystemProvider that,
        Path dir,
        DirectoryStream.Filter<? super Path> filter
    ) {
        policyChecker.checkFileRead(callerClass, dir);
    }

    @Override
    public void checkCreateDirectory(Class<?> callerClass, FileSystemProvider that, Path dir, FileAttribute<?>... attrs) {
        policyChecker.checkFileWrite(callerClass, dir);
    }

    @Override
    public void checkCreateSymbolicLink(Class<?> callerClass, FileSystemProvider that, Path link, Path target, FileAttribute<?>... attrs) {
        policyChecker.checkFileWrite(callerClass, link);
        policyChecker.checkFileRead(callerClass, resolveLinkTarget(link, target));
    }

    @Override
    public void checkCreateLink(Class<?> callerClass, FileSystemProvider that, Path link, Path existing) {
        policyChecker.checkFileWrite(callerClass, link);
        policyChecker.checkFileRead(callerClass, resolveLinkTarget(link, existing));
    }

    @Override
    public void checkDelete(Class<?> callerClass, FileSystemProvider that, Path path) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void checkDeleteIfExists(Class<?> callerClass, FileSystemProvider that, Path path) {
        policyChecker.checkFileWrite(callerClass, path);
    }

    @Override
    public void checkReadSymbolicLink(Class<?> callerClass, FileSystemProvider that, Path link) {
        policyChecker.checkFileRead(callerClass, link);
    }

    @Override
    public void checkCopy(Class<?> callerClass, FileSystemProvider that, Path source, Path target, CopyOption... options) {
        policyChecker.checkFileWrite(callerClass, target);
        policyChecker.checkFileRead(callerClass, source);
    }

    @Override
    public void checkMove(Class<?> callerClass, FileSystemProvider that, Path source, Path target, CopyOption... options) {
        policyChecker.checkFileWrite(callerClass, target);
        policyChecker.checkFileWrite(callerClass, source);
    }

    @Override
    public void checkIsSameFile(Class<?> callerClass, FileSystemProvider that, Path path, Path path2) {
        policyChecker.checkFileRead(callerClass, path);
        policyChecker.checkFileRead(callerClass, path2);
    }

    @Override
    public void checkIsHidden(Class<?> callerClass, FileSystemProvider that, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void checkGetFileStore(Class<?> callerClass, FileSystemProvider that, Path path) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void checkCheckAccess(Class<?> callerClass, FileSystemProvider that, Path path, AccessMode... modes) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void checkGetFileAttributeView(Class<?> callerClass, FileSystemProvider that, Path path, Class<?> type, LinkOption... options) {
        policyChecker.checkGetFileAttributeView(callerClass);
    }

    @Override
    public void checkReadAttributes(Class<?> callerClass, FileSystemProvider that, Path path, Class<?> type, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void checkReadAttributes(Class<?> callerClass, FileSystemProvider that, Path path, String attributes, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void checkSetAttribute(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        String attribute,
        Object value,
        LinkOption... options
    ) {
        policyChecker.checkFileWrite(callerClass, path);

    }

    // Thread management

    @Override
    public void check$java_lang_Thread$start(Class<?> callerClass, Thread thread) {
        policyChecker.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_Thread$setDaemon(Class<?> callerClass, Thread thread, boolean on) {
        policyChecker.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_ThreadGroup$setDaemon(Class<?> callerClass, ThreadGroup threadGroup, boolean daemon) {
        policyChecker.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_util_concurrent_ForkJoinPool$setParallelism(Class<?> callerClass, ForkJoinPool forkJoinPool, int size) {
        policyChecker.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_Thread$setName(Class<?> callerClass, Thread thread, String name) {
        policyChecker.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_Thread$setPriority(Class<?> callerClass, Thread thread, int newPriority) {
        policyChecker.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_Thread$setUncaughtExceptionHandler(
        Class<?> callerClass,
        Thread thread,
        Thread.UncaughtExceptionHandler ueh
    ) {
        policyChecker.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void check$java_lang_ThreadGroup$setMaxPriority(Class<?> callerClass, ThreadGroup threadGroup, int pri) {
        policyChecker.checkManageThreadsEntitlement(callerClass);
    }

    @Override
    public void checkGetFileStoreAttributeView(Class<?> callerClass, FileStore that, Class<?> type) {
        policyChecker.checkWriteStoreAttributes(callerClass);
    }

    @Override
    public void checkGetAttribute(Class<?> callerClass, FileStore that, String attribute) {
        policyChecker.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkGetBlockSize(Class<?> callerClass, FileStore that) {
        policyChecker.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkGetTotalSpace(Class<?> callerClass, FileStore that) {
        policyChecker.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkGetUnallocatedSpace(Class<?> callerClass, FileStore that) {
        policyChecker.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkGetUsableSpace(Class<?> callerClass, FileStore that) {
        policyChecker.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkIsReadOnly(Class<?> callerClass, FileStore that) {
        policyChecker.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkName(Class<?> callerClass, FileStore that) {
        policyChecker.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkType(Class<?> callerClass, FileStore that) {
        policyChecker.checkReadStoreAttributes(callerClass);
    }

    @Override
    public void checkPathToRealPath(Class<?> callerClass, Path that, LinkOption... options) throws NoSuchFileException {
        boolean followLinks = true;
        for (LinkOption option : options) {
            if (option == LinkOption.NOFOLLOW_LINKS) {
                followLinks = false;
            }
        }
        policyChecker.checkFileRead(callerClass, that, followLinks);
    }

    @Override
    public void checkPathRegister(Class<?> callerClass, Path that, WatchService watcher, WatchEvent.Kind<?>... events) {
        policyChecker.checkFileRead(callerClass, that);
    }

    @Override
    public void checkPathRegister(
        Class<?> callerClass,
        Path that,
        WatchService watcher,
        WatchEvent.Kind<?>[] events,
        WatchEvent.Modifier... modifiers
    ) {
        policyChecker.checkFileRead(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$connect(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$getHeaderFields(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$getHeaderField(
        Class<?> callerClass,
        java.net.URLConnection that,
        String name
    ) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$getHeaderField(Class<?> callerClass, java.net.URLConnection that, int n) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$getContentLength(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$getContentLengthLong(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$getHeaderFieldKey(
        Class<?> callerClass,
        java.net.URLConnection that,
        int n
    ) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$getLastModified(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$sun_net_www_protocol_file_FileURLConnection$getInputStream(Class<?> callerClass, java.net.URLConnection that) {
        policyChecker.checkURLFileRead(callerClass, that.getURL());
    }

    @Override
    public void check$java_net_JarURLConnection$getManifest(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    private void checkJarURLAccess(Class<?> callerClass, JarURLConnection connection) {
        policyChecker.checkJarURLAccess(callerClass, connection);
    }

    @Override
    public void check$java_net_JarURLConnection$getJarEntry(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$java_net_JarURLConnection$getAttributes(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$java_net_JarURLConnection$getMainAttributes(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$java_net_JarURLConnection$getCertificates(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$getJarFile(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$getJarEntry(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$connect(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$getInputStream(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$getContentLength(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$getContentLengthLong(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$getContent(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$getContentType(Class<?> callerClass, java.net.JarURLConnection that) {
        checkJarURLAccess(callerClass, that);
    }

    @Override
    public void check$sun_net_www_protocol_jar_JarURLConnection$getHeaderField(
        Class<?> callerClass,
        java.net.JarURLConnection that,
        String name
    ) {
        checkJarURLAccess(callerClass, that);
    }
}
