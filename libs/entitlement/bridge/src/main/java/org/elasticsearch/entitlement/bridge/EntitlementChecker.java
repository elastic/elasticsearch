/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import jdk.nio.Channels;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.net.ContentHandlerFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.DatagramSocketImplFactory;
import java.net.FileNameMap;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.logging.FileHandler;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

@SuppressWarnings("unused") // Called from instrumentation code inserted by the Entitlements agent
public interface EntitlementChecker {

    /// /////////////////
    //
    // Exit the JVM process
    //

    void check$java_lang_Runtime$exit(Class<?> callerClass, Runtime runtime, int status);

    void check$java_lang_Runtime$halt(Class<?> callerClass, Runtime runtime, int status);

    void check$java_lang_System$$exit(Class<?> callerClass, int status);

    /// /////////////////
    //
    // create class loaders
    //

    void check$java_lang_ClassLoader$(Class<?> callerClass);

    void check$java_lang_ClassLoader$(Class<?> callerClass, ClassLoader parent);

    void check$java_lang_ClassLoader$(Class<?> callerClass, String name, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory);

    void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent);

    void check$java_net_URLClassLoader$(Class<?> callerClass, String name, URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory);

    void check$java_security_SecureClassLoader$(Class<?> callerClass);

    void check$java_security_SecureClassLoader$(Class<?> callerClass, ClassLoader parent);

    void check$java_security_SecureClassLoader$(Class<?> callerClass, String name, ClassLoader parent);

    /// /////////////////
    //
    // "setFactory" methods
    //

    void check$javax_net_ssl_HttpsURLConnection$setSSLSocketFactory(Class<?> callerClass, HttpsURLConnection conn, SSLSocketFactory sf);

    void check$javax_net_ssl_HttpsURLConnection$$setDefaultSSLSocketFactory(Class<?> callerClass, SSLSocketFactory sf);

    void check$javax_net_ssl_HttpsURLConnection$$setDefaultHostnameVerifier(Class<?> callerClass, HostnameVerifier hv);

    void check$javax_net_ssl_SSLContext$$setDefault(Class<?> callerClass, SSLContext context);

    /// /////////////////
    //
    // Process creation
    //

    void check$java_lang_ProcessBuilder$start(Class<?> callerClass, ProcessBuilder that);

    void check$java_lang_ProcessBuilder$$startPipeline(Class<?> callerClass, List<ProcessBuilder> builders);

    /// /////////////////
    //
    // System Properties and similar
    //

    void check$java_lang_System$$setProperties(Class<?> callerClass, Properties props);

    void check$java_lang_System$$setProperty(Class<?> callerClass, String key, String value);

    void check$java_lang_System$$clearProperty(Class<?> callerClass, String key);

    /// /////////////////
    //
    // JVM-wide state changes
    //

    void check$com_sun_tools_jdi_VirtualMachineManagerImpl$$virtualMachineManager(Class<?> callerClass);

    void check$java_lang_System$$setErr(Class<?> callerClass, PrintStream err);

    void check$java_lang_System$$setIn(Class<?> callerClass, InputStream in);

    void check$java_lang_System$$setOut(Class<?> callerClass, PrintStream out);

    void check$java_lang_Runtime$addShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook);

    void check$java_lang_Runtime$removeShutdownHook(Class<?> callerClass, Runtime runtime, Thread hook);

    void check$java_lang_Thread$$setDefaultUncaughtExceptionHandler(Class<?> callerClass, Thread.UncaughtExceptionHandler ueh);

    void check$java_net_DatagramSocket$$setDatagramSocketImplFactory(Class<?> callerClass, DatagramSocketImplFactory fac);

    void check$java_net_HttpURLConnection$$setFollowRedirects(Class<?> callerClass, boolean set);

    void check$java_net_ServerSocket$$setSocketFactory(Class<?> callerClass, SocketImplFactory fac);

    void check$java_net_Socket$$setSocketImplFactory(Class<?> callerClass, SocketImplFactory fac);

    void check$java_net_URL$$setURLStreamHandlerFactory(Class<?> callerClass, URLStreamHandlerFactory fac);

    void check$java_net_URLConnection$$setFileNameMap(Class<?> callerClass, FileNameMap map);

    void check$java_net_URLConnection$$setContentHandlerFactory(Class<?> callerClass, ContentHandlerFactory fac);

    void check$java_text_spi_BreakIteratorProvider$(Class<?> callerClass);

    void check$java_text_spi_CollatorProvider$(Class<?> callerClass);

    void check$java_text_spi_DateFormatProvider$(Class<?> callerClass);

    void check$java_text_spi_DateFormatSymbolsProvider$(Class<?> callerClass);

    void check$java_text_spi_DecimalFormatSymbolsProvider$(Class<?> callerClass);

    void check$java_text_spi_NumberFormatProvider$(Class<?> callerClass);

    void check$java_util_spi_CalendarDataProvider$(Class<?> callerClass);

    void check$java_util_spi_CalendarNameProvider$(Class<?> callerClass);

    void check$java_util_spi_CurrencyNameProvider$(Class<?> callerClass);

    void check$java_util_spi_LocaleNameProvider$(Class<?> callerClass);

    void check$java_util_spi_LocaleServiceProvider$(Class<?> callerClass);

    void check$java_util_spi_TimeZoneNameProvider$(Class<?> callerClass);

    void check$java_util_logging_LogManager$(Class<?> callerClass);

    void check$java_util_Locale$$setDefault(Class<?> callerClass, Locale locale);

    void check$java_util_Locale$$setDefault(Class<?> callerClass, Locale.Category category, Locale locale);

    void check$java_util_TimeZone$$setDefault(Class<?> callerClass, TimeZone zone);

    void check$jdk_tools_jlink_internal_Jlink$(Class<?> callerClass);

    void check$jdk_tools_jlink_internal_Main$$run(Class<?> callerClass, PrintWriter out, PrintWriter err, String... args);

    void check$jdk_vm_ci_services_JVMCIServiceLocator$$getProviders(Class<?> callerClass, Class<?> service);

    void check$jdk_vm_ci_services_Services$$load(Class<?> callerClass, Class<?> service);

    void check$jdk_vm_ci_services_Services$$loadSingle(Class<?> callerClass, Class<?> service, boolean required);

    void check$java_nio_charset_spi_CharsetProvider$(Class<?> callerClass);

    /// /////////////////
    //
    // Network access
    //
    void check$java_net_ProxySelector$$setDefault(Class<?> callerClass, ProxySelector ps);

    void check$java_net_ResponseCache$$setDefault(Class<?> callerClass, ResponseCache rc);

    void check$java_net_URL$(Class<?> callerClass, String protocol, String host, int port, String file, URLStreamHandler handler);

    void check$java_net_URL$(Class<?> callerClass, URL context, String spec, URLStreamHandler handler);

    void check$java_net_DatagramSocket$bind(Class<?> callerClass, DatagramSocket that, SocketAddress addr);

    void check$java_net_DatagramSocket$connect(Class<?> callerClass, DatagramSocket that, InetAddress addr);

    void check$java_net_DatagramSocket$connect(Class<?> callerClass, DatagramSocket that, SocketAddress addr);

    void check$java_net_DatagramSocket$joinGroup(Class<?> callerClass, DatagramSocket that, SocketAddress addr, NetworkInterface ni);

    void check$java_net_DatagramSocket$leaveGroup(Class<?> callerClass, DatagramSocket that, SocketAddress addr, NetworkInterface ni);

    void check$java_net_DatagramSocket$receive(Class<?> callerClass, DatagramSocket that, DatagramPacket p);

    void check$java_net_DatagramSocket$send(Class<?> callerClass, DatagramSocket that, DatagramPacket p);

    void check$java_net_MulticastSocket$joinGroup(Class<?> callerClass, MulticastSocket that, InetAddress addr);

    void check$java_net_MulticastSocket$joinGroup(Class<?> callerClass, MulticastSocket that, SocketAddress addr, NetworkInterface ni);

    void check$java_net_MulticastSocket$leaveGroup(Class<?> callerClass, MulticastSocket that, InetAddress addr);

    void check$java_net_MulticastSocket$leaveGroup(Class<?> callerClass, MulticastSocket that, SocketAddress addr, NetworkInterface ni);

    void check$java_net_MulticastSocket$send(Class<?> callerClass, MulticastSocket that, DatagramPacket p, byte ttl);

    void check$java_net_spi_InetAddressResolverProvider$(Class<?> callerClass);

    void check$java_net_spi_URLStreamHandlerProvider$(Class<?> callerClass);

    // Binding/connecting ctor
    void check$java_net_ServerSocket$(Class<?> callerClass, int port);

    void check$java_net_ServerSocket$(Class<?> callerClass, int port, int backlog);

    void check$java_net_ServerSocket$(Class<?> callerClass, int port, int backlog, InetAddress bindAddr);

    void check$java_net_ServerSocket$accept(Class<?> callerClass, ServerSocket that);

    void check$java_net_ServerSocket$implAccept(Class<?> callerClass, ServerSocket that, Socket s);

    void check$java_net_ServerSocket$bind(Class<?> callerClass, ServerSocket that, SocketAddress endpoint);

    void check$java_net_ServerSocket$bind(Class<?> callerClass, ServerSocket that, SocketAddress endpoint, int backlog);

    // Binding/connecting ctors
    void check$java_net_Socket$(Class<?> callerClass, Proxy proxy);

    void check$java_net_Socket$(Class<?> callerClass, String host, int port);

    void check$java_net_Socket$(Class<?> callerClass, InetAddress address, int port);

    void check$java_net_Socket$(Class<?> callerClass, String host, int port, InetAddress localAddr, int localPort);

    void check$java_net_Socket$(Class<?> callerClass, InetAddress address, int port, InetAddress localAddr, int localPort);

    void check$java_net_Socket$(Class<?> callerClass, String host, int port, boolean stream);

    void check$java_net_Socket$(Class<?> callerClass, InetAddress host, int port, boolean stream);

    void check$java_net_Socket$bind(Class<?> callerClass, Socket that, SocketAddress endpoint);

    void check$java_net_Socket$connect(Class<?> callerClass, Socket that, SocketAddress endpoint);

    void check$java_net_Socket$connect(Class<?> callerClass, Socket that, SocketAddress endpoint, int backlog);

    // URLConnection (java.net + sun.net.www)

    void check$java_net_URL$openConnection(Class<?> callerClass, java.net.URL that);

    void check$java_net_URL$openConnection(Class<?> callerClass, java.net.URL that, Proxy proxy);

    void check$java_net_URL$openStream(Class<?> callerClass, java.net.URL that);

    void check$java_net_URL$getContent(Class<?> callerClass, java.net.URL that);

    void check$java_net_URL$getContent(Class<?> callerClass, java.net.URL that, Class<?>[] classes);

    void check$java_net_URLConnection$getContentLength(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_URLConnection$getContentLengthLong(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_URLConnection$getContentType(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_URLConnection$getContentEncoding(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_URLConnection$getExpiration(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_URLConnection$getDate(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_URLConnection$getLastModified(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_URLConnection$getHeaderFieldInt(Class<?> callerClass, java.net.URLConnection that, String name, int defaultValue);

    void check$java_net_URLConnection$getHeaderFieldLong(Class<?> callerClass, java.net.URLConnection that, String name, long defaultValue);

    void check$java_net_URLConnection$getHeaderFieldDate(Class<?> callerClass, java.net.URLConnection that, String name, long defaultValue);

    void check$java_net_URLConnection$getContent(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_URLConnection$getContent(Class<?> callerClass, java.net.URLConnection that, Class<?>[] classes);

    void check$java_net_HttpURLConnection$getResponseCode(Class<?> callerClass, java.net.HttpURLConnection that);

    void check$java_net_HttpURLConnection$getResponseMessage(Class<?> callerClass, java.net.HttpURLConnection that);

    void check$java_net_HttpURLConnection$getHeaderFieldDate(
        Class<?> callerClass,
        java.net.HttpURLConnection that,
        String name,
        long defaultValue
    );

    // Using java.net.URLConnection for "that" as sun.net.www.* is not exported
    void check$sun_net_www_URLConnection$getHeaderField(Class<?> callerClass, java.net.URLConnection that, String name);

    void check$sun_net_www_URLConnection$getHeaderFields(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_URLConnection$getHeaderFieldKey(Class<?> callerClass, java.net.URLConnection that, int n);

    void check$sun_net_www_URLConnection$getHeaderField(Class<?> callerClass, java.net.URLConnection that, int n);

    void check$sun_net_www_URLConnection$getContentType(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_URLConnection$getContentLength(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_ftp_FtpURLConnection$connect(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_ftp_FtpURLConnection$getInputStream(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_ftp_FtpURLConnection$getOutputStream(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_http_HttpURLConnection$$openConnectionCheckRedirects(Class<?> callerClass, java.net.URLConnection c);

    void check$sun_net_www_protocol_http_HttpURLConnection$connect(Class<?> callerClass, java.net.HttpURLConnection that);

    void check$sun_net_www_protocol_http_HttpURLConnection$getOutputStream(Class<?> callerClass, java.net.HttpURLConnection that);

    void check$sun_net_www_protocol_http_HttpURLConnection$getInputStream(Class<?> callerClass, java.net.HttpURLConnection that);

    void check$sun_net_www_protocol_http_HttpURLConnection$getErrorStream(Class<?> callerClass, java.net.HttpURLConnection that);

    void check$sun_net_www_protocol_http_HttpURLConnection$getHeaderField(
        Class<?> callerClass,
        java.net.HttpURLConnection that,
        String name
    );

    void check$sun_net_www_protocol_http_HttpURLConnection$getHeaderFields(Class<?> callerClass, java.net.HttpURLConnection that);

    void check$sun_net_www_protocol_http_HttpURLConnection$getHeaderField(Class<?> callerClass, java.net.HttpURLConnection that, int n);

    void check$sun_net_www_protocol_http_HttpURLConnection$getHeaderFieldKey(Class<?> callerClass, java.net.HttpURLConnection that, int n);

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$connect(Class<?> callerClass, javax.net.ssl.HttpsURLConnection that);

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getOutputStream(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getInputStream(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getErrorStream(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderField(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        String name
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFields(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderField(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        int n
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFieldKey(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        int n
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getResponseCode(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getResponseMessage(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContentLength(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContentLengthLong(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContentType(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContentEncoding(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getExpiration(Class<?> callerClass, javax.net.ssl.HttpsURLConnection that);

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getDate(Class<?> callerClass, javax.net.ssl.HttpsURLConnection that);

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getLastModified(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFieldInt(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        String name,
        int defaultValue
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFieldLong(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        String name,
        long defaultValue
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getHeaderFieldDate(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        String name,
        long defaultValue
    );

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContent(Class<?> callerClass, javax.net.ssl.HttpsURLConnection that);

    void check$sun_net_www_protocol_https_HttpsURLConnectionImpl$getContent(
        Class<?> callerClass,
        javax.net.ssl.HttpsURLConnection that,
        Class<?>[] classes
    );

    void check$sun_net_www_protocol_https_AbstractDelegateHttpsURLConnection$connect(Class<?> callerClass, java.net.HttpURLConnection that);

    void check$sun_net_www_protocol_mailto_MailToURLConnection$connect(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_mailto_MailToURLConnection$getOutputStream(Class<?> callerClass, java.net.URLConnection that);

    // Network miscellanea

    // HttpClient#send and sendAsync are abstract, so we instrument their internal implementations
    void check$jdk_internal_net_http_HttpClientImpl$send(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest request,
        HttpResponse.BodyHandler<?> responseBodyHandler
    );

    void check$jdk_internal_net_http_HttpClientImpl$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler
    );

    void check$jdk_internal_net_http_HttpClientImpl$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler,
        HttpResponse.PushPromiseHandler<?> pushPromiseHandler
    );

    void check$jdk_internal_net_http_HttpClientFacade$send(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest request,
        HttpResponse.BodyHandler<?> responseBodyHandler
    );

    void check$jdk_internal_net_http_HttpClientFacade$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler
    );

    void check$jdk_internal_net_http_HttpClientFacade$sendAsync(
        Class<?> callerClass,
        HttpClient that,
        HttpRequest userRequest,
        HttpResponse.BodyHandler<?> responseHandler,
        HttpResponse.PushPromiseHandler<?> pushPromiseHandler
    );

    // We need to check the LDAPCertStore, as this will connect, but this is internal/created via SPI,
    // so we instrument the general factory instead and then filter in the check method implementation
    void check$java_security_cert_CertStore$$getInstance(Class<?> callerClass, String type, CertStoreParameters params);

    /* NIO
     * For NIO, we are sometime able to check a method on the public surface/interface (e.g. AsynchronousServerSocketChannel#bind)
     * but most of the time these methods are abstract in the public classes/interfaces (e.g. ServerSocketChannel#accept,
     * NetworkChannel#bind), so we are forced to implement the "impl" classes.
     * You can distinguish the 2 cases form the namespaces: java_nio_channels for the public ones, sun_nio_ch for the implementation
     * classes. When you see a check on a sun_nio_ch class/method, this means the matching method on the public class is abstract
     * (not instrumentable).
     */

    // bind

    void check$java_nio_channels_AsynchronousServerSocketChannel$bind(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        SocketAddress local
    );

    void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$bind(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        SocketAddress local,
        int backlog
    );

    void check$sun_nio_ch_AsynchronousSocketChannelImpl$bind(Class<?> callerClass, AsynchronousSocketChannel that, SocketAddress local);

    void check$sun_nio_ch_DatagramChannelImpl$bind(Class<?> callerClass, DatagramChannel that, SocketAddress local);

    void check$java_nio_channels_ServerSocketChannel$bind(Class<?> callerClass, ServerSocketChannel that, SocketAddress local);

    void check$sun_nio_ch_ServerSocketChannelImpl$bind(Class<?> callerClass, ServerSocketChannel that, SocketAddress local, int backlog);

    void check$sun_nio_ch_SocketChannelImpl$bind(Class<?> callerClass, SocketChannel that, SocketAddress local);

    // connect

    void check$sun_nio_ch_SocketChannelImpl$connect(Class<?> callerClass, SocketChannel that, SocketAddress remote);

    void check$sun_nio_ch_AsynchronousSocketChannelImpl$connect(Class<?> callerClass, AsynchronousSocketChannel that, SocketAddress remote);

    void check$sun_nio_ch_AsynchronousSocketChannelImpl$connect(
        Class<?> callerClass,
        AsynchronousSocketChannel that,
        SocketAddress remote,
        Object attachment,
        CompletionHandler<Void, Object> handler
    );

    void check$sun_nio_ch_DatagramChannelImpl$connect(Class<?> callerClass, DatagramChannel that, SocketAddress remote);

    // accept

    void check$sun_nio_ch_ServerSocketChannelImpl$accept(Class<?> callerClass, ServerSocketChannel that);

    void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$accept(Class<?> callerClass, AsynchronousServerSocketChannel that);

    void check$sun_nio_ch_AsynchronousServerSocketChannelImpl$accept(
        Class<?> callerClass,
        AsynchronousServerSocketChannel that,
        Object attachment,
        CompletionHandler<AsynchronousSocketChannel, Object> handler
    );

    // send/receive

    void check$sun_nio_ch_DatagramChannelImpl$send(Class<?> callerClass, DatagramChannel that, ByteBuffer src, SocketAddress target);

    void check$sun_nio_ch_DatagramChannelImpl$receive(Class<?> callerClass, DatagramChannel that, ByteBuffer dst);

    // providers (SPI)

    // protected constructors
    void check$java_nio_channels_spi_SelectorProvider$(Class<?> callerClass);

    void check$java_nio_channels_spi_AsynchronousChannelProvider$(Class<?> callerClass);

    // provider methods (dynamic)
    void checkSelectorProviderInheritedChannel(Class<?> callerClass, SelectorProvider that);

    /// /////////////////
    //
    // Load native libraries
    //
    // Using the list of restricted methods from https://download.java.net/java/early_access/jdk24/docs/api/restricted-list.html
    void check$java_lang_Runtime$load(Class<?> callerClass, Runtime that, String filename);

    void check$java_lang_Runtime$loadLibrary(Class<?> callerClass, Runtime that, String libname);

    void check$java_lang_System$$load(Class<?> callerClass, String filename);

    void check$java_lang_System$$loadLibrary(Class<?> callerClass, String libname);

    // Sealed implementation of java.lang.foreign.AddressLayout
    void check$jdk_internal_foreign_layout_ValueLayouts$OfAddressImpl$withTargetLayout(
        Class<?> callerClass,
        AddressLayout that,
        MemoryLayout memoryLayout
    );

    // Sealed implementation of java.lang.foreign.Linker
    void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        FunctionDescriptor function,
        Linker.Option... options
    );

    void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        MemorySegment address,
        FunctionDescriptor function,
        Linker.Option... options
    );

    void check$jdk_internal_foreign_abi_AbstractLinker$upcallStub(
        Class<?> callerClass,
        Linker that,
        MethodHandle target,
        FunctionDescriptor function,
        Arena arena,
        Linker.Option... options
    );

    // Sealed implementation for java.lang.foreign.MemorySegment.reinterpret(long)
    void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(Class<?> callerClass, MemorySegment that, long newSize);

    void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        long newSize,
        Arena arena,
        Consumer<MemorySegment> cleanup
    );

    void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        Arena arena,
        Consumer<MemorySegment> cleanup
    );

    void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, Arena arena);

    void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, Arena arena);

    void check$java_lang_ModuleLayer$Controller$enableNativeAccess(Class<?> callerClass, ModuleLayer.Controller that, Module target);

    /// /////////////////
    //
    // File access
    //

    // old io (ie File)
    void check$java_io_File$canExecute(Class<?> callerClass, File file);

    void check$java_io_File$canRead(Class<?> callerClass, File file);

    void check$java_io_File$canWrite(Class<?> callerClass, File file);

    void check$java_io_File$createNewFile(Class<?> callerClass, File file);

    void check$java_io_File$$createTempFile(Class<?> callerClass, String prefix, String suffix, File directory);

    void check$java_io_File$delete(Class<?> callerClass, File file);

    void check$java_io_File$deleteOnExit(Class<?> callerClass, File file);

    void check$java_io_File$exists(Class<?> callerClass, File file);

    void check$java_io_File$isDirectory(Class<?> callerClass, File file);

    void check$java_io_File$isFile(Class<?> callerClass, File file);

    void check$java_io_File$isHidden(Class<?> callerClass, File file);

    void check$java_io_File$lastModified(Class<?> callerClass, File file);

    void check$java_io_File$length(Class<?> callerClass, File file);

    void check$java_io_File$list(Class<?> callerClass, File file);

    void check$java_io_File$list(Class<?> callerClass, File file, FilenameFilter filter);

    void check$java_io_File$listFiles(Class<?> callerClass, File file);

    void check$java_io_File$listFiles(Class<?> callerClass, File file, FileFilter filter);

    void check$java_io_File$listFiles(Class<?> callerClass, File file, FilenameFilter filter);

    void check$java_io_File$mkdir(Class<?> callerClass, File file);

    void check$java_io_File$mkdirs(Class<?> callerClass, File file);

    void check$java_io_File$renameTo(Class<?> callerClass, File file, File dest);

    void check$java_io_File$setExecutable(Class<?> callerClass, File file, boolean executable);

    void check$java_io_File$setExecutable(Class<?> callerClass, File file, boolean executable, boolean ownerOnly);

    void check$java_io_File$setLastModified(Class<?> callerClass, File file, long time);

    void check$java_io_File$setReadable(Class<?> callerClass, File file, boolean readable);

    void check$java_io_File$setReadable(Class<?> callerClass, File file, boolean readable, boolean ownerOnly);

    void check$java_io_File$setReadOnly(Class<?> callerClass, File file);

    void check$java_io_File$setWritable(Class<?> callerClass, File file, boolean writable);

    void check$java_io_File$setWritable(Class<?> callerClass, File file, boolean writable, boolean ownerOnly);

    void check$java_io_FileInputStream$(Class<?> callerClass, File file);

    void check$java_io_FileInputStream$(Class<?> callerClass, FileDescriptor fd);

    void check$java_io_FileInputStream$(Class<?> callerClass, String name);

    void check$java_io_FileOutputStream$(Class<?> callerClass, File file);

    void check$java_io_FileOutputStream$(Class<?> callerClass, File file, boolean append);

    void check$java_io_FileOutputStream$(Class<?> callerClass, FileDescriptor fd);

    void check$java_io_FileOutputStream$(Class<?> callerClass, String name);

    void check$java_io_FileOutputStream$(Class<?> callerClass, String name, boolean append);

    void check$java_io_FileReader$(Class<?> callerClass, File file);

    void check$java_io_FileReader$(Class<?> callerClass, File file, Charset charset);

    void check$java_io_FileReader$(Class<?> callerClass, FileDescriptor fd);

    void check$java_io_FileReader$(Class<?> callerClass, String name);

    void check$java_io_FileReader$(Class<?> callerClass, String name, Charset charset);

    void check$java_io_FileWriter$(Class<?> callerClass, File file);

    void check$java_io_FileWriter$(Class<?> callerClass, File file, boolean append);

    void check$java_io_FileWriter$(Class<?> callerClass, File file, Charset charset);

    void check$java_io_FileWriter$(Class<?> callerClass, File file, Charset charset, boolean append);

    void check$java_io_FileWriter$(Class<?> callerClass, FileDescriptor fd);

    void check$java_io_FileWriter$(Class<?> callerClass, String name);

    void check$java_io_FileWriter$(Class<?> callerClass, String name, boolean append);

    void check$java_io_FileWriter$(Class<?> callerClass, String name, Charset charset);

    void check$java_io_FileWriter$(Class<?> callerClass, String name, Charset charset, boolean append);

    void check$java_io_RandomAccessFile$(Class<?> callerClass, String name, String mode);

    void check$java_io_RandomAccessFile$(Class<?> callerClass, File file, String mode);

    void check$java_security_KeyStore$$getInstance(Class<?> callerClass, File file, char[] password);

    void check$java_security_KeyStore$$getInstance(Class<?> callerClass, File file, KeyStore.LoadStoreParameter param);

    void check$java_security_KeyStore$Builder$$newInstance(Class<?> callerClass, File file, KeyStore.ProtectionParameter protection);

    void check$java_security_KeyStore$Builder$$newInstance(
        Class<?> callerClass,
        String type,
        Provider provider,
        File file,
        KeyStore.ProtectionParameter protection
    );

    void check$java_util_Scanner$(Class<?> callerClass, File source);

    void check$java_util_Scanner$(Class<?> callerClass, File source, String charsetName);

    void check$java_util_Scanner$(Class<?> callerClass, File source, Charset charset);

    void check$java_util_jar_JarFile$(Class<?> callerClass, String name);

    void check$java_util_jar_JarFile$(Class<?> callerClass, String name, boolean verify);

    void check$java_util_jar_JarFile$(Class<?> callerClass, File file);

    void check$java_util_jar_JarFile$(Class<?> callerClass, File file, boolean verify);

    void check$java_util_jar_JarFile$(Class<?> callerClass, File file, boolean verify, int mode);

    void check$java_util_jar_JarFile$(Class<?> callerClass, File file, boolean verify, int mode, Runtime.Version version);

    void check$java_util_zip_ZipFile$(Class<?> callerClass, String name);

    void check$java_util_zip_ZipFile$(Class<?> callerClass, String name, Charset charset);

    void check$java_util_zip_ZipFile$(Class<?> callerClass, File file);

    void check$java_util_zip_ZipFile$(Class<?> callerClass, File file, int mode);

    void check$java_util_zip_ZipFile$(Class<?> callerClass, File file, Charset charset);

    void check$java_util_zip_ZipFile$(Class<?> callerClass, File file, int mode, Charset charset);

    // nio
    // channels
    void check$java_nio_channels_FileChannel$(Class<?> callerClass);

    void check$java_nio_channels_FileChannel$$open(
        Class<?> callerClass,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    );

    void check$java_nio_channels_FileChannel$$open(Class<?> callerClass, Path path, OpenOption... options);

    void check$java_nio_channels_AsynchronousFileChannel$(Class<?> callerClass);

    void check$java_nio_channels_AsynchronousFileChannel$$open(
        Class<?> callerClass,
        Path path,
        Set<? extends OpenOption> options,
        ExecutorService executor,
        FileAttribute<?>... attrs
    );

    void check$java_nio_channels_AsynchronousFileChannel$$open(Class<?> callerClass, Path path, OpenOption... options);

    void check$jdk_nio_Channels$$readWriteSelectableChannel(
        Class<?> callerClass,
        FileDescriptor fd,
        Channels.SelectableChannelCloser closer
    );

    // files
    void check$java_nio_file_Files$$getOwner(Class<?> callerClass, Path path, LinkOption... options);

    void check$java_nio_file_Files$$probeContentType(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$setOwner(Class<?> callerClass, Path path, UserPrincipal principal);

    void check$java_nio_file_Files$$newInputStream(Class<?> callerClass, Path path, OpenOption... options);

    void check$java_nio_file_Files$$newOutputStream(Class<?> callerClass, Path path, OpenOption... options);

    void check$java_nio_file_Files$$newByteChannel(
        Class<?> callerClass,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    );

    void check$java_nio_file_Files$$newByteChannel(Class<?> callerClass, Path path, OpenOption... options);

    void check$java_nio_file_Files$$newDirectoryStream(Class<?> callerClass, Path dir);

    void check$java_nio_file_Files$$newDirectoryStream(Class<?> callerClass, Path dir, String glob);

    void check$java_nio_file_Files$$newDirectoryStream(Class<?> callerClass, Path dir, DirectoryStream.Filter<? super Path> filter);

    void check$java_nio_file_Files$$createFile(Class<?> callerClass, Path path, FileAttribute<?>... attrs);

    void check$java_nio_file_Files$$createDirectory(Class<?> callerClass, Path dir, FileAttribute<?>... attrs);

    void check$java_nio_file_Files$$createDirectories(Class<?> callerClass, Path dir, FileAttribute<?>... attrs);

    void check$java_nio_file_Files$$createTempFile(Class<?> callerClass, Path dir, String prefix, String suffix, FileAttribute<?>... attrs);

    void check$java_nio_file_Files$$createTempFile(Class<?> callerClass, String prefix, String suffix, FileAttribute<?>... attrs);

    void check$java_nio_file_Files$$createTempDirectory(Class<?> callerClass, Path dir, String prefix, FileAttribute<?>... attrs);

    void check$java_nio_file_Files$$createTempDirectory(Class<?> callerClass, String prefix, FileAttribute<?>... attrs);

    void check$java_nio_file_Files$$createSymbolicLink(Class<?> callerClass, Path link, Path target, FileAttribute<?>... attrs);

    void check$java_nio_file_Files$$createLink(Class<?> callerClass, Path link, Path existing);

    void check$java_nio_file_Files$$delete(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$deleteIfExists(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$copy(Class<?> callerClass, Path source, Path target, CopyOption... options);

    void check$java_nio_file_Files$$move(Class<?> callerClass, Path source, Path target, CopyOption... options);

    void check$java_nio_file_Files$$readSymbolicLink(Class<?> callerClass, Path link);

    void check$java_nio_file_Files$$getFileStore(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$isSameFile(Class<?> callerClass, Path path, Path path2);

    void check$java_nio_file_Files$$mismatch(Class<?> callerClass, Path path, Path path2);

    void check$java_nio_file_Files$$isHidden(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$getFileAttributeView(
        Class<?> callerClass,
        Path path,
        Class<? extends FileAttributeView> type,
        LinkOption... options
    );

    void check$java_nio_file_Files$$readAttributes(
        Class<?> callerClass,
        Path path,
        Class<? extends BasicFileAttributes> type,
        LinkOption... options
    );

    void check$java_nio_file_Files$$setAttribute(Class<?> callerClass, Path path, String attribute, Object value, LinkOption... options);

    void check$java_nio_file_Files$$getAttribute(Class<?> callerClass, Path path, String attribute, LinkOption... options);

    void check$java_nio_file_Files$$readAttributes(Class<?> callerClass, Path path, String attributes, LinkOption... options);

    void check$java_nio_file_Files$$getPosixFilePermissions(Class<?> callerClass, Path path, LinkOption... options);

    void check$java_nio_file_Files$$setPosixFilePermissions(Class<?> callerClass, Path path, Set<PosixFilePermission> perms);

    void check$java_nio_file_Files$$isSymbolicLink(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$isDirectory(Class<?> callerClass, Path path, LinkOption... options);

    void check$java_nio_file_Files$$isRegularFile(Class<?> callerClass, Path path, LinkOption... options);

    void check$java_nio_file_Files$$getLastModifiedTime(Class<?> callerClass, Path path, LinkOption... options);

    void check$java_nio_file_Files$$setLastModifiedTime(Class<?> callerClass, Path path, FileTime time);

    void check$java_nio_file_Files$$size(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$exists(Class<?> callerClass, Path path, LinkOption... options);

    void check$java_nio_file_Files$$notExists(Class<?> callerClass, Path path, LinkOption... options);

    void check$java_nio_file_Files$$isReadable(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$isWritable(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$isExecutable(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$walkFileTree(
        Class<?> callerClass,
        Path start,
        Set<FileVisitOption> options,
        int maxDepth,
        FileVisitor<? super Path> visitor
    );

    void check$java_nio_file_Files$$walkFileTree(Class<?> callerClass, Path start, FileVisitor<? super Path> visitor);

    void check$java_nio_file_Files$$newBufferedReader(Class<?> callerClass, Path path, Charset cs);

    void check$java_nio_file_Files$$newBufferedReader(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$newBufferedWriter(Class<?> callerClass, Path path, Charset cs, OpenOption... options);

    void check$java_nio_file_Files$$newBufferedWriter(Class<?> callerClass, Path path, OpenOption... options);

    void check$java_nio_file_Files$$copy(Class<?> callerClass, InputStream in, Path target, CopyOption... options);

    void check$java_nio_file_Files$$copy(Class<?> callerClass, Path source, OutputStream out);

    void check$java_nio_file_Files$$readAllBytes(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$readString(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$readString(Class<?> callerClass, Path path, Charset cs);

    void check$java_nio_file_Files$$readAllLines(Class<?> callerClass, Path path, Charset cs);

    void check$java_nio_file_Files$$readAllLines(Class<?> callerClass, Path path);

    void check$java_nio_file_Files$$write(Class<?> callerClass, Path path, byte[] bytes, OpenOption... options);

    void check$java_nio_file_Files$$write(
        Class<?> callerClass,
        Path path,
        Iterable<? extends CharSequence> lines,
        Charset cs,
        OpenOption... options
    );

    void check$java_nio_file_Files$$write(Class<?> callerClass, Path path, Iterable<? extends CharSequence> lines, OpenOption... options);

    void check$java_nio_file_Files$$writeString(Class<?> callerClass, Path path, CharSequence csq, OpenOption... options);

    void check$java_nio_file_Files$$writeString(Class<?> callerClass, Path path, CharSequence csq, Charset cs, OpenOption... options);

    void check$java_nio_file_Files$$list(Class<?> callerClass, Path dir);

    void check$java_nio_file_Files$$walk(Class<?> callerClass, Path start, int maxDepth, FileVisitOption... options);

    void check$java_nio_file_Files$$walk(Class<?> callerClass, Path start, FileVisitOption... options);

    void check$java_nio_file_Files$$find(
        Class<?> callerClass,
        Path start,
        int maxDepth,
        BiPredicate<Path, BasicFileAttributes> matcher,
        FileVisitOption... options
    );

    void check$java_nio_file_Files$$lines(Class<?> callerClass, Path path, Charset cs);

    void check$java_nio_file_Files$$lines(Class<?> callerClass, Path path);

    void check$java_nio_file_spi_FileSystemProvider$(Class<?> callerClass);

    void check$java_util_logging_FileHandler$(Class<?> callerClass);

    void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern);

    void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern, boolean append);

    void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern, int limit, int count);

    void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern, int limit, int count, boolean append);

    void check$java_util_logging_FileHandler$(Class<?> callerClass, String pattern, long limit, int count, boolean append);

    void check$java_util_logging_FileHandler$close(Class<?> callerClass, FileHandler that);

    void check$java_net_http_HttpRequest$BodyPublishers$$ofFile(Class<?> callerClass, Path path);

    void check$java_net_http_HttpResponse$BodyHandlers$$ofFile(Class<?> callerClass, Path path);

    void check$java_net_http_HttpResponse$BodyHandlers$$ofFile(Class<?> callerClass, Path path, OpenOption... options);

    void check$java_net_http_HttpResponse$BodyHandlers$$ofFileDownload(Class<?> callerClass, Path directory, OpenOption... openOptions);

    void check$java_net_http_HttpResponse$BodySubscribers$$ofFile(Class<?> callerClass, Path directory);

    void check$java_net_http_HttpResponse$BodySubscribers$$ofFile(Class<?> callerClass, Path directory, OpenOption... openOptions);

    void checkNewFileSystem(Class<?> callerClass, FileSystemProvider that, URI uri, Map<String, ?> env);

    void checkNewFileSystem(Class<?> callerClass, FileSystemProvider that, Path path, Map<String, ?> env);

    void checkNewInputStream(Class<?> callerClass, FileSystemProvider that, Path path, OpenOption... options);

    void checkNewOutputStream(Class<?> callerClass, FileSystemProvider that, Path path, OpenOption... options);

    void checkNewFileChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    );

    void checkNewAsynchronousFileChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        ExecutorService executor,
        FileAttribute<?>... attrs
    );

    void checkNewByteChannel(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Set<? extends OpenOption> options,
        FileAttribute<?>... attrs
    );

    void checkNewDirectoryStream(Class<?> callerClass, FileSystemProvider that, Path dir, DirectoryStream.Filter<? super Path> filter);

    void checkCreateDirectory(Class<?> callerClass, FileSystemProvider that, Path dir, FileAttribute<?>... attrs);

    void checkCreateSymbolicLink(Class<?> callerClass, FileSystemProvider that, Path link, Path target, FileAttribute<?>... attrs);

    void checkCreateLink(Class<?> callerClass, FileSystemProvider that, Path link, Path existing);

    void checkDelete(Class<?> callerClass, FileSystemProvider that, Path path);

    void checkDeleteIfExists(Class<?> callerClass, FileSystemProvider that, Path path);

    void checkReadSymbolicLink(Class<?> callerClass, FileSystemProvider that, Path link);

    void checkCopy(Class<?> callerClass, FileSystemProvider that, Path source, Path target, CopyOption... options);

    void checkMove(Class<?> callerClass, FileSystemProvider that, Path source, Path target, CopyOption... options);

    void checkIsSameFile(Class<?> callerClass, FileSystemProvider that, Path path, Path path2);

    void checkIsHidden(Class<?> callerClass, FileSystemProvider that, Path path);

    void checkGetFileStore(Class<?> callerClass, FileSystemProvider that, Path path);

    void checkCheckAccess(Class<?> callerClass, FileSystemProvider that, Path path, AccessMode... modes);

    void checkGetFileAttributeView(Class<?> callerClass, FileSystemProvider that, Path path, Class<?> type, LinkOption... options);

    void checkReadAttributes(Class<?> callerClass, FileSystemProvider that, Path path, Class<?> type, LinkOption... options);

    void checkReadAttributes(Class<?> callerClass, FileSystemProvider that, Path path, String attributes, LinkOption... options);

    void checkReadAttributesIfExists(Class<?> callerClass, FileSystemProvider that, Path path, Class<?> type, LinkOption... options);

    void checkSetAttribute(Class<?> callerClass, FileSystemProvider that, Path path, String attribute, Object value, LinkOption... options);

    void checkExists(Class<?> callerClass, FileSystemProvider that, Path path, LinkOption... options);

    // file store
    void checkGetFileStoreAttributeView(Class<?> callerClass, FileStore that, Class<?> type);

    void checkGetAttribute(Class<?> callerClass, FileStore that, String attribute);

    void checkGetBlockSize(Class<?> callerClass, FileStore that);

    void checkGetTotalSpace(Class<?> callerClass, FileStore that);

    void checkGetUnallocatedSpace(Class<?> callerClass, FileStore that);

    void checkGetUsableSpace(Class<?> callerClass, FileStore that);

    void checkIsReadOnly(Class<?> callerClass, FileStore that);

    void checkName(Class<?> callerClass, FileStore that);

    void checkType(Class<?> callerClass, FileStore that);

    // path
    void checkPathToRealPath(Class<?> callerClass, Path that, LinkOption... options) throws NoSuchFileException;

    void checkPathRegister(Class<?> callerClass, Path that, WatchService watcher, WatchEvent.Kind<?>... events);

    void checkPathRegister(
        Class<?> callerClass,
        Path that,
        WatchService watcher,
        WatchEvent.Kind<?>[] events,
        WatchEvent.Modifier... modifiers
    );

    // URLConnection

    void check$sun_net_www_protocol_file_FileURLConnection$connect(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_file_FileURLConnection$getHeaderFields(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_file_FileURLConnection$getHeaderField(Class<?> callerClass, java.net.URLConnection that, String name);

    void check$sun_net_www_protocol_file_FileURLConnection$getHeaderField(Class<?> callerClass, java.net.URLConnection that, int n);

    void check$sun_net_www_protocol_file_FileURLConnection$getContentLength(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_file_FileURLConnection$getContentLengthLong(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_file_FileURLConnection$getHeaderFieldKey(Class<?> callerClass, java.net.URLConnection that, int n);

    void check$sun_net_www_protocol_file_FileURLConnection$getLastModified(Class<?> callerClass, java.net.URLConnection that);

    void check$sun_net_www_protocol_file_FileURLConnection$getInputStream(Class<?> callerClass, java.net.URLConnection that);

    void check$java_net_JarURLConnection$getManifest(Class<?> callerClass, java.net.JarURLConnection that);

    void check$java_net_JarURLConnection$getJarEntry(Class<?> callerClass, java.net.JarURLConnection that);

    void check$java_net_JarURLConnection$getAttributes(Class<?> callerClass, java.net.JarURLConnection that);

    void check$java_net_JarURLConnection$getMainAttributes(Class<?> callerClass, java.net.JarURLConnection that);

    void check$java_net_JarURLConnection$getCertificates(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$getJarFile(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$getJarEntry(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$connect(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$getInputStream(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$getContentLength(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$getContentLengthLong(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$getContent(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$getContentType(Class<?> callerClass, java.net.JarURLConnection that);

    void check$sun_net_www_protocol_jar_JarURLConnection$getHeaderField(Class<?> callerClass, java.net.JarURLConnection that, String name);

    ////////////////////
    //
    // Thread management
    //

    void check$java_lang_Thread$start(Class<?> callerClass, Thread thread);

    void check$java_lang_Thread$setDaemon(Class<?> callerClass, Thread thread, boolean on);

    void check$java_lang_ThreadGroup$setDaemon(Class<?> callerClass, ThreadGroup threadGroup, boolean daemon);

    void check$java_util_concurrent_ForkJoinPool$setParallelism(Class<?> callerClass, ForkJoinPool forkJoinPool, int size);

    void check$java_lang_Thread$setName(Class<?> callerClass, Thread thread, String name);

    void check$java_lang_Thread$setPriority(Class<?> callerClass, Thread thread, int newPriority);

    void check$java_lang_Thread$setUncaughtExceptionHandler(Class<?> callerClass, Thread thread, Thread.UncaughtExceptionHandler ueh);

    void check$java_lang_ThreadGroup$setMaxPriority(Class<?> callerClass, ThreadGroup threadGroup, int pri);
}
