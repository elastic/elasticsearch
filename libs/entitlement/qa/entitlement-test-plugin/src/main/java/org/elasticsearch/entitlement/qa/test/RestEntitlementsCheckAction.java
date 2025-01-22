/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyBreakIteratorProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyCalendarDataProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyCalendarNameProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyCollatorProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyCurrencyNameProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyDateFormatProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyDateFormatSymbolsProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyDecimalFormatSymbolsProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyLocaleNameProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyLocaleServiceProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyNumberFormatProvider;
import org.elasticsearch.entitlement.qa.test.DummyImplementations.DummyTimeZoneNameProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.ProxySelector;
import java.net.ResponseCache;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import static java.lang.Thread.currentThread;
import static java.util.Map.entry;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.entitlement.qa.entitled.EntitledActions.newThread;
import static org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction.CheckAction.alwaysDenied;
import static org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction.CheckAction.deniedToPlugins;
import static org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction.CheckAction.forPlugins;
import static org.elasticsearch.rest.RestRequest.Method.GET;

@SuppressWarnings("unused")
public class RestEntitlementsCheckAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckAction.class);
    public static final Thread NO_OP_SHUTDOWN_HOOK = newThread(() -> {}, "Shutdown hook for testing");

    record CheckAction(CheckedRunnable<Exception> action, boolean isAlwaysDeniedToPlugins, Integer fromJavaVersion) {
        /**
         * These cannot be granted to plugins, so our test plugins cannot test the "allowed" case.
         */
        static CheckAction deniedToPlugins(CheckedRunnable<Exception> action) {
            return new CheckAction(action, true, null);
        }

        static CheckAction forPlugins(CheckedRunnable<Exception> action) {
            return new CheckAction(action, false, null);
        }

        static CheckAction alwaysDenied(CheckedRunnable<Exception> action) {
            return new CheckAction(action, true, null);
        }
    }

    private static final Map<String, CheckAction> checkActions = Stream.<Map.Entry<String, CheckAction>>of(
        entry("runtime_exit", deniedToPlugins(RestEntitlementsCheckAction::runtimeExit)),
        entry("runtime_halt", deniedToPlugins(RestEntitlementsCheckAction::runtimeHalt)),
        entry("system_exit", deniedToPlugins(RestEntitlementsCheckAction::systemExit)),
        entry("create_classloader", forPlugins(RestEntitlementsCheckAction::createClassLoader)),
        entry("processBuilder_start", deniedToPlugins(RestEntitlementsCheckAction::processBuilder_start)),
        entry("processBuilder_startPipeline", deniedToPlugins(RestEntitlementsCheckAction::processBuilder_startPipeline)),
        entry("set_https_connection_properties", forPlugins(RestEntitlementsCheckAction::setHttpsConnectionProperties)),
        entry("set_default_ssl_socket_factory", alwaysDenied(RestEntitlementsCheckAction::setDefaultSSLSocketFactory)),
        entry("set_default_hostname_verifier", alwaysDenied(RestEntitlementsCheckAction::setDefaultHostnameVerifier)),
        entry("set_default_ssl_context", alwaysDenied(RestEntitlementsCheckAction::setDefaultSSLContext)),
        entry("system_setIn", alwaysDenied(RestEntitlementsCheckAction::system$$setIn)),
        entry("system_setOut", alwaysDenied(RestEntitlementsCheckAction::system$$setOut)),
        entry("system_setErr", alwaysDenied(RestEntitlementsCheckAction::system$$setErr)),
        entry("runtime_addShutdownHook", alwaysDenied(RestEntitlementsCheckAction::runtime$addShutdownHook)),
        entry("runtime_removeShutdownHook", alwaysDenied(RestEntitlementsCheckAction::runtime$$removeShutdownHook)),
        entry(
            "thread_setDefaultUncaughtExceptionHandler",
            alwaysDenied(RestEntitlementsCheckAction::thread$$setDefaultUncaughtExceptionHandler)
        ),
        entry("localeServiceProvider", alwaysDenied(RestEntitlementsCheckAction::localeServiceProvider$)),
        entry("breakIteratorProvider", alwaysDenied(RestEntitlementsCheckAction::breakIteratorProvider$)),
        entry("collatorProvider", alwaysDenied(RestEntitlementsCheckAction::collatorProvider$)),
        entry("dateFormatProvider", alwaysDenied(RestEntitlementsCheckAction::dateFormatProvider$)),
        entry("dateFormatSymbolsProvider", alwaysDenied(RestEntitlementsCheckAction::dateFormatSymbolsProvider$)),
        entry("decimalFormatSymbolsProvider", alwaysDenied(RestEntitlementsCheckAction::decimalFormatSymbolsProvider$)),
        entry("numberFormatProvider", alwaysDenied(RestEntitlementsCheckAction::numberFormatProvider$)),
        entry("calendarDataProvider", alwaysDenied(RestEntitlementsCheckAction::calendarDataProvider$)),
        entry("calendarNameProvider", alwaysDenied(RestEntitlementsCheckAction::calendarNameProvider$)),
        entry("currencyNameProvider", alwaysDenied(RestEntitlementsCheckAction::currencyNameProvider$)),
        entry("localeNameProvider", alwaysDenied(RestEntitlementsCheckAction::localeNameProvider$)),
        entry("timeZoneNameProvider", alwaysDenied(RestEntitlementsCheckAction::timeZoneNameProvider$)),
        entry("logManager", alwaysDenied(RestEntitlementsCheckAction::logManager$)),

        entry("locale_setDefault", alwaysDenied(WritePropertiesCheckActions::setDefaultLocale)),
        entry("locale_setDefaultForCategory", alwaysDenied(WritePropertiesCheckActions::setDefaultLocaleForCategory)),
        entry("timeZone_setDefault", alwaysDenied(WritePropertiesCheckActions::setDefaultTimeZone)),

        entry("system_setProperty", forPlugins(WritePropertiesCheckActions::setSystemProperty)),
        entry("system_clearProperty", forPlugins(WritePropertiesCheckActions::clearSystemProperty)),
        entry("system_setSystemProperties", alwaysDenied(WritePropertiesCheckActions::setSystemProperties)),

        // This group is a bit nasty: if entitlements don't prevent these, then networking is
        // irreparably borked for the remainder of the test run.
        entry(
            "datagramSocket_setDatagramSocketImplFactory",
            alwaysDenied(RestEntitlementsCheckAction::datagramSocket$$setDatagramSocketImplFactory)
        ),
        entry("httpURLConnection_setFollowRedirects", alwaysDenied(RestEntitlementsCheckAction::httpURLConnection$$setFollowRedirects)),
        entry("serverSocket_setSocketFactory", alwaysDenied(RestEntitlementsCheckAction::serverSocket$$setSocketFactory)),
        entry("socket_setSocketImplFactory", alwaysDenied(RestEntitlementsCheckAction::socket$$setSocketImplFactory)),
        entry("url_setURLStreamHandlerFactory", alwaysDenied(RestEntitlementsCheckAction::url$$setURLStreamHandlerFactory)),
        entry("urlConnection_setFileNameMap", alwaysDenied(RestEntitlementsCheckAction::urlConnection$$setFileNameMap)),
        entry("urlConnection_setContentHandlerFactory", alwaysDenied(RestEntitlementsCheckAction::urlConnection$$setContentHandlerFactory)),

        entry("proxySelector_setDefault", alwaysDenied(RestEntitlementsCheckAction::setDefaultProxySelector)),
        entry("responseCache_setDefault", alwaysDenied(RestEntitlementsCheckAction::setDefaultResponseCache)),
        entry(
            "createInetAddressResolverProvider",
            new CheckAction(VersionSpecificNetworkChecks::createInetAddressResolverProvider, true, 18)
        ),
        entry("createURLStreamHandlerProvider", alwaysDenied(RestEntitlementsCheckAction::createURLStreamHandlerProvider)),
        entry("createURLWithURLStreamHandler", alwaysDenied(RestEntitlementsCheckAction::createURLWithURLStreamHandler)),
        entry("createURLWithURLStreamHandler2", alwaysDenied(RestEntitlementsCheckAction::createURLWithURLStreamHandler2)),
        entry("datagram_socket_bind", forPlugins(RestEntitlementsCheckAction::bindDatagramSocket)),
        entry("datagram_socket_connect", forPlugins(RestEntitlementsCheckAction::connectDatagramSocket)),
        entry("datagram_socket_send", forPlugins(RestEntitlementsCheckAction::sendDatagramSocket)),
        entry("datagram_socket_receive", forPlugins(RestEntitlementsCheckAction::receiveDatagramSocket)),
        entry("datagram_socket_join_group", forPlugins(RestEntitlementsCheckAction::joinGroupDatagramSocket)),
        entry("datagram_socket_leave_group", forPlugins(RestEntitlementsCheckAction::leaveGroupDatagramSocket)),

        entry("create_socket_with_proxy", forPlugins(NetworkAccessCheckActions::createSocketWithProxy)),
        entry("socket_bind", forPlugins(NetworkAccessCheckActions::socketBind)),
        entry("socket_connect", forPlugins(NetworkAccessCheckActions::socketConnect)),
        entry("server_socket_bind", forPlugins(NetworkAccessCheckActions::serverSocketBind)),
        entry("server_socket_accept", forPlugins(NetworkAccessCheckActions::serverSocketAccept)),

        entry("url_open_connection_proxy", forPlugins(NetworkAccessCheckActions::urlOpenConnectionWithProxy)),
        entry("http_client_send", forPlugins(VersionSpecificNetworkChecks::httpClientSend)),
        entry("http_client_send_async", forPlugins(VersionSpecificNetworkChecks::httpClientSendAsync)),
        entry("create_ldap_cert_store", forPlugins(NetworkAccessCheckActions::createLDAPCertStore)),

        entry("server_socket_channel_bind", forPlugins(NetworkAccessCheckActions::serverSocketChannelBind)),
        entry("server_socket_channel_bind_backlog", forPlugins(NetworkAccessCheckActions::serverSocketChannelBindWithBacklog)),
        entry("server_socket_channel_accept", forPlugins(NetworkAccessCheckActions::serverSocketChannelAccept)),
        entry("asynchronous_server_socket_channel_bind", forPlugins(NetworkAccessCheckActions::asynchronousServerSocketChannelBind)),
        entry(
            "asynchronous_server_socket_channel_bind_backlog",
            forPlugins(NetworkAccessCheckActions::asynchronousServerSocketChannelBindWithBacklog)
        ),
        entry("asynchronous_server_socket_channel_accept", forPlugins(NetworkAccessCheckActions::asynchronousServerSocketChannelAccept)),
        entry(
            "asynchronous_server_socket_channel_accept_with_handler",
            forPlugins(NetworkAccessCheckActions::asynchronousServerSocketChannelAcceptWithHandler)
        ),
        entry("socket_channel_bind", forPlugins(NetworkAccessCheckActions::socketChannelBind)),
        entry("socket_channel_connect", forPlugins(NetworkAccessCheckActions::socketChannelConnect)),
        entry("asynchronous_socket_channel_bind", forPlugins(NetworkAccessCheckActions::asynchronousSocketChannelBind)),
        entry("asynchronous_socket_channel_connect", forPlugins(NetworkAccessCheckActions::asynchronousSocketChannelConnect)),
        entry(
            "asynchronous_socket_channel_connect_with_completion",
            forPlugins(NetworkAccessCheckActions::asynchronousSocketChannelConnectWithCompletion)
        ),
        entry("datagram_channel_bind", forPlugins(NetworkAccessCheckActions::datagramChannelBind)),
        entry("datagram_channel_connect", forPlugins(NetworkAccessCheckActions::datagramChannelConnect)),
        entry("datagram_channel_send", forPlugins(NetworkAccessCheckActions::datagramChannelSend)),
        entry("datagram_channel_receive", forPlugins(NetworkAccessCheckActions::datagramChannelReceive)),

        entry("runtime_load", forPlugins(LoadNativeLibrariesCheckActions::runtimeLoad)),
        entry("runtime_load_library", forPlugins(LoadNativeLibrariesCheckActions::runtimeLoadLibrary)),
        entry("system_load", forPlugins(LoadNativeLibrariesCheckActions::systemLoad)),
        entry("system_load_library", forPlugins(LoadNativeLibrariesCheckActions::systemLoadLibrary)),
        entry("enable_native_access", new CheckAction(VersionSpecificNativeChecks::enableNativeAccess, false, 22)),
        entry("address_target_layout", new CheckAction(VersionSpecificNativeChecks::addressLayoutWithTargetLayout, false, 22)),
        entry("donwncall_handle", new CheckAction(VersionSpecificNativeChecks::linkerDowncallHandle, false, 22)),
        entry("donwncall_handle_with_address", new CheckAction(VersionSpecificNativeChecks::linkerDowncallHandleWithAddress, false, 22)),
        entry("upcall_stub", new CheckAction(VersionSpecificNativeChecks::linkerUpcallStub, false, 22)),
        entry("reinterpret", new CheckAction(VersionSpecificNativeChecks::memorySegmentReinterpret, false, 22)),
        entry("reinterpret_cleanup", new CheckAction(VersionSpecificNativeChecks::memorySegmentReinterpretWithCleanup, false, 22)),
        entry(
            "reinterpret_size_cleanup",
            new CheckAction(VersionSpecificNativeChecks::memorySegmentReinterpretWithSizeAndCleanup, false, 22)
        ),
        entry("symbol_lookup_name", new CheckAction(VersionSpecificNativeChecks::symbolLookupWithName, false, 22)),
        entry("symbol_lookup_path", new CheckAction(VersionSpecificNativeChecks::symbolLookupWithPath, false, 22)),

        entry("create_scanner", forPlugins(FileCheckActions::createScannerFile)),
        entry("create_scanner_with_charset", forPlugins(FileCheckActions::createScannerFileWithCharset)),
        entry("create_scanner_with_charset_name", forPlugins(FileCheckActions::createScannerFileWithCharsetName)),
        entry("create_file_output_stream_string", forPlugins(FileCheckActions::createFileOutputStreamString)),
        entry("create_file_output_stream_string_with_append", forPlugins(FileCheckActions::createFileOutputStreamStringWithAppend)),
        entry("create_file_output_stream_file", forPlugins(FileCheckActions::createFileOutputStreamFile)),
        entry("create_file_output_stream_file_with_append", forPlugins(FileCheckActions::createFileOutputStreamFileWithAppend)),
        entry("files_probe_content_type", forPlugins(FileCheckActions::filesProbeContentType)),
        entry("files_set_owner", forPlugins(FileCheckActions::filesSetOwner)),

        entry("java_lang_Thread$setContextClassLoader", forPlugins(RestEntitlementsCheckAction::java_lang_Thread$setContextClassLoader)),
        entry("java_lang_Thread$_1", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$_1)),
        entry("java_lang_Thread$_2", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$_2)),
        entry("java_lang_Thread$_3", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$_3)),
        entry("java_lang_Thread$_4", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$_4)),
        // entry("java_lang_Thread$_5", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$_5)), // This check is not yet active
        entry("java_lang_Thread$_6", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$_6)),
        entry("java_lang_Thread$_7", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$_7)),
        entry(
            "java_lang_ThreadBuilders$PlatformThreadBuilder$unstarted",
            deniedToPlugins(RestEntitlementsCheckAction::java_lang_ThreadBuilders$PlatformThreadBuilder$unstarted)
        ),
        entry(
            "java_lang_ThreadBuilders$PlatformThreadFactory$newThread",
            deniedToPlugins(RestEntitlementsCheckAction::java_lang_ThreadBuilders$PlatformThreadFactory$newThread)
        ),
        entry("java_lang_ThreadGroup$_1", deniedToPlugins(RestEntitlementsCheckAction::java_lang_ThreadGroup$_1)),
        entry("java_lang_ThreadGroup$_2", deniedToPlugins(RestEntitlementsCheckAction::java_lang_ThreadGroup$_2)),
        entry("java_util_concurrent_ForkJoinPool$_1", deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$_1)),
        entry("java_util_concurrent_ForkJoinPool$_2", deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$_2)),
        entry("java_util_concurrent_ForkJoinPool$_3", deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$_3)),
        entry("java_util_concurrent_ForkJoinPool$_4", deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$_4)),
        entry(
            "java_util_concurrent_ForkJoinPool$DefaultForkJoinWorkerThreadFactory$newThread",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$DefaultForkJoinWorkerThreadFactory$newThread)
        ),
        entry(
            "java_util_concurrent_ForkJoinWorkerThread$_1",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinWorkerThread$_1)
        ),
        entry(
            "java_util_concurrent_ForkJoinWorkerThread$_2",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinWorkerThread$_2)
        ),
        entry("java_lang_Thread$setDaemon", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$setDaemon)),
        entry("java_lang_ThreadGroup$setDaemon", deniedToPlugins(RestEntitlementsCheckAction::java_lang_ThreadGroup$setDaemon)),
        entry(
            "java_util_concurrent_ForkJoinPool$setParallelism",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$setParallelism)
        ),
        entry("java_lang_Thread$interrupt", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$interrupt)),
        entry("java_lang_ThreadGroup$interrupt", alwaysDenied(RestEntitlementsCheckAction::java_lang_ThreadGroup$interrupt)),
        entry(
            "java_util_concurrent_ForkJoinPool$close",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$close)
        ),
        entry(
            "java_util_concurrent_ForkJoinPool$shutdown",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$shutdown)
        ),
        entry(
            "java_util_concurrent_ForkJoinPool$shutdownNow",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ForkJoinPool$shutdownNow)
        ),
        entry(
            "java_util_concurrent_ThreadPerTaskExecutor$close",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ThreadPerTaskExecutor$close)
        ),
        entry(
            "java_util_concurrent_ThreadPerTaskExecutor$shutdown",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ThreadPerTaskExecutor$shutdown)
        ),
        entry(
            "java_util_concurrent_ThreadPerTaskExecutor$shutdownNow",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ThreadPerTaskExecutor$shutdownNow)
        ),
        entry(
            "java_util_concurrent_ThreadPoolExecutor$shutdown",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ThreadPoolExecutor$shutdown)
        ),
        entry(
            "java_util_concurrent_ThreadPoolExecutor$shutdownNow",
            deniedToPlugins(RestEntitlementsCheckAction::java_util_concurrent_ThreadPoolExecutor$shutdownNow)
        ),
        entry("java_lang_Thread$setName", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$setName)),
        entry("java_lang_Thread$setPriority", deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$setPriority)),
        entry(
            "java_lang_Thread$setUncaughtExceptionHandler",
            deniedToPlugins(RestEntitlementsCheckAction::java_lang_Thread$setUncaughtExceptionHandler)
        ),
        entry("java_lang_ThreadGroup$setMaxPriority", deniedToPlugins(RestEntitlementsCheckAction::java_lang_ThreadGroup$setMaxPriority))
    )
        .filter(entry -> entry.getValue().fromJavaVersion() == null || Runtime.version().feature() >= entry.getValue().fromJavaVersion())
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    private static void createURLStreamHandlerProvider() {
        var x = new URLStreamHandlerProvider() {
            @Override
            public URLStreamHandler createURLStreamHandler(String protocol) {
                return null;
            }
        };
    }

    @SuppressWarnings("deprecation")
    private static void createURLWithURLStreamHandler() throws MalformedURLException {
        var x = new URL("http", "host", 1234, "file", new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                return null;
            }
        });
    }

    @SuppressWarnings("deprecation")
    private static void createURLWithURLStreamHandler2() throws MalformedURLException {
        var x = new URL(null, "spec", new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                return null;
            }
        });
    }

    private static void setDefaultResponseCache() {
        ResponseCache.setDefault(null);
    }

    private static void setDefaultProxySelector() {
        ProxySelector.setDefault(null);
    }

    private static void setDefaultSSLContext() throws NoSuchAlgorithmException {
        SSLContext.setDefault(SSLContext.getDefault());
    }

    private static void setDefaultHostnameVerifier() {
        HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> false);
    }

    private static void setDefaultSSLSocketFactory() {
        HttpsURLConnection.setDefaultSSLSocketFactory(new DummyImplementations.DummySSLSocketFactory());
    }

    @SuppressForbidden(reason = "Specifically testing Runtime.exit")
    private static void runtimeExit() {
        Runtime.getRuntime().exit(123);
    }

    @SuppressForbidden(reason = "Specifically testing Runtime.halt")
    private static void runtimeHalt() {
        Runtime.getRuntime().halt(123);
    }

    @SuppressForbidden(reason = "Specifically testing System.exit")
    private static void systemExit() {
        System.exit(123);
    }

    private static void createClassLoader() throws IOException {
        try (var classLoader = new URLClassLoader("test", new URL[0], RestEntitlementsCheckAction.class.getClassLoader())) {
            logger.info("Created URLClassLoader [{}]", classLoader.getName());
        }
    }

    private static void processBuilder_start() throws IOException {
        new ProcessBuilder("").start();
    }

    private static void processBuilder_startPipeline() throws IOException {
        ProcessBuilder.startPipeline(List.of());
    }

    private static void setHttpsConnectionProperties() {
        new DummyImplementations.DummyHttpsURLConnection().setSSLSocketFactory(new DummyImplementations.DummySSLSocketFactory());
    }

    private static void system$$setIn() {
        System.setIn(System.in);
    }

    @SuppressForbidden(reason = "This should be a no-op so we don't interfere with system streams")
    private static void system$$setOut() {
        System.setOut(System.out);
    }

    @SuppressForbidden(reason = "This should be a no-op so we don't interfere with system streams")
    private static void system$$setErr() {
        System.setErr(System.err);
    }

    private static void runtime$addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(NO_OP_SHUTDOWN_HOOK);
    }

    private static void runtime$$removeShutdownHook() {
        Runtime.getRuntime().removeShutdownHook(NO_OP_SHUTDOWN_HOOK);
    }

    private static void thread$$setDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(Thread.getDefaultUncaughtExceptionHandler());
    }

    private static void localeServiceProvider$() {
        new DummyLocaleServiceProvider();
    }

    private static void breakIteratorProvider$() {
        new DummyBreakIteratorProvider();
    }

    private static void collatorProvider$() {
        new DummyCollatorProvider();
    }

    private static void dateFormatProvider$() {
        new DummyDateFormatProvider();
    }

    private static void dateFormatSymbolsProvider$() {
        new DummyDateFormatSymbolsProvider();
    }

    private static void decimalFormatSymbolsProvider$() {
        new DummyDecimalFormatSymbolsProvider();
    }

    private static void numberFormatProvider$() {
        new DummyNumberFormatProvider();
    }

    private static void calendarDataProvider$() {
        new DummyCalendarDataProvider();
    }

    private static void calendarNameProvider$() {
        new DummyCalendarNameProvider();
    }

    private static void currencyNameProvider$() {
        new DummyCurrencyNameProvider();
    }

    private static void localeNameProvider$() {
        new DummyLocaleNameProvider();
    }

    private static void timeZoneNameProvider$() {
        new DummyTimeZoneNameProvider();
    }

    private static void logManager$() {
        new java.util.logging.LogManager() {
        };
    }

    @SuppressWarnings("deprecation")
    @SuppressForbidden(reason = "We're required to prevent calls to this forbidden API")
    private static void datagramSocket$$setDatagramSocketImplFactory() throws IOException {
        DatagramSocket.setDatagramSocketImplFactory(() -> { throw new IllegalStateException(); });
    }

    private static void httpURLConnection$$setFollowRedirects() {
        HttpURLConnection.setFollowRedirects(HttpURLConnection.getFollowRedirects());
    }

    @SuppressWarnings("deprecation")
    @SuppressForbidden(reason = "We're required to prevent calls to this forbidden API")
    private static void serverSocket$$setSocketFactory() throws IOException {
        ServerSocket.setSocketFactory(() -> { throw new IllegalStateException(); });
    }

    @SuppressWarnings("deprecation")
    @SuppressForbidden(reason = "We're required to prevent calls to this forbidden API")
    private static void socket$$setSocketImplFactory() throws IOException {
        Socket.setSocketImplFactory(() -> { throw new IllegalStateException(); });
    }

    private static void url$$setURLStreamHandlerFactory() {
        URL.setURLStreamHandlerFactory(__ -> { throw new IllegalStateException(); });
    }

    private static void urlConnection$$setFileNameMap() {
        URLConnection.setFileNameMap(__ -> { throw new IllegalStateException(); });
    }

    private static void urlConnection$$setContentHandlerFactory() {
        URLConnection.setContentHandlerFactory(__ -> { throw new IllegalStateException(); });
    }

    private static void bindDatagramSocket() throws SocketException {
        try (var socket = new DatagramSocket(null)) {
            socket.bind(null);
        }
    }

    @SuppressForbidden(reason = "testing entitlements")
    private static void connectDatagramSocket() throws SocketException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.connect(new InetSocketAddress(1234));
        }
    }

    private static void joinGroupDatagramSocket() throws IOException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.joinGroup(
                new InetSocketAddress(InetAddress.getByAddress(new byte[] { (byte) 230, 0, 0, 1 }), 1234),
                NetworkInterface.getByIndex(0)
            );
        }
    }

    private static void leaveGroupDatagramSocket() throws IOException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.leaveGroup(
                new InetSocketAddress(InetAddress.getByAddress(new byte[] { (byte) 230, 0, 0, 1 }), 1234),
                NetworkInterface.getByIndex(0)
            );
        }
    }

    @SuppressForbidden(reason = "testing entitlements")
    private static void sendDatagramSocket() throws IOException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.send(new DatagramPacket(new byte[] { 0 }, 1, InetAddress.getLocalHost(), 1234));
        }
    }

    @SuppressForbidden(reason = "testing entitlements")
    private static void receiveDatagramSocket() throws IOException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.receive(new DatagramPacket(new byte[1], 1, InetAddress.getLocalHost(), 1234));
        }
    }

    public static Set<String> getCheckActionsAllowedInPlugins() {
        return checkActions.entrySet()
            .stream()
            .filter(kv -> kv.getValue().isAlwaysDeniedToPlugins() == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    public static Set<String> getAllCheckActions() {
        return checkActions.keySet();
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_entitlement_check"));
    }

    @Override
    public String getName() {
        return "check_entitlement_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        logger.debug("RestEntitlementsCheckAction rest handler [{}]", request.path());
        var actionName = request.param("action");
        if (Strings.isNullOrEmpty(actionName)) {
            throw new IllegalArgumentException("Missing action parameter");
        }
        var checkAction = checkActions.get(actionName);
        if (checkAction == null) {
            throw new IllegalArgumentException(Strings.format("Unknown action [%s]", actionName));
        }

        return channel -> {
            logger.info("Calling check action [{}]", actionName);
            checkAction.action().run();
            logger.debug("Check action [{}] returned", actionName);
            channel.sendResponse(new RestResponse(RestStatus.OK, Strings.format("Succesfully executed action [%s]", actionName)));
        };
    }

    static void java_lang_Thread$setContextClassLoader() {
        currentThread().setContextClassLoader(currentThread().getContextClassLoader());
    }

    static void java_lang_Thread$_1() {
        new Thread();
    }

    static void java_lang_Thread$_2() {
        new Thread(() -> {});
    }

    static void java_lang_Thread$_3() {
        new Thread(currentThread().getThreadGroup(), () -> {});
    }

    static void java_lang_Thread$_4() {
        new Thread("test");
    }

    static void java_lang_Thread$_5() {
        new Thread(currentThread().getThreadGroup(), "test");
    }

    static void java_lang_Thread$_6() {
        new Thread(currentThread().getThreadGroup(), () -> {}, "test");
    }

    static void java_lang_Thread$_7() {
        new Thread(currentThread().getThreadGroup(), () -> {}, "test", 123);
    }

    static void java_lang_Thread$_8() {
        new Thread(currentThread().getThreadGroup(), () -> {}, "test", 123, false);
    }

    static void java_lang_ThreadBuilders$PlatformThreadBuilder$unstarted() {
        Thread.ofPlatform().unstarted(() -> {});
    }

    static void java_lang_ThreadBuilders$PlatformThreadFactory$newThread() {
        Thread.ofPlatform().factory().newThread(() -> {});
    }

    static void java_lang_ThreadGroup$_1() {
        new ThreadGroup("test");
    }

    static void java_lang_ThreadGroup$_2() {
        new ThreadGroup(currentThread().getThreadGroup(), "test");
    }

    static void java_util_concurrent_ForkJoinPool$_1() {
        try (var __ = new ForkJoinPool()) {}
    }

    static void java_util_concurrent_ForkJoinPool$_2() {
        try (var __ = new ForkJoinPool(1)) {}
    }

    static void java_util_concurrent_ForkJoinPool$_3() {
        try (var __ = new ForkJoinPool(1, null, null, false)) {}
    }

    static void java_util_concurrent_ForkJoinPool$_4() {
        try (
            var __ = new ForkJoinPool(1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, false, 1, 1, 1, ___ -> false, 1, SECONDS)
        ) {}
    }

    static void java_util_concurrent_ForkJoinPool$DefaultForkJoinWorkerThreadFactory$newThread() {
        ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(ForkJoinPool.commonPool());
    }

    static void java_util_concurrent_ForkJoinWorkerThread$_1() {
        new ForkJoinWorkerThread(ForkJoinPool.commonPool()) {
        };
    }

    static void java_util_concurrent_ForkJoinWorkerThread$_2() {
        new ForkJoinWorkerThread(currentThread().getThreadGroup(), ForkJoinPool.commonPool(), false) {
        };
    }

    static void java_lang_Thread$setDaemon() {
        currentThread().setDaemon(currentThread().isDaemon());
    }

    static void java_lang_ThreadGroup$setDaemon() {
        currentThread().getThreadGroup().setDaemon(currentThread().getThreadGroup().isDaemon());
    }

    static void java_util_concurrent_ForkJoinPool$setParallelism() {
        ForkJoinPool.commonPool().setParallelism(ForkJoinPool.commonPool().getParallelism());
    }

    static void java_lang_Thread$interrupt() {
        currentThread().interrupt();
        Thread.interrupted();
    }

    static void java_lang_ThreadGroup$interrupt() {
        // Very disruptive!
        currentThread().getThreadGroup().interrupt();
    }

    static void java_util_concurrent_ForkJoinPool$close() {
        new ForkJoinPool().close();
    }

    static void java_util_concurrent_ForkJoinPool$shutdown() {
        try (var pool = new ForkJoinPool()) {
            pool.shutdown();
        }
    }

    static void java_util_concurrent_ForkJoinPool$shutdownNow() {
        try (var pool = new ForkJoinPool()) {
            pool.shutdownNow();
        }
    }

    static void java_util_concurrent_ThreadPerTaskExecutor$close() {
        Executors.newVirtualThreadPerTaskExecutor().close();
    }

    static void java_util_concurrent_ThreadPerTaskExecutor$shutdown() {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.shutdown();
        }
    }

    static void java_util_concurrent_ThreadPerTaskExecutor$shutdownNow() {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.shutdownNow();
        }
    }

    static void java_util_concurrent_ThreadPoolExecutor$shutdown() {
        try (ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, SECONDS, new SynchronousQueue<>())) {
            executor.shutdown();
        }
    }

    static void java_util_concurrent_ThreadPoolExecutor$shutdownNow() {
        try (ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, SECONDS, new SynchronousQueue<>())) {
            executor.shutdownNow();
        }
    }

    static void java_lang_Thread$setName() {
        currentThread().setName(currentThread().getName());
    }

    static void java_lang_Thread$setPriority() {
        currentThread().setPriority(currentThread().getPriority());
    }

    static void java_lang_Thread$setUncaughtExceptionHandler() {
        currentThread().setUncaughtExceptionHandler(currentThread().getUncaughtExceptionHandler());
    }

    static void java_lang_ThreadGroup$setMaxPriority() {
        currentThread().getThreadGroup().setMaxPriority(currentThread().getThreadGroup().getMaxPriority());
    }

}
