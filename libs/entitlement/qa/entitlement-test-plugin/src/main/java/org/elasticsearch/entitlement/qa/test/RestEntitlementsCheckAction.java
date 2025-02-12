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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import static java.util.Map.entry;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;
import static org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction.CheckAction.alwaysDenied;
import static org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction.CheckAction.deniedToPlugins;
import static org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction.CheckAction.forPlugins;
import static org.elasticsearch.rest.RestRequest.Method.GET;

@SuppressWarnings("unused")
public class RestEntitlementsCheckAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckAction.class);

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

    private static final Map<String, CheckAction> checkActions = Stream.of(
        Stream.<Entry<String, CheckAction>>of(
            entry("create_classloader", forPlugins(RestEntitlementsCheckAction::createClassLoader)),
            entry("processBuilder_start", deniedToPlugins(RestEntitlementsCheckAction::processBuilder_start)),
            entry("processBuilder_startPipeline", deniedToPlugins(RestEntitlementsCheckAction::processBuilder_startPipeline)),
            entry("set_https_connection_properties", forPlugins(RestEntitlementsCheckAction::setHttpsConnectionProperties)),
            entry("set_default_ssl_socket_factory", alwaysDenied(RestEntitlementsCheckAction::setDefaultSSLSocketFactory)),
            entry("set_default_hostname_verifier", alwaysDenied(RestEntitlementsCheckAction::setDefaultHostnameVerifier)),
            entry("set_default_ssl_context", alwaysDenied(RestEntitlementsCheckAction::setDefaultSSLContext)),
            entry(
                "thread_setDefaultUncaughtExceptionHandler",
                alwaysDenied(RestEntitlementsCheckAction::thread$$setDefaultUncaughtExceptionHandler)
            ),
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
            entry(
                "urlConnection_setContentHandlerFactory",
                alwaysDenied(RestEntitlementsCheckAction::urlConnection$$setContentHandlerFactory)
            ),

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
            entry(
                "asynchronous_server_socket_channel_accept",
                forPlugins(NetworkAccessCheckActions::asynchronousServerSocketChannelAccept)
            ),
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
            entry("system_load_library", forPlugins(LoadNativeLibrariesCheckActions::systemLoadLibrary))
        ),
        getTestEntries(FileCheckActions.class),
        getTestEntries(SpiActions.class),
        getTestEntries(SystemActions.class),
        getTestEntries(NativeActions.class),
        getTestEntries(FileStoreActions.class)
    )
        .flatMap(Function.identity())
        .filter(entry -> entry.getValue().fromJavaVersion() == null || Runtime.version().feature() >= entry.getValue().fromJavaVersion())
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));

    @SuppressForbidden(reason = "Need package private methods so we don't have to make them all public")
    private static Method[] getDeclaredMethods(Class<?> clazz) {
        return clazz.getDeclaredMethods();
    }

    private static Stream<Entry<String, CheckAction>> getTestEntries(Class<?> actionsClass) {
        List<Entry<String, CheckAction>> entries = new ArrayList<>();
        for (var method : getDeclaredMethods(actionsClass)) {
            var testAnnotation = method.getAnnotation(EntitlementTest.class);
            if (testAnnotation == null) {
                continue;
            }
            if (Modifier.isStatic(method.getModifiers()) == false) {
                throw new AssertionError("Entitlement test method [" + method + "] must be static");
            }
            if (method.getParameterTypes().length != 0) {
                throw new AssertionError("Entitlement test method [" + method + "] must not have parameters");
            }

            CheckedRunnable<Exception> runnable = () -> {
                try {
                    method.invoke(null);
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                } catch (InvocationTargetException e) {
                    if (e.getCause() instanceof Exception exc) {
                        throw exc;
                    } else {
                        throw new AssertionError(e);
                    }
                }
            };
            boolean deniedToPlugins = testAnnotation.expectedAccess() != PLUGINS;
            Integer fromJavaVersion = testAnnotation.fromJavaVersion() == -1 ? null : testAnnotation.fromJavaVersion();
            entries.add(entry(method.getName(), new CheckAction(runnable, deniedToPlugins, fromJavaVersion)));
        }
        return entries.stream();
    }

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

    private static void thread$$setDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(Thread.getDefaultUncaughtExceptionHandler());
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
            .map(Entry::getKey)
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
            channel.sendResponse(new RestResponse(RestStatus.OK, Strings.format("Succesfully executed action [%s]", actionName)));
        };
    }
}
