/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.common;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyBreakIteratorProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyCalendarDataProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyCalendarNameProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyCollatorProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyCurrencyNameProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyDateFormatProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyDateFormatSymbolsProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyDecimalFormatSymbolsProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyLocaleNameProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyLocaleServiceProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyNumberFormatProvider;
import org.elasticsearch.entitlement.qa.common.DummyImplementations.DummyTimeZoneNameProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.DatagramSocket;
import java.net.DatagramSocketImpl;
import java.net.DatagramSocketImplFactory;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import static java.util.Map.entry;
import static org.elasticsearch.entitlement.qa.common.RestEntitlementsCheckAction.CheckAction.alwaysDenied;
import static org.elasticsearch.entitlement.qa.common.RestEntitlementsCheckAction.CheckAction.deniedToPlugins;
import static org.elasticsearch.entitlement.qa.common.RestEntitlementsCheckAction.CheckAction.forPlugins;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestEntitlementsCheckAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckAction.class);
    public static final Thread NO_OP_SHUTDOWN_HOOK = new Thread(() -> {}, "Shutdown hook for testing");
    private final String prefix;

    record CheckAction(Runnable action, boolean isAlwaysDeniedToPlugins) {
        /**
         * These cannot be granted to plugins, so our test plugins cannot test the "allowed" case.
         * Used both for always-denied entitlements as well as those granted only to the server itself.
         */
        static CheckAction deniedToPlugins(Runnable action) {
            return new CheckAction(action, true);
        }

        static CheckAction forPlugins(Runnable action) {
            return new CheckAction(action, false);
        }

        static CheckAction alwaysDenied(Runnable action) {
            return new CheckAction(action, true);
        }
    }

    private static final Map<String, CheckAction> checkActions = Map.ofEntries(
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
        entry("urlConnection_setContentHandlerFactory", alwaysDenied(RestEntitlementsCheckAction::urlConnection$$setContentHandlerFactory))
    );

    private static void setDefaultSSLContext() {
        try {
            SSLContext.setDefault(SSLContext.getDefault());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
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

    private static void createClassLoader() {
        try (var classLoader = new URLClassLoader("test", new URL[0], RestEntitlementsCheckAction.class.getClassLoader())) {
            logger.info("Created URLClassLoader [{}]", classLoader.getName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void processBuilder_start() {
        try {
            new ProcessBuilder("").start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void processBuilder_startPipeline() {
        try {
            ProcessBuilder.startPipeline(List.of());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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
    private static void datagramSocket$$setDatagramSocketImplFactory() {
        try {
            DatagramSocket.setDatagramSocketImplFactory(new DatagramSocketImplFactory() {
                @Override
                public DatagramSocketImpl createDatagramSocketImpl() {
                    throw new IllegalStateException();
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void httpURLConnection$$setFollowRedirects() {
        HttpURLConnection.setFollowRedirects(HttpURLConnection.getFollowRedirects());
    }

    @SuppressWarnings("deprecation")
    @SuppressForbidden(reason = "We're required to prevent calls to this forbidden API")
    private static void serverSocket$$setSocketFactory() {
        try {
            ServerSocket.setSocketFactory(() -> { throw new IllegalStateException(); });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @SuppressForbidden(reason = "We're required to prevent calls to this forbidden API")
    private static void socket$$setSocketImplFactory() {
        try {
            Socket.setSocketImplFactory(() -> { throw new IllegalStateException(); });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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

    public RestEntitlementsCheckAction(String prefix) {
        this.prefix = prefix;
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
        return List.of(new Route(GET, "/_entitlement/" + prefix + "/_check"));
    }

    @Override
    public String getName() {
        return "check_" + prefix + "_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        logger.info("RestEntitlementsCheckAction rest handler [{}]", request.path());
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
