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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.entitlement.qa.common.RestEntitlementsCheckAction.CheckAction.deniedToPlugins;
import static org.elasticsearch.entitlement.qa.common.RestEntitlementsCheckAction.CheckAction.forPlugins;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestEntitlementsCheckAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckAction.class);
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
    }

    private static final Map<String, CheckAction> checkActions = Map.ofEntries(
        entry("runtime_exit", deniedToPlugins(RestEntitlementsCheckAction::runtimeExit)),
        entry("runtime_halt", deniedToPlugins(RestEntitlementsCheckAction::runtimeHalt)),
        entry("create_classloader", forPlugins(RestEntitlementsCheckAction::createClassLoader)),
        // entry("processBuilder_start", deniedToPlugins(RestEntitlementsCheckAction::processBuilder_start)),
        entry("processBuilder_startPipeline", deniedToPlugins(RestEntitlementsCheckAction::processBuilder_startPipeline))
    );

    @SuppressForbidden(reason = "Specifically testing Runtime.exit")
    private static void runtimeExit() {
        Runtime.getRuntime().exit(123);
    }

    @SuppressForbidden(reason = "Specifically testing Runtime.halt")
    private static void runtimeHalt() {
        Runtime.getRuntime().halt(123);
    }

    private static void createClassLoader() {
        try (var classLoader = new URLClassLoader("test", new URL[0], RestEntitlementsCheckAction.class.getClassLoader())) {
            logger.info("Created URLClassLoader [{}]", classLoader.getName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void processBuilder_start() {
        // TODO: processBuilder().start();
    }

    private static void processBuilder_startPipeline() {
        try {
            ProcessBuilder.startPipeline(List.of());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public RestEntitlementsCheckAction(String prefix) {
        this.prefix = prefix;
    }

    public static Set<String> getServerAndPluginsCheckActions() {
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
