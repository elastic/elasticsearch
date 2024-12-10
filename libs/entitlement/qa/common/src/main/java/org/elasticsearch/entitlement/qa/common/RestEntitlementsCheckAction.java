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
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestEntitlementsCheckAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckAction.class);
    private final String prefix;

    private record CheckAction(Runnable action, boolean isServerOnly) {

        static CheckAction serverOnly(Runnable action) {
            return new CheckAction(action, true);
        }

        static CheckAction serverAndPlugin(Runnable action) {
            return new CheckAction(action, false);
        }
    }

    private static final Map<String, CheckAction> checkActions = Map.ofEntries(
        entry("system_exit", CheckAction.serverOnly(RestEntitlementsCheckAction::systemExit)),
        entry("create_classloader", CheckAction.serverAndPlugin(RestEntitlementsCheckAction::createClassLoader))
    );

    @SuppressForbidden(reason = "Specifically testing System.exit")
    private static void systemExit() {
        logger.info("Calling System.exit(123);");
        System.exit(123);
    }

    private static void createClassLoader() {
        logger.info("Calling new URLClassLoader");
        try (var classLoader = new URLClassLoader("test", new URL[0], RestEntitlementsCheckAction.class.getClassLoader())) {
            logger.info("Created URLClassLoader [{}]", classLoader.getName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public RestEntitlementsCheckAction(String prefix) {
        this.prefix = prefix;
    }

    public static Set<String> getServerAndPluginsCheckActions() {
        return checkActions.entrySet()
            .stream()
            .filter(kv -> kv.getValue().isServerOnly() == false)
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
            checkAction.action().run();
            channel.sendResponse(new RestResponse(RestStatus.OK, Strings.format("Succesfully executed action [%s]", actionName)));
        };
    }
}
