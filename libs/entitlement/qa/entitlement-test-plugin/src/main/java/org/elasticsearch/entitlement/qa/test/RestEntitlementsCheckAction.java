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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_ALLOWED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;
import static org.elasticsearch.rest.RestRequest.Method.GET;

@SuppressWarnings("unused")
public class RestEntitlementsCheckAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckAction.class);

    record CheckAction(
        CheckedConsumer<Environment, Exception> action,
        EntitlementTest.ExpectedAccess expectedAccess,
        Class<? extends Exception> expectedExceptionIfDenied,
        Integer fromJavaVersion
    ) {}

    private static final Map<String, CheckAction> checkActions = collectTests(
        FileCheckActions.class,
        FileStoreActions.class,
        JvmActions.class,
        LoadNativeLibrariesCheckActions.class,
        ManageThreadsActions.class,
        NativeActions.class,
        NetworkAccessCheckActions.class,
        NioChannelsActions.class,
        NioFilesActions.class,
        NioFileSystemActions.class,
        OperatingSystemActions.class,
        PathActions.class,
        SpiActions.class,
        SystemActions.class,
        URLConnectionFileActions.class,
        URLConnectionNetworkActions.class
    );

    private static Map<String, CheckAction> collectTests(Class<?>... testClasses) {
        List<Entry<String, CheckAction>> entries = new ArrayList<>();
        for (Class<?> testClass : testClasses) {
            getTestEntries(entries, testClass, a -> a.fromJavaVersion() == null || Runtime.version().feature() >= a.fromJavaVersion());
        }
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Entry<String, CheckAction>[] entriesArray = entries.toArray(new Entry[0]);
        return Map.ofEntries(entriesArray);
    }

    private final Environment environment;

    public RestEntitlementsCheckAction(Environment environment) {
        this.environment = environment;
    }

    @SuppressForbidden(reason = "Need package private methods so we don't have to make them all public")
    private static Method[] getDeclaredMethods(Class<?> clazz) {
        return clazz.getDeclaredMethods();
    }

    private static void getTestEntries(List<Entry<String, CheckAction>> entries, Class<?> actionsClass, Predicate<CheckAction> filter) {
        for (var method : getDeclaredMethods(actionsClass)) {
            var testAnnotation = method.getAnnotation(EntitlementTest.class);
            if (testAnnotation == null) {
                continue;
            }
            if (Modifier.isStatic(method.getModifiers()) == false) {
                throw new AssertionError("Entitlement test method [" + method + "] must be static");
            }
            if (Modifier.isPrivate(method.getModifiers())) {
                throw new AssertionError("Entitlement test method [" + method + "] must not be private");
            }
            final CheckedConsumer<Environment, Exception> call = createConsumerForMethod(method);
            CheckedConsumer<Environment, Exception> runnable = env -> {
                try {
                    call.accept(env);
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
            Integer fromJavaVersion = testAnnotation.fromJavaVersion() == -1 ? null : testAnnotation.fromJavaVersion();
            var checkAction = new CheckAction(
                runnable,
                testAnnotation.expectedAccess(),
                testAnnotation.expectedExceptionIfDenied(),
                fromJavaVersion
            );
            if (filter.test(checkAction)) {
                entries.add(entry(method.getName(), checkAction));
            }
        }
    }

    private static CheckedConsumer<Environment, Exception> createConsumerForMethod(Method method) {
        Class<?>[] parameters = method.getParameterTypes();
        if (parameters.length == 0) {
            return env -> method.invoke(null);
        }
        if (parameters.length == 1 && parameters[0].equals(Environment.class)) {
            return env -> method.invoke(null, env);
        }
        throw new AssertionError("Entitlement test method [" + method + "] must have no parameters or 1 parameter (Environment)");
    }

    public static Set<String> getCheckActionsAllowedInPlugins() {
        return checkActions.entrySet()
            .stream()
            .filter(kv -> kv.getValue().expectedAccess().equals(PLUGINS) || kv.getValue().expectedAccess().equals(ALWAYS_ALLOWED))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    public static Set<String> getAlwaysAllowedCheckActions() {
        return checkActions.entrySet()
            .stream()
            .filter(kv -> kv.getValue().expectedAccess().equals(ALWAYS_ALLOWED))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    public static Set<String> getDeniableCheckActions() {
        return checkActions.entrySet()
            .stream()
            .filter(kv -> kv.getValue().expectedAccess().equals(ALWAYS_ALLOWED) == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
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
            RestResponse response;
            try {
                checkAction.action().accept(environment);
                response = new RestResponse(RestStatus.OK, Strings.format("Succesfully executed action [%s]", actionName));
            } catch (Exception e) {
                var statusCode = checkAction.expectedExceptionIfDenied.isInstance(e)
                    ? RestStatus.FORBIDDEN
                    : RestStatus.INTERNAL_SERVER_ERROR;
                response = new RestResponse(channel, statusCode, e);
                response.addHeader("expectedException", checkAction.expectedExceptionIfDenied.getName());
            }
            logger.debug("Check action [{}] returned status [{}]", actionName, response.status().getStatus());
            channel.sendResponse(response);
        };
    }
}
