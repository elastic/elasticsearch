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
import org.elasticsearch.core.CheckedFunction;
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
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;
import static org.elasticsearch.rest.RestRequest.Method.GET;

@SuppressWarnings("unused")
public class RestEntitlementsCheckAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestEntitlementsCheckAction.class);

    record CheckAction(
        CheckedFunction<Environment, Object, Exception> action,
        EntitlementTest.ExpectedAccess expectedAccess,
        Class<? extends Exception> expectedExceptionIfDenied,
        String[] expectedDefaultIfDenied,
        Class<?> expectedDefaultType,
        boolean isExpectedDefaultNull,
        boolean isExpectedNoOp,
        Integer fromJavaVersion
    ) {}

    private static final Map<String, CheckAction> checkActions = collectTests(
        FileCheckActions.class,
        FileStoreActions.class,
        JvmActions.class,
        LoadNativeLibrariesCheckActions.class,
        ManageThreadsActions.class,
        StructuredTaskScopeActions.class,
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
            String[] expectedDefault = testAnnotation.expectedDefaultIfDenied();
            Class<?> expectedDefaultType = testAnnotation.expectedDefaultType();
            boolean isExpectedDefaultNull = testAnnotation.isExpectedDefaultNull();
            boolean isExpectedNoOp = testAnnotation.isExpectedNoOp();
            boolean hasDefaultValue = expectedDefault.length > 0;
            if (hasDefaultValue && expectedDefault.length != 1) {
                throw new AssertionError("Entitlement test method [" + method + "] expectedDefaultIfDenied must have exactly one element");
            }
            if (expectedDefaultType != void.class && hasDefaultValue == false) {
                throw new AssertionError(
                    "Entitlement test method [" + method + "] expectedDefaultType requires expectedDefaultIfDenied to be set"
                );
            }
            if (expectedDefaultType != void.class && expectedDefaultType != method.getReturnType()) {
                throw new AssertionError(
                    "Entitlement test method ["
                        + method
                        + "] expectedDefaultType ["
                        + expectedDefaultType.getName()
                        + "] does not match return type ["
                        + method.getReturnType().getName()
                        + "]"
                );
            }
            int denialStrategyCount = (hasDefaultValue ? 1 : 0) + (isExpectedDefaultNull ? 1 : 0) + (isExpectedNoOp ? 1 : 0);
            if (denialStrategyCount > 1) {
                throw new AssertionError(
                    "Entitlement test method ["
                        + method
                        + "] must set at most one of expectedDefaultIfDenied, isExpectedDefaultNull, or isExpectedNoOp"
                );
            }
            if ((hasDefaultValue || isExpectedDefaultNull) && method.getReturnType() == void.class) {
                throw new AssertionError(
                    "Entitlement test method [" + method + "] must have a return type when a default value is expected"
                );
            }
            if (isExpectedNoOp && method.getReturnType() != boolean.class) {
                throw new AssertionError("Entitlement test method [" + method + "] must return boolean when isExpectedNoOp is set");
            }
            final CheckedFunction<Environment, Object, Exception> call = createFunctionForMethod(method);
            CheckedFunction<Environment, Object, Exception> action = env -> {
                try {
                    return call.apply(env);
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
                action,
                testAnnotation.expectedAccess(),
                testAnnotation.expectedExceptionIfDenied(),
                expectedDefault,
                expectedDefaultType,
                isExpectedDefaultNull,
                isExpectedNoOp,
                fromJavaVersion
            );
            if (filter.test(checkAction)) {
                entries.add(entry(method.getName(), checkAction));
            }
        }
    }

    private static CheckedFunction<Environment, Object, Exception> createFunctionForMethod(Method method) {
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

    public static Set<String> getAlwaysDeniedCheckActions() {
        return checkActions.entrySet()
            .stream()
            .filter(kv -> kv.getValue().expectedAccess().equals(ALWAYS_DENIED))
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

    private static final java.util.Map<Class<?>, Class<?>> PRIMITIVE_TO_BOXED = java.util.Map.of(
        boolean.class,
        Boolean.class,
        int.class,
        Integer.class,
        long.class,
        Long.class,
        double.class,
        Double.class,
        float.class,
        Float.class,
        short.class,
        Short.class,
        byte.class,
        Byte.class,
        char.class,
        Character.class
    );

    private static Class<?> boxed(Class<?> type) {
        return PRIMITIVE_TO_BOXED.getOrDefault(type, type);
    }

    private static final String NOT_ENTITLED_EXCEPTION_NAME = "org.elasticsearch.entitlement.bridge.NotEntitledException";

    private static boolean hasCause(Throwable e, String className) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause.getClass().getName().equals(className)) {
                return true;
            }
        }
        return false;
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
                Object result = checkAction.action().apply(environment);
                response = new RestResponse(RestStatus.OK, Strings.format("Successfully executed action [%s]", actionName));
                if (result != null) {
                    response.addHeader("resultValue", result.toString());
                    response.addHeader("resultType", result.getClass().getName());
                } else {
                    response.addHeader("resultIsNull", "true");
                }
                if (checkAction.expectedDefaultIfDenied().length == 1) {
                    response.addHeader("expectedDefaultIfDenied", checkAction.expectedDefaultIfDenied()[0]);
                }
                if (checkAction.expectedDefaultType() != void.class) {
                    Class<?> expectedType = boxed(checkAction.expectedDefaultType());
                    response.addHeader("defaultTypeMatch", String.valueOf(result != null && expectedType.isInstance(result)));
                }
                if (checkAction.isExpectedDefaultNull()) {
                    response.addHeader("isExpectedDefaultNull", "true");
                }
                if (checkAction.isExpectedNoOp()) {
                    response.addHeader("noOpChanged", result.toString());
                }
            } catch (Exception e) {
                var statusCode = checkAction.expectedExceptionIfDenied.isInstance(e)
                    ? RestStatus.FORBIDDEN
                    : RestStatus.INTERNAL_SERVER_ERROR;
                response = new RestResponse(channel, statusCode, e);
                response.addHeader("actualException", e.getClass().getName());
                response.addHeader("expectedException", checkAction.expectedExceptionIfDenied.getName());
                if (statusCode == RestStatus.FORBIDDEN && e.getCause() != null) {
                    response.addHeader("notEntitledCause", String.valueOf(hasCause(e, NOT_ENTITLED_EXCEPTION_NAME)));
                }
            }
            logger.debug("Check action [{}] returned status [{}]", actionName, response.status().getStatus());
            channel.sendResponse(response);
        };
    }
}
