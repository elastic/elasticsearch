/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * A {@link TransportAction} which, on creation
 * <ol>
 *     <li>Looks for an annotated {@link ActionHandler} method on itself</li>
 *     <li>Verifies that it is possible to supply all the arguments to that method</li>
 *     <li>registers a handler with the transport service that binds it own {@link #actionName} to the annotated method</li>
 * </ol>
 */
public abstract class ParameterizedTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {

    private final Logger logger = LogManager.getLogger(getClass());

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    protected @interface ActionHandler {

    }

    public interface Services {
        TransportService transportService();

        ClusterService clusterService();
    }

    @FunctionalInterface
    private interface Invoker<Req extends ActionRequest, Resp extends ActionResponse> {
        void invoke(Task task, Req request, ActionListener<Resp> response);
    }

    @FunctionalInterface
    private interface ParameterSupplier<Req extends ActionRequest, Resp extends ActionResponse> {
        Object resolveParameter(Task task, Req request, ActionListener<Resp> channel);
    }

    private final Invoker<Request, Response> handler;

    @SuppressWarnings("this-escape")
    protected ParameterizedTransportAction(
        String actionName,
        boolean canTripCircuitBreaker,
        Services services,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Executor executor
    ) {
        super(actionName, canTripCircuitBreaker, services.transportService(), actionFilters, requestReader, executor);
        handler = resolveHandler(services);
    }

    @Override
    protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        handler.invoke(task, request, listener);
    }

    private Invoker<Request, Response> resolveHandler(Services services) {
        final List<Method> methods = Arrays.stream(this.getClass().getMethods())
            .filter(method -> method.isAnnotationPresent(ActionHandler.class))
            .toList();
        final Method executeMethod = switch (methods.size()) {
            case 1 -> methods.getFirst();
            case 0 -> throw new IllegalStateException(
                "cannot find @" + ActionHandler.class.getSimpleName() + " annotated method in [" + getClass() + "]"
            );
            default -> throw new IllegalStateException(
                "found multiple ["
                    + methods.size()
                    + "] @"
                    + ActionHandler.class.getSimpleName()
                    + " annotated methods in ["
                    + getClass()
                    + "] : "
                    + methods
            );
        };

        final Class<?>[] parameterTypes = executeMethod.getParameterTypes();
        final List<ParameterSupplier<Request, Response>> parameterSuppliers = new ArrayList<>(parameterTypes.length);

        for (int i = 0; i < parameterTypes.length; i++) {
            final Class<?> type = parameterTypes[i];
            try {
                ParameterSupplier<Request, Response> param = resolveParameter(type, services);
                parameterSuppliers.add(param);
            } catch (IllegalArgumentException e) {
                throw new ElasticsearchException(
                    "cannot provide parameter of type [{}] for parameter #{} to {}",
                    e,
                    type,
                    i + 1,
                    executeMethod
                );
            }
        }

        final MethodHandle handle;
        try {
            handle = MethodHandles.lookup().unreflect(executeMethod);
        } catch (IllegalAccessException e) {
            throw new ElasticsearchException("cannot access @{} method [{}]", e, ActionHandler.class.getSimpleName(), executeMethod);
        }
        return buildInvoker(parameterSuppliers, handle, executeMethod);
    }

    @SuppressForbidden(reason = "Need to use MethodHandle.invoke because we don't know the argument types at compile time")
    private Invoker<Request, Response> buildInvoker(
        List<ParameterSupplier<Request, Response>> parameterSuppliers,
        MethodHandle handle,
        Method executeMethod
    ) {
        return (task, request, listener) -> {
            Object[] args = new Object[parameterSuppliers.size() + 1];
            args[0] = this;
            for (int i = 1; i < args.length; i++) {
                ParameterSupplier<Request, Response> ps = parameterSuppliers.get(i - 1);
                args[i] = ps.resolveParameter(task, request, listener);
            }
            try {
                handle.invokeWithArguments(args);
            } catch (Throwable e) {
                final String message = Strings.format("failed to invoke @%s method [%s]", e, ActionHandler.class.getSimpleName());
                logger.warn(message, e);
                throw new ElasticsearchException(message, executeMethod);
            }
        };
    }

    private ParameterSupplier<Request, Response> resolveParameter(Class<?> type, Services services) {
        if (ActionRequest.class.isAssignableFrom(type)) {
            // Typically the declared parameter will be a concrete class like "DoMagicRequest"
            // That is not equal to "ActionRequest", nor is it assignable from "ActionRequest"
            // But the reverse is true: "ActionRequest" is assignable from "DoMagicRequest"
            // which is the closest we have to knowing if this is the "*Request" parameter to the method
            // (alternatively, we could look at the actual Generic types on this class to find out the real request type)
            return (task, request, listener) -> type.cast(request);
        }
        if (ActionListener.class.equals(type)) {
            return (task, request, listener) -> listener;
        }
        if (Task.class.equals(type)) {
            return (task, request, listener) -> task;
        }
        if (ClusterService.class.equals(type)) {
            return (task, request, listener) -> services.clusterService();
        }
        if (ClusterState.class.equals(type)) {
            return (task, request, listener) -> services.clusterService().state();
        }
        if (TransportService.class.equals(type)) {
            return (task, request, listener) -> services.transportService();
        }
        throw new IllegalArgumentException("cannot bind to parameter type: " + type);
    }
}
