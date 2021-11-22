/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class APM extends Plugin implements NetworkPlugin {

    public static final Set<String> TRACE_HEADERS = Set.of(Task.TRACE_PARENT, Task.TRACE_STATE);

    private final SetOnce<APMTracer> tracer = new SetOnce<>();
    private final Settings settings;

    public APM(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        tracer.set(new APMTracer(settings, threadPool, clusterService));
        return List.of(tracer.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(APMTracer.APM_ENABLED_SETTING, APMTracer.APM_ENDPOINT_SETTING, APMTracer.APM_TOKEN_SETTING);
    }

    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        return List.of(new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new ApmTransportInterceptor(sender, threadContext);
            }
        });
    }

    private class ApmTransportInterceptor implements TransportInterceptor.AsyncSender {

        private final TransportInterceptor.AsyncSender sender;
        private final ThreadContext threadContext;

        ApmTransportInterceptor(TransportInterceptor.AsyncSender sender, ThreadContext threadContext) {
            this.sender = sender;
            this.threadContext = threadContext;
        }

        @Override
        public <T extends TransportResponse> void sendRequest(
            Transport.Connection connection,
            String action,
            TransportRequest request,
            TransportRequestOptions options,
            TransportResponseHandler<T> handler
        ) {
            try (var ignored = withParentContext(String.valueOf(request.getParentTask().getId()))) {
                sender.sendRequest(connection, action, request, options, handler);
            }
        }

        private Releasable withParentContext(String parentTaskId) {
            var aTracer = tracer.get();
            if (aTracer == null) {
                return null;
            }
            if (aTracer.isEnabled() == false) {
                return null;
            }
            var headers = aTracer.getSpanHeadersById(parentTaskId);
            if (headers == null) {
                return null;
            }
            final Releasable releasable = threadContext.removeRequestHeaders(TRACE_HEADERS);
            threadContext.putHeader(headers);
            return releasable;
        }
    }
}
