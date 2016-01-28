/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.plugin.core.LicenseUtils;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.action.interceptor.RequestInterceptor;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.authz.privilege.HealthAndStatsPrivilege;
import org.elasticsearch.shield.crypto.CryptoService;
import org.elasticsearch.shield.license.ShieldLicenseState;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.shield.support.Automatons;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.shield.support.Exceptions.authorizationError;

/**
 *
 */
public class ShieldActionFilter extends AbstractComponent implements ActionFilter {

    private static final Predicate<String> LICENSE_EXPIRATION_ACTION_MATCHER = HealthAndStatsPrivilege.INSTANCE.predicate();
    // FIXME clean up this hack
    static final Predicate<String> INTERNAL_PREDICATE = new AutomatonPredicate(Automatons.patterns("internal:*"));

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final CryptoService cryptoService;
    private final AuditTrail auditTrail;
    private final ShieldActionMapper actionMapper;
    private final Set<RequestInterceptor> requestInterceptors;
    private final ShieldLicenseState licenseState;
    private final ThreadContext threadContext;

    @Inject
    public ShieldActionFilter(Settings settings, AuthenticationService authcService, AuthorizationService authzService, CryptoService cryptoService,
                              AuditTrail auditTrail, ShieldLicenseState licenseState, ShieldActionMapper actionMapper, Set<RequestInterceptor> requestInterceptors,
                              ThreadPool threadPool) {
        super(settings);
        this.authcService = authcService;
        this.authzService = authzService;
        this.cryptoService = cryptoService;
        this.auditTrail = auditTrail;
        this.actionMapper = actionMapper;
        this.licenseState = licenseState;
        this.requestInterceptors = requestInterceptors;
        this.threadContext = threadPool.getThreadContext();
    }

    @Override
    public void apply(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {

        /**
            A functional requirement - when the license of shield is disabled (invalid/expires), shield will continue
            to operate normally, except all read operations will be blocked.
         */
        if (!licenseState.statsAndHealthEnabled() && LICENSE_EXPIRATION_ACTION_MATCHER.test(action)) {
            logger.error("blocking [{}] operation due to expired license. Cluster health, cluster stats and indices stats \n" +
                    "operations are blocked on shield license expiration. All data operations (read and write) continue to work. \n" +
                    "If you have a new license, please update it. Otherwise, please reach out to your support contact.", action);
            throw LicenseUtils.newComplianceException(ShieldPlugin.NAME);
        }

        final ThreadContext.StoredContext original = threadContext.newStoredContext();
        try {
            if (licenseState.securityEnabled()) {
                // FIXME yet another hack. Needed to work around something like
                /*
                FailedNodeException[total failure in fetching]; nested: ElasticsearchSecurityException[action [internal:gateway/local/started_shards] is unauthorized for user [test_user]];
                    at org.elasticsearch.gateway.AsyncShardFetch$1.onFailure(AsyncShardFetch.java:284)
                    at org.elasticsearch.action.support.TransportAction$1.onFailure(TransportAction.java:84)
                    at org.elasticsearch.shield.action.ShieldActionFilter.apply(ShieldActionFilter.java:121)
                    at org.elasticsearch.action.support.TransportAction$RequestFilterChain.proceed(TransportAction.java:133)
                    at org.elasticsearch.action.support.TransportAction.execute(TransportAction.java:107)
                    at org.elasticsearch.action.support.TransportAction.execute(TransportAction.java:74)
                    at org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.list(TransportNodesListGatewayStartedShards.java:78)
                    at org.elasticsearch.gateway.AsyncShardFetch.asyncFetch(AsyncShardFetch.java:274)
                    at org.elasticsearch.gateway.AsyncShardFetch.fetchData(AsyncShardFetch.java:124)
                    at org.elasticsearch.gateway.GatewayAllocator$InternalPrimaryShardAllocator.fetchData(GatewayAllocator.java:156)
                    at org.elasticsearch.gateway.PrimaryShardAllocator.allocateUnassigned(PrimaryShardAllocator.java:83)
                    at org.elasticsearch.gateway.GatewayAllocator.allocateUnassigned(GatewayAllocator.java:120)
                    at org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators.allocateUnassigned(ShardsAllocators.java:72)
                    at org.elasticsearch.cluster.routing.allocation.AllocationService.reroute(AllocationService.java:309)
                    at org.elasticsearch.cluster.routing.allocation.AllocationService.reroute(AllocationService.java:273)
                    at org.elasticsearch.cluster.routing.allocation.AllocationService.reroute(AllocationService.java:259)
                    at org.elasticsearch.cluster.routing.RoutingService$2.execute(RoutingService.java:158)
                    at org.elasticsearch.cluster.ClusterStateUpdateTask.execute(ClusterStateUpdateTask.java:45)
                    at org.elasticsearch.cluster.service.InternalClusterService.runTasksForExecutor(InternalClusterService.java:447)
                    at org.elasticsearch.cluster.service.InternalClusterService$UpdateTask.run(InternalClusterService.java:757)
                    at org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor$FilterRunnable.run(EsThreadPoolExecutor.java:211)
                    at org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor$TieBreakingPrioritizedRunnable.runAndClean(PrioritizedEsThreadPoolExecutor.java:237)
                    at org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor$TieBreakingPrioritizedRunnable.run(PrioritizedEsThreadPoolExecutor.java:200)
                    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
                    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
                    at java.lang.Thread.run(Thread.java:745)
                 */
                if (INTERNAL_PREDICATE.test(action)) {
                    try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
                        String shieldAction = actionMapper.action(action, request);
                        User user = authcService.authenticate(shieldAction, request, User.SYSTEM);
                        authzService.authorize(user, shieldAction, request);
                        request = unsign(user, shieldAction, request);

                        for (RequestInterceptor interceptor : requestInterceptors) {
                            if (interceptor.supports(request)) {
                                interceptor.intercept(request, user);
                            }
                        }
                        chain.proceed(task, action, request, new SigningListener(this, listener, original));
                        return;
                    }
                }


                /**
                 here we fallback on the system user. Internal system requests are requests that are triggered by
                 the system itself (e.g. pings, update mappings, share relocation, etc...) and were not originated
                 by user interaction. Since these requests are triggered by es core modules, they are security
                 agnostic and therefore not associated with any user. When these requests execute locally, they
                 are executed directly on their relevant action. Since there is no other way a request can make
                 it to the action without an associated user (not via REST or transport - this is taken care of by
                 the {@link Rest} filter and the {@link ServerTransport} filter respectively), it's safe to assume a system user
                 here if a request is not associated with any other user.
                 */

                String shieldAction = actionMapper.action(action, request);
                User user = authcService.authenticate(shieldAction, request, User.SYSTEM);
                authzService.authorize(user, shieldAction, request);
                request = unsign(user, shieldAction, request);

                for (RequestInterceptor interceptor : requestInterceptors) {
                    if (interceptor.supports(request)) {
                        interceptor.intercept(request, user);
                    }
                }
                chain.proceed(task, action, request, new SigningListener(this, listener, original));
            } else {
                chain.proceed(task, action, request, listener);
            }
        } catch (Throwable t) {
            original.restore();
            listener.onFailure(t);
        }
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    <Request extends ActionRequest> Request unsign(User user, String action, Request request) {

        try {

            if (request instanceof SearchScrollRequest) {
                SearchScrollRequest scrollRequest = (SearchScrollRequest) request;
                String scrollId = scrollRequest.scrollId();
                scrollRequest.scrollId(cryptoService.unsignAndVerify(scrollId));
                return request;
            }

            if (request instanceof ClearScrollRequest) {
                ClearScrollRequest clearScrollRequest = (ClearScrollRequest) request;
                boolean isClearAllScrollRequest = clearScrollRequest.scrollIds().contains("_all");
                if (!isClearAllScrollRequest) {
                    List<String> signedIds = clearScrollRequest.scrollIds();
                    List<String> unsignedIds = new ArrayList<>(signedIds.size());
                    for (String signedId : signedIds) {
                        unsignedIds.add(cryptoService.unsignAndVerify(signedId));
                    }
                    clearScrollRequest.scrollIds(unsignedIds);
                }
                return request;
            }

            return request;

        } catch (IllegalArgumentException | IllegalStateException e) {
            auditTrail.tamperedRequest(user, action, request);
            throw authorizationError("invalid request. {}", e.getMessage());
        }
    }

    <Response extends ActionResponse> Response sign(Response response) throws IOException {

        if (response instanceof SearchResponse) {
            SearchResponse searchResponse = (SearchResponse) response;
            String scrollId = searchResponse.getScrollId();
            if (scrollId != null && !cryptoService.signed(scrollId)) {
                searchResponse.scrollId(cryptoService.sign(scrollId));
            }
            return response;
        }

        return response;
    }

    static class SigningListener<Response extends ActionResponse> implements ActionListener<Response> {

        private final ShieldActionFilter filter;
        private final ActionListener innerListener;
        private final ThreadContext.StoredContext threadContext;

        private SigningListener(ShieldActionFilter filter, ActionListener innerListener, ThreadContext.StoredContext threadContext) {
            this.filter = filter;
            this.innerListener = innerListener;
            this.threadContext = threadContext;
        }

        @Override @SuppressWarnings("unchecked")
        public void onResponse(Response response) {
            threadContext.restore();
            try {
                response = this.filter.sign(response);
                innerListener.onResponse(response);
            } catch (IOException e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            threadContext.restore();
            innerListener.onFailure(e);
        }
    }
}
