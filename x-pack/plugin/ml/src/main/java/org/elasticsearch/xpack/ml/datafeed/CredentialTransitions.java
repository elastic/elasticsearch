/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeWithHeadersAsync;
import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

/**
 * Credential transition policy and execution for CPS datafeeds: decide mint/rekey/clear/keep,
 * validate-before-mint, persist via {@link CredentialTransitions.Change}, and best-effort revoke.
 */
public final class CredentialTransitions {

    private static final Logger logger = LogManager.getLogger(CredentialTransitions.class);

    /**
     * How a datafeed config update should treat the persisted cloud internal credential envelope.
     */
    public sealed interface Change permits Change.Keep, Change.Mint, Change.Clear {

        record Keep() implements Change {}

        /**
         * Provides a hook that receives the applied {@link DatafeedConfig} and resolves the
         * credential to persist (probe → mint → {@code credentialListener.onResponse}).
         */
        record Mint(BiConsumer<DatafeedConfig, ActionListener<PersistedCloudCredential>> mintHook) implements Change {}

        record Clear() implements Change {}

        Keep KEEP = new Keep();
        Clear CLEAR = new Clear();
    }

    public enum Intent {
        KEEP,
        REPLACE,
        CLEAR
    }

    public record TransitionContext(
        boolean crossProjectEnabled,
        boolean callerHasCloudCredential,
        boolean envelopeExists,
        boolean affectsCrossProjectSearchSurface
    ) {}

    private final AnomalyDetectionAuditor auditor;
    private final Supplier<InternalCloudApiKeyService> apiKeyServiceSupplier;
    private final Supplier<CloudCredentialManager> credentialManagerSupplier;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final DatafeedConfigProvider datafeedConfigProvider;

    public CredentialTransitions(
        AnomalyDetectionAuditor auditor,
        Supplier<InternalCloudApiKeyService> apiKeyServiceSupplier,
        Supplier<CloudCredentialManager> credentialManagerSupplier,
        Client client,
        NamedXContentRegistry xContentRegistry,
        DatafeedConfigProvider datafeedConfigProvider
    ) {
        this.auditor = auditor;
        this.apiKeyServiceSupplier = apiKeyServiceSupplier;
        this.credentialManagerSupplier = credentialManagerSupplier;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.datafeedConfigProvider = datafeedConfigProvider;
    }

    public static Intent decideForUpdate(TransitionContext ctx) {
        if (ctx.crossProjectEnabled() == false) {
            return Intent.KEEP;
        }
        if (ctx.callerHasCloudCredential() == false && ctx.envelopeExists()) {
            return Intent.CLEAR;
        }
        if (ctx.callerHasCloudCredential() && (ctx.envelopeExists() == false || ctx.affectsCrossProjectSearchSurface())) {
            return Intent.REPLACE;
        }
        return Intent.KEEP;
    }

    public static Intent decideForCreate(TransitionContext ctx) {
        if (ctx.crossProjectEnabled() && ctx.callerHasCloudCredential()) {
            return Intent.REPLACE;
        }
        return Intent.KEEP;
    }

    public boolean hasCloudManagedCredential(ThreadPool threadPool) {
        return credentialManagerSupplier.get().hasCloudManagedCredential(threadPool.getThreadContext());
    }

    public void executeUpdate(
        Intent intent,
        UpdateDatafeedAction.Request request,
        String jobId,
        Map<String, String> headers,
        ThreadPool threadPool,
        SecurityContext securityContext,
        BiConsumer<DatafeedConfig, ActionListener<Boolean>> validator,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        switch (intent) {
            case CLEAR -> applyDowngrade(request, jobId, headers, validator, listener);
            case REPLACE -> applyRekey(request, jobId, headers, threadPool, securityContext, validator, listener);
            case KEEP -> persistUpdateWithoutCredentialChange(request, headers, validator, listener);
        }
    }

    public void executePut(
        Intent intent,
        PutDatafeedAction.Request request,
        ClusterState clusterState,
        ThreadPool threadPool,
        @Nullable SecurityContext securityContext,
        PutPersistentCallback persistFn,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        if (intent == Intent.REPLACE) {
            String datafeedId = request.getDatafeed().getId();
            String jobId = request.getDatafeed().getJobId();
            Map<String, String> headers = threadPool.getThreadContext().getHeaders();
            CloudCredential carriedCredential = request.getCloudCredential();
            validateSearchBeforeMint(request.getDatafeed(), headers, carriedCredential, listener.delegateFailureAndWrap((l, ignored) -> {
                mintCpsKeyForDatafeed(datafeedId, threadPool, securityContext, carriedCredential, l, (newCredential, userHeaders) -> {
                    DatafeedConfig.Builder builder = new DatafeedConfig.Builder(request.getDatafeed());
                    builder.setCloudInternalCredential(newCredential);
                    PutDatafeedAction.Request updatedRequest = new PutDatafeedAction.Request(builder.build());
                    updatedRequest.masterNodeTimeout(request.masterNodeTimeout());
                    persistFn.put(
                        updatedRequest,
                        userHeaders,
                        clusterState,
                        revokeKeyOnFailure(newCredential, jobId, ActionListener.wrap(response -> {
                            auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_MINTED));
                            l.onResponse(response);
                        }, l::onFailure))
                    );
                });
            }));
        } else {
            persistFn.put(request, threadPool.getThreadContext().getHeaders(), clusterState, listener);
        }
    }

    /**
     * Best-effort revokes a persisted envelope before running {@code continuation}.
     */
    public void revokeEnvelopeIfPresent(String datafeedId, DatafeedConfig config, Runnable continuation) {
        PersistedCloudCredential cred = config.getCloudInternalCredential();
        if (cred == null) {
            continuation.run();
            return;
        }
        apiKeyServiceSupplier.get().revokeCloudAuthentication(cred, ActionListener.wrap(ignored -> {
            auditor.info(config.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOKED));
            cred.close();
            continuation.run();
        }, e -> {
            logger.warn(() -> "[" + datafeedId + "] Failed to revoke internal cloud API key [" + cred.id() + "] on datafeed delete", e);
            auditor.info(config.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOCATION_FAILED, cred.id()));
            cred.close();
            continuation.run();
        }));
    }

    @FunctionalInterface
    public interface PutPersistentCallback {
        void put(
            PutDatafeedAction.Request request,
            Map<String, String> headers,
            ClusterState clusterState,
            ActionListener<PutDatafeedAction.Response> listener
        );
    }

    private void persistUpdateWithoutCredentialChange(
        UpdateDatafeedAction.Request request,
        Map<String, String> headers,
        BiConsumer<DatafeedConfig, ActionListener<Boolean>> validator,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        datafeedConfigProvider.updateDatefeedConfig(
            request.getUpdate().getId(),
            request.getUpdate(),
            headers,
            validator,
            listener.delegateFailureAndWrap((l, updatedConfig) -> l.onResponse(new PutDatafeedAction.Response(updatedConfig)))
        );
    }

    private void applyDowngrade(
        UpdateDatafeedAction.Request request,
        String jobId,
        Map<String, String> headers,
        BiConsumer<DatafeedConfig, ActionListener<Boolean>> validator,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        String datafeedId = request.getUpdate().getId();
        datafeedConfigProvider.updateDatefeedConfig(
            datafeedId,
            request.getUpdate(),
            headers,
            Change.CLEAR,
            validator,
            listener.delegateFailureAndWrap((l, tuple) -> {
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_CLEARED));
                DatafeedConfig updatedConfig = tuple.v1();
                PersistedCloudCredential oldCredential = tuple.v2();
                if (oldCredential != null) {
                    bestEffortRevokeOldKey(datafeedId, oldCredential, updatedConfig, l);
                } else {
                    l.onResponse(new PutDatafeedAction.Response(updatedConfig));
                }
            })
        );
    }

    /**
     * Performs a re-key update: probe → mint (via {@link Change.Mint} hook inside the
     * {@link DatafeedConfigProvider} transaction) → persist → audit REKEYED → revoke old key.
     * If anything after the mint fails, the newly minted key is best-effort revoked.
     */
    private void applyRekey(
        UpdateDatafeedAction.Request request,
        String jobId,
        Map<String, String> headers,
        ThreadPool threadPool,
        SecurityContext securityContext,
        BiConsumer<DatafeedConfig, ActionListener<Boolean>> validator,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        String datafeedId = request.getUpdate().getId();
        AtomicReference<PersistedCloudCredential> mintedCredRef = new AtomicReference<>();

        // If a credential was minted but a subsequent step fails, revoke it best-effort
        ActionListener<PutDatafeedAction.Response> guardedListener = ActionListener.wrap(listener::onResponse, e -> {
            PersistedCloudCredential minted = mintedCredRef.get();
            if (minted == null) {
                listener.onFailure(e);
                return;
            }
            apiKeyServiceSupplier.get().revokeCloudAuthentication(minted, ActionListener.wrap(ignored -> {
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOKED));
                minted.close();
                listener.onFailure(e);
            }, revokeFailure -> {
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOCATION_FAILED, minted.id()));
                minted.close();
                listener.onFailure(e);
            }));
        });

        // The hook is invoked by DatafeedConfigProvider after GET+apply, ensuring the probe
        // and mint run against the single authoritative applied config (no duplicate apply).
        Change.Mint mintChange = new Change.Mint(
            (applied, credentialListener) -> validateSearchBeforeMint(
                applied,
                headers,
                request.getCloudCredential(),
                credentialListener.delegateFailureAndWrap(
                    (cl, ignored) -> mintCpsKeyForDatafeed(
                        datafeedId,
                        threadPool,
                        securityContext,
                        request.getCloudCredential(),
                        cl,
                        (newCred, userHeaders) -> {
                            mintedCredRef.set(newCred);
                            cl.onResponse(newCred);
                        }
                    )
                )
            )
        );

        datafeedConfigProvider.updateDatefeedConfig(
            datafeedId,
            request.getUpdate(),
            headers,
            mintChange,
            validator,
            guardedListener.delegateFailureAndWrap((l, tuple) -> finalizeRekey(datafeedId, tuple, l))
        );
    }

    private void finalizeRekey(
        String datafeedId,
        Tuple<DatafeedConfig, PersistedCloudCredential> tuple,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        DatafeedConfig updatedConfig = tuple.v1();
        PersistedCloudCredential oldCredential = tuple.v2();
        auditor.info(updatedConfig.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REKEYED));
        if (oldCredential != null) {
            bestEffortRevokeOldKey(datafeedId, oldCredential, updatedConfig, listener);
        } else {
            listener.onResponse(new PutDatafeedAction.Response(updatedConfig));
        }
    }

    private void bestEffortRevokeOldKey(
        String datafeedId,
        PersistedCloudCredential oldCredential,
        DatafeedConfig patchedConfig,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        apiKeyServiceSupplier.get().revokeCloudAuthentication(oldCredential, ActionListener.wrap(ignored -> {
            auditor.info(patchedConfig.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOKED));
            oldCredential.close();
            listener.onResponse(new PutDatafeedAction.Response(patchedConfig));
        }, e -> {
            logger.warn(() -> "[" + datafeedId + "] Failed to revoke superseded internal cloud API key [" + oldCredential.id() + "]", e);
            auditor.info(
                patchedConfig.getJobId(),
                Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOCATION_FAILED, oldCredential.id())
            );
            oldCredential.close();
            listener.onResponse(new PutDatafeedAction.Response(patchedConfig));
        }));
    }

    private void validateSearchBeforeMint(
        DatafeedConfig config,
        Map<String, String> headers,
        @Nullable CloudCredential carriedCredential,
        ActionListener<Void> listener
    ) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0);
        QueryBuilder query = config.getParsedQuery(xContentRegistry);
        if (query != null) {
            sourceBuilder.query(query);
        }
        if (config.getRuntimeMappings() != null && config.getRuntimeMappings().isEmpty() == false) {
            sourceBuilder.runtimeMappings(config.getRuntimeMappings());
        }
        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(String[]::new)).indicesOptions(
            config.getIndicesOptions()
        ).source(sourceBuilder);
        if (config.getProjectRouting() != null) {
            searchRequest.setProjectRouting(config.getProjectRouting());
        }
        final CloudCredentialManager credentialManager = credentialManagerSupplier.get();
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        final CloudCredential callerCredential = resolveCallerCredential(carriedCredential, threadContext);
        final Client searchClient = credentialManager.wrapClient(client, callerCredential);
        executeWithHeadersAsync(
            headers,
            ML_ORIGIN,
            searchClient,
            TransportSearchAction.TYPE,
            true,
            searchRequest,
            listener.delegateFailureAndWrap((l, response) -> {
                if (response == null) {
                    l.onFailure(
                        new ElasticsearchStatusException(
                            "Unexpected null response from datafeed search probe",
                            RestStatus.INTERNAL_SERVER_ERROR
                        )
                    );
                    return;
                }
                if (response.status() != RestStatus.OK) {
                    l.onFailure(
                        new ElasticsearchStatusException(
                            "Datafeed search probe failed with status [" + response.status() + "]",
                            response.status()
                        )
                    );
                    return;
                }
                String securityDiagnosis = DatafeedSearchProbeDiagnostics.diagnoseSearchProbeFailure(response);
                if (securityDiagnosis != null) {
                    l.onFailure(new ElasticsearchStatusException(securityDiagnosis, RestStatus.FORBIDDEN));
                    return;
                }
                l.onResponse(null);
            })
        );
    }

    private void mintCpsKeyForDatafeed(
        String datafeedId,
        ThreadPool threadPool,
        @Nullable SecurityContext securityContext,
        @Nullable CloudCredential carriedCredential,
        ActionListener<?> failurePropagator,
        BiConsumer<PersistedCloudCredential, Map<String, String>> onSuccess
    ) {
        useSecondaryAuthIfAvailable(securityContext, () -> {
            final ThreadContext threadContext = threadPool.getThreadContext();
            final CloudCredential callerCredential = resolveCallerCredential(carriedCredential, threadContext);
            Map<String, String> userHeaders = threadPool.getThreadContext().getHeaders();
            apiKeyServiceSupplier.get()
                .grantCloudAuthentication(
                    callerCredential,
                    "datafeed:" + datafeedId,
                    ActionListener.wrap(
                        result -> useSecondaryAuthIfAvailable(
                            securityContext,
                            () -> onSuccess.accept(result.persistedCredential(), userHeaders)
                        ),
                        e -> {
                            logger.error(() -> "[" + datafeedId + "] Failed to mint internal cloud API key for CPS datafeed", e);
                            failurePropagator.onFailure(e);
                        }
                    )
                );
        });
    }

    @Nullable
    private CloudCredential resolveCallerCredential(@Nullable CloudCredential carriedCredential, ThreadContext threadContext) {
        if (carriedCredential != null) {
            return carriedCredential;
        }
        CloudCredentialManager credentialManager = credentialManagerSupplier.get();
        if (credentialManager.hasCloudManagedCredential(threadContext)) {
            return credentialManager.extractCloudManagedCredential(threadContext);
        }
        return null;
    }

    private <T> ActionListener<T> revokeKeyOnFailure(PersistedCloudCredential mintedCredential, String jobId, ActionListener<T> delegate) {
        return ActionListener.wrap(
            delegate::onResponse,
            e -> apiKeyServiceSupplier.get().revokeCloudAuthentication(mintedCredential, ActionListener.wrap(ignored -> {
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOKED));
                mintedCredential.close();
                delegate.onFailure(e);
            }, revokeFailure -> {
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CPS_KEY_REVOCATION_FAILED, mintedCredential.id()));
                mintedCredential.close();
                delegate.onFailure(e);
            }))
        );
    }
}
