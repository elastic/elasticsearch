/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.admin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ShieldTemplateService is responsible for adding the template needed for the
 * {@code .shield} administrative index.
 */
public class ShieldTemplateService extends AbstractComponent implements ClusterStateListener {

    public static final String SHIELD_ADMIN_INDEX_NAME = ".shield";
    public static final String SHIELD_TEMPLATE_NAME = "shield-index-template";

    private final ThreadPool threadPool;
    private final Provider<Client> clientProvider;
    private final Provider<AuthenticationService> authProvider;
    private final ShieldInternalUserHolder adminUser;
    private final AtomicBoolean templateCreationPending = new AtomicBoolean(false);

    @Inject
    public ShieldTemplateService(Settings settings, ClusterService clusterService,
            Provider<Client> clientProvider, ThreadPool threadPool,
            Provider<AuthenticationService> authProvider,
            ShieldInternalUserHolder userHolder) {
        super(settings);
        this.threadPool = threadPool;
        this.clientProvider = clientProvider;
        this.authProvider = authProvider;
        this.adminUser = userHolder;
        clusterService.add(this);
    }

    private void createShieldTemplate() {
        Client client = this.clientProvider.get();
        AuthenticationService authService = this.authProvider.get();
        try (InputStream is = getClass().getResourceAsStream("/" + SHIELD_TEMPLATE_NAME + ".json")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            final byte[] template = out.toByteArray();
            logger.info("--> putting the shield index template");
            PutIndexTemplateRequest putTemplateRequest = client.admin().indices()
                    .preparePutTemplate(SHIELD_TEMPLATE_NAME).setSource(template).request();
            authService.attachUserHeaderIfMissing(putTemplateRequest, adminUser.user());
            PutIndexTemplateResponse templateResponse = client.admin().indices().putTemplate(putTemplateRequest).get();
            if (templateResponse.isAcknowledged() == false) {
                throw new ElasticsearchException("adding template for shield admin index was not acknowledged");
            }
        } catch (Exception e) {
            logger.error("failed to create shield admin index template [{}]",
                    e, SHIELD_ADMIN_INDEX_NAME);
            throw new IllegalStateException("failed to create shield admin index template [" +
                    SHIELD_ADMIN_INDEX_NAME + "]", e);
        }

    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .shield-audit-
            // but they may not have been restored from the cluster state on disk
            logger.debug("template service waiting until state has been recovered");
            return;
        }
        
        IndexRoutingTable shieldIndexRouting = event.state().routingTable().index(SHIELD_ADMIN_INDEX_NAME);

        if (shieldIndexRouting == null) {
            if (event.localNodeMaster()) {
                ClusterState state = event.state();
                // TODO for the future need to add some checking in the event the template needs to be updated...
                IndexTemplateMetaData templateMeta = state.metaData().templates().get(SHIELD_TEMPLATE_NAME);
                final boolean createTemplate = (templateMeta == null);

                if (createTemplate && templateCreationPending.compareAndSet(false, true)) {
                    threadPool.generic().execute(new AbstractRunnable() {
                        @Override
                        public void onFailure(Throwable t) {
                            logger.warn("failed to create shield admin template", t);
                            templateCreationPending.set(false);
                        }

                        @Override
                        protected void doRun() throws Exception {
                            if (createTemplate) {
                                createShieldTemplate();
                            }
                            templateCreationPending.set(false);
                        }
                    });
                }
            }
        }
    }
}
