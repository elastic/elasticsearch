/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class provides utility methods to read/write {@link SamlServiceProviderDocument} to an Elasticsearch index.
 */
public class SamlServiceProviderIndex implements Closeable {

    private final Logger logger = LogManager.getLogger();

    private final Client client;
    private final ClusterService clusterService;
    private volatile boolean aliasExists;
    private volatile boolean templateInstalled;

    static final String ALIAS_NAME = "saml-service-provider";
    static final String INDEX_NAME = "saml-service-provider-v1";
    static final String TEMPLATE_NAME = ALIAS_NAME;

    private static final String TEMPLATE_RESOURCE = "/index/saml-service-provider-template.json";
    private static final String TEMPLATE_META_VERSION_KEY = "idp-version";
    private static final String TEMPLATE_VERSION_SUBSTITUTE = "idp.template.version";
    private final ClusterStateListener clusterStateListener;

    public SamlServiceProviderIndex(Client client, ClusterService clusterService) {
        this.client = new OriginSettingClient(client, ClientHelper.IDP_ORIGIN);
        this.clusterService = clusterService;
        this.clusterStateListener = this::clusterChanged;
        clusterService.addListener(clusterStateListener);
    }

    private void clusterChanged(ClusterChangedEvent clusterChangedEvent) {
        final ClusterState state = clusterChangedEvent.state();
        installTemplateIfRequired(state);
        checkForAliasStateChange(state);
    }

    private void installTemplateIfRequired(ClusterState state) {
        if (templateInstalled) {
            return;
        }
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (isTemplateUpToDate(state)) {
            templateInstalled = true;
            return;
        }
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }
        installIndexTemplate(ActionListener.wrap(
            installed -> {
                templateInstalled = true;
                if (installed) {
                    logger.debug("Template [{}] has been updated", TEMPLATE_NAME);
                } else {
                    logger.debug("Template [{}] appears to be up to date", TEMPLATE_NAME);
                }
            }, e -> logger.warn(new ParameterizedMessage("Failed to install template [{}]", TEMPLATE_NAME), e)
        ));
    }

    private void checkForAliasStateChange(ClusterState state) {
        final AliasOrIndex aliasInfo = state.getMetaData().getAliasAndIndexLookup().get(ALIAS_NAME);
        final boolean previousState = aliasExists;
        this.aliasExists = aliasInfo != null;
        if (aliasExists != previousState) {
            logChangedAliasState(aliasInfo);
        }
    }

    @Override
    public void close() {
        logger.debug("Closing ... removing cluster state listener");
        clusterService.removeListener(clusterStateListener);
    }

    private void logChangedAliasState(AliasOrIndex aliasInfo) {
        if (aliasInfo == null) {
            logger.warn("service provider index/alias [{}] no longer exists", ALIAS_NAME);
        } else if (aliasInfo.isAlias() == false) {
            logger.warn("service provider index [{}] exists as a concrete index, but it should be an alias", ALIAS_NAME);
        } else if (aliasInfo.getIndices().size() != 1) {
            logger.warn("service provider alias [{}] refers to multiple indices [{}] - this is unexpected and is likely to cause problems",
                ALIAS_NAME, Strings.collectionToCommaDelimitedString(aliasInfo.getIndices()));
        } else {
            logger.info("service provider alias [{}] refers to [{}]", ALIAS_NAME, aliasInfo.getIndices().get(0).getIndex());
        }
    }

    public void installIndexTemplate(ActionListener<Boolean> listener) {
        final ClusterState state = clusterService.state();
        if (isTemplateUpToDate(state)) {
            listener.onResponse(false);
        }
        final String template = TemplateUtils.loadTemplate(TEMPLATE_RESOURCE, Version.CURRENT.toString(), TEMPLATE_VERSION_SUBSTITUTE);
        final PutIndexTemplateRequest request = new PutIndexTemplateRequest(TEMPLATE_NAME).source(template, XContentType.JSON);
        client.admin().indices().putTemplate(request, ActionListener.wrap(response -> {
            logger.info("Installed template [{}]", TEMPLATE_NAME);
            listener.onResponse(true);
        }, listener::onFailure));
    }

    private boolean isTemplateUpToDate(ClusterState state) {
        return TemplateUtils.checkTemplateExistsAndIsUpToDate(TEMPLATE_NAME, TEMPLATE_META_VERSION_KEY, state, logger);
    }

    public void writeDocument(SamlServiceProviderDocument document, ActionListener<String> listener) {
        final ValidationException exception = document.validate();
        if (exception != null) {
            listener.onFailure(exception);
            return;
        }
        if (templateInstalled) {
            _writeDocument(document, listener);
        } else {
            installIndexTemplate(ActionListener.wrap(installed -> _writeDocument(document, listener), listener::onFailure));
        }
    }

    private void _writeDocument(SamlServiceProviderDocument document, ActionListener<String> listener) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             XContentBuilder xContentBuilder = new XContentBuilder(XContentType.JSON.xContent(), out)) {
            document.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            // Due to the lack of "alias templates" (at the current time), we cannot write to the alias if it doesn't exist yet
            // - that would cause the alias to be created as a concrete index, which is not what we want.
            // So, until we know that the alias exists we have to write to the expected index name instead.
            final IndexRequest request = new IndexRequest(aliasExists ? ALIAS_NAME : INDEX_NAME)
                .source(xContentBuilder)
                .id(document.docId);
            client.index(request, ActionListener.wrap(response -> {
                logger.debug("Wrote service provider [{}][{}] as document [{}]", document.name, document.entityId, response.getId());
                listener.onResponse(response.getId());
            }, listener::onFailure));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    public void readDocument(String documentId, ActionListener<SamlServiceProviderDocument> listener) {
        final GetRequest request = new GetRequest(ALIAS_NAME, documentId);
        client.get(request, ActionListener.wrap(response -> {
            final SamlServiceProviderDocument document = toDocument(documentId, response.getSourceAsBytesRef());
            listener.onResponse(document);
        }, listener::onFailure));
    }

    public void findByEntityId(String entityId, ActionListener<Set<SamlServiceProviderDocument>> listener) {
        final QueryBuilder query = QueryBuilders.termQuery(SamlServiceProviderDocument.Fields.ENTITY_ID.getPreferredName(), entityId);
        findDocuments(query, listener);
    }

    public void findAll(ActionListener<Set<SamlServiceProviderDocument>> listener) {
        final QueryBuilder query = QueryBuilders.matchAllQuery();
        findDocuments(query, listener);
    }

    public void refresh(ActionListener<Void> listener) {
        client.admin().indices().refresh(new RefreshRequest(ALIAS_NAME), ActionListener.wrap(
            response -> listener.onResponse(null), listener::onFailure));
    }

    private void findDocuments(QueryBuilder query, ActionListener<Set<SamlServiceProviderDocument>> listener) {
        logger.trace("Searching [{}] for [{}]", ALIAS_NAME, query);
        final SearchRequest request = client.prepareSearch(ALIAS_NAME)
            .setQuery(query)
            .setSize(1000)
            .setFetchSource(true)
            .request();
        client.search(request, ActionListener.wrap(response -> {
            logger.trace("Search hits: [{}] [{}]", response.getHits().getTotalHits(), Arrays.toString(response.getHits().getHits()));
            final Set<SamlServiceProviderDocument> docs = Stream.of(response.getHits().getHits())
                .map(hit -> toDocument(hit.getId(), hit.getSourceRef()))
                .collect(Collectors.toUnmodifiableSet());
            listener.onResponse(docs);
        }, listener::onFailure));
    }

    private SamlServiceProviderDocument toDocument(String documentId, BytesReference source) {
        try (StreamInput in = source.streamInput();
             XContentParser parser = XContentType.JSON.xContent().createParser(
                 NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, in)) {
            return SamlServiceProviderDocument.fromXContent(documentId, parser);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse document [" + documentId + "]", e);
        }
    }
}
