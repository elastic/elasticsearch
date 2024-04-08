/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class provides utility methods to read/write {@link SamlServiceProviderDocument} to an Elasticsearch index.
 */
public class SamlServiceProviderIndex implements Closeable {

    private static final Logger logger = LogManager.getLogger(SamlServiceProviderIndex.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ClusterStateListener clusterStateListener;
    private volatile boolean aliasExists;
    private volatile boolean templateInstalled;

    public static final String ALIAS_NAME = "saml-service-provider";
    public static final String INDEX_NAME = "saml-service-provider-v1";
    static final String TEMPLATE_NAME = ALIAS_NAME;

    private static final String TEMPLATE_RESOURCE = "/idp/saml-service-provider-template.json";
    private static final String TEMPLATE_META_VERSION_KEY = "idp-version";
    private static final String TEMPLATE_VERSION_SUBSTITUTE = "idp.template.version";

    public static final class DocumentVersion {
        public final String id;
        public final long primaryTerm;
        public final long seqNo;

        public DocumentVersion(String id, long primaryTerm, long seqNo) {
            this.id = id;
            this.primaryTerm = primaryTerm;
            this.seqNo = seqNo;
        }

        public DocumentVersion(GetResponse get) {
            this(get.getId(), get.getPrimaryTerm(), get.getSeqNo());
        }

        public DocumentVersion(GetResult get) {
            this(get.getId(), get.getPrimaryTerm(), get.getSeqNo());
        }

        public DocumentVersion(SearchHit hit) {
            this(hit.getId(), hit.getPrimaryTerm(), hit.getSeqNo());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final DocumentVersion that = (DocumentVersion) o;
            return Objects.equals(this.id, that.id) && primaryTerm == that.primaryTerm && seqNo == that.seqNo;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, primaryTerm, seqNo);
        }
    }

    public static final class DocumentSupplier {
        public final DocumentVersion version;
        public final Supplier<SamlServiceProviderDocument> document;

        public DocumentSupplier(DocumentVersion version, Supplier<SamlServiceProviderDocument> document) {
            this.version = version;
            this.document = CachedSupplier.wrap(document);
        }

        public SamlServiceProviderDocument getDocument() {
            return document.get();
        }
    }

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
        installIndexTemplate(ActionListener.wrap(installed -> {
            templateInstalled = true;
            if (installed) {
                logger.debug("Template [{}] has been updated", TEMPLATE_NAME);
            } else {
                logger.debug("Template [{}] appears to be up to date", TEMPLATE_NAME);
            }
        }, e -> logger.warn(() -> "Failed to install template [" + TEMPLATE_NAME + "]", e)));
    }

    private void checkForAliasStateChange(ClusterState state) {
        final IndexAbstraction aliasInfo = state.getMetadata().getIndicesLookup().get(ALIAS_NAME);
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

    private void logChangedAliasState(IndexAbstraction aliasInfo) {
        if (aliasInfo == null) {
            logger.warn("service provider index/alias [{}] no longer exists", ALIAS_NAME);
        } else if (aliasInfo.getType() != IndexAbstraction.Type.ALIAS) {
            logger.warn("service provider index [{}] does not exist as an alias, but it should be", ALIAS_NAME);
        } else if (aliasInfo.getIndices().size() != 1) {
            logger.warn(
                "service provider alias [{}] refers to multiple indices [{}] - this is unexpected and is likely to cause problems",
                ALIAS_NAME,
                Strings.collectionToCommaDelimitedString(aliasInfo.getIndices())
            );
        } else {
            logger.info("service provider alias [{}] refers to [{}]", ALIAS_NAME, aliasInfo.getIndices().get(0));
        }
    }

    public void installIndexTemplate(ActionListener<Boolean> listener) {
        final ClusterState state = clusterService.state();
        if (isTemplateUpToDate(state)) {
            listener.onResponse(false);
            return;
        }
        final String template = TemplateUtils.loadTemplate(TEMPLATE_RESOURCE, Version.CURRENT.toString(), TEMPLATE_VERSION_SUBSTITUTE);
        final PutIndexTemplateRequest request = new PutIndexTemplateRequest(TEMPLATE_NAME).source(template, XContentType.JSON);
        client.admin().indices().putTemplate(request, listener.delegateFailureAndWrap((l, response) -> {
            logger.info("Installed template [{}]", TEMPLATE_NAME);
            l.onResponse(true);
        }));
    }

    private boolean isTemplateUpToDate(ClusterState state) {
        return TemplateUtils.checkTemplateExistsAndIsUpToDate(TEMPLATE_NAME, TEMPLATE_META_VERSION_KEY, state, logger);
    }

    public void deleteDocument(DocumentVersion version, WriteRequest.RefreshPolicy refreshPolicy, ActionListener<DeleteResponse> listener) {
        final DeleteRequest request = new DeleteRequest(aliasExists ? ALIAS_NAME : INDEX_NAME).id(version.id)
            .setIfSeqNo(version.seqNo)
            .setIfPrimaryTerm(version.primaryTerm)
            .setRefreshPolicy(refreshPolicy);
        client.delete(request, listener.delegateFailureAndWrap((l, response) -> {
            logger.debug("Deleted service provider document [{}] ({})", version.id, response.getResult());
            l.onResponse(response);
        }));
    }

    public void writeDocument(
        SamlServiceProviderDocument document,
        DocWriteRequest.OpType opType,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<DocWriteResponse> listener
    ) {
        final ValidationException exception = document.validate();
        if (exception != null) {
            listener.onFailure(exception);
            return;
        }

        if (templateInstalled) {
            _writeDocument(document, opType, refreshPolicy, listener);
        } else {
            installIndexTemplate(listener.delegateFailureAndWrap((l, installed) -> _writeDocument(document, opType, refreshPolicy, l)));
        }
    }

    private void _writeDocument(
        SamlServiceProviderDocument document,
        DocWriteRequest.OpType opType,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<DocWriteResponse> listener
    ) {
        try (
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            XContentBuilder xContentBuilder = new XContentBuilder(XContentType.JSON.xContent(), out)
        ) {
            document.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            // Due to the lack of "alias templates" (at the current time), we cannot write to the alias if it doesn't exist yet
            // - that would cause the alias to be created as a concrete index, which is not what we want.
            // So, until we know that the alias exists we have to write to the expected index name instead.
            final IndexRequest request = new IndexRequest(aliasExists ? ALIAS_NAME : INDEX_NAME).opType(opType)
                .source(xContentBuilder)
                .id(document.docId)
                .setRefreshPolicy(refreshPolicy);
            client.index(request, listener.delegateFailureAndWrap((l, response) -> {
                logger.debug(
                    "Wrote service provider [{}][{}] as document [{}] ({})",
                    document.name,
                    document.entityId,
                    response.getId(),
                    response.getResult()
                );
                l.onResponse(response);
            }));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    public void readDocument(String documentId, ActionListener<DocumentSupplier> listener) {
        final GetRequest request = new GetRequest(ALIAS_NAME, documentId);
        client.get(request, listener.delegateFailureAndWrap((l, response) -> {
            if (response.isExists()) {
                l.onResponse(
                    new DocumentSupplier(new DocumentVersion(response), () -> toDocument(documentId, response.getSourceAsBytesRef()))
                );
            } else {
                l.onResponse(null);
            }
        }));
    }

    public void findByEntityId(String entityId, ActionListener<Set<DocumentSupplier>> listener) {
        final QueryBuilder query = QueryBuilders.termQuery(SamlServiceProviderDocument.Fields.ENTITY_ID.getPreferredName(), entityId);
        findDocuments(query, listener);
    }

    public void findAll(ActionListener<Set<DocumentSupplier>> listener) {
        final QueryBuilder query = QueryBuilders.matchAllQuery();
        findDocuments(query, listener);
    }

    public void refresh(ActionListener<Void> listener) {
        client.admin()
            .indices()
            .refresh(new RefreshRequest(ALIAS_NAME), listener.delegateFailureAndWrap((l, response) -> l.onResponse(null)));
    }

    private void findDocuments(QueryBuilder query, ActionListener<Set<DocumentSupplier>> listener) {
        logger.trace("Searching [{}] for [{}]", ALIAS_NAME, query);
        final SearchRequest request = client.prepareSearch(ALIAS_NAME).setQuery(query).setSize(1000).setFetchSource(true).request();
        client.search(request, ActionListener.wrap(response -> {
            if (logger.isTraceEnabled()) {
                logger.trace("Search hits: [{}] [{}]", response.getHits().getTotalHits(), Arrays.toString(response.getHits().getHits()));
            }
            final Set<DocumentSupplier> docs = Stream.of(response.getHits().getHits())
                .map(hit -> new DocumentSupplier(new DocumentVersion(hit), () -> toDocument(hit.getId(), hit.getSourceRef())))
                .collect(Collectors.toUnmodifiableSet());
            listener.onResponse(docs);
        }, ex -> {
            if (ex instanceof IndexNotFoundException) {
                listener.onResponse(Set.of());
            } else {
                listener.onFailure(ex);
            }
        }));
    }

    private static SamlServiceProviderDocument toDocument(String documentId, BytesReference source) {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                source,
                XContentType.JSON
            )
        ) {
            return SamlServiceProviderDocument.fromXContent(documentId, parser);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse document [" + documentId + "]", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{alias=" + ALIAS_NAME + " [" + (aliasExists ? "exists" : "not-found") + "]}";
    }
}
