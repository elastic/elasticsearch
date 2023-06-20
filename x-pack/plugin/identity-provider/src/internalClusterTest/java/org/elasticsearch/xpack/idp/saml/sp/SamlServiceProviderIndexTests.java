/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.idp.IdentityProviderPlugin;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_SSO_REDIRECT_ENDPOINT;
import static org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderTestUtils.randomDocument;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SamlServiceProviderIndexTests extends ESSingleNodeTestCase {

    private ClusterService clusterService;
    private SamlServiceProviderIndex serviceProviderIndex;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, IdentityProviderPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(IDP_ENTITY_ID.getKey(), "urn:idp:org")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/init")
            .put(super.nodeSettings())
            .build();
    }

    @Before
    public void setupComponents() throws Exception {
        clusterService = super.getInstanceFromNode(ClusterService.class);
        serviceProviderIndex = new SamlServiceProviderIndex(client(), clusterService);
    }

    @After
    public void deleteTemplateAndIndex() {
        indicesAdmin().delete(new DeleteIndexRequest(SamlServiceProviderIndex.INDEX_NAME + "*")).actionGet();
        indicesAdmin().prepareDeleteTemplate(SamlServiceProviderIndex.TEMPLATE_NAME).get();
        serviceProviderIndex.close();
    }

    public void testWriteAndFindServiceProvidersFromIndex() {
        final int count = randomIntBetween(3, 5);
        List<SamlServiceProviderDocument> documents = new ArrayList<>(count);

        // Install the template
        assertTrue("Template should have been installed", installTemplate());
        // No need to install it again
        assertFalse("Template should not have been installed a second time", installTemplate());

        // Index should not exist yet
        assertThat(clusterService.state().metadata().index(SamlServiceProviderIndex.INDEX_NAME), nullValue());

        for (int i = 0; i < count; i++) {
            final SamlServiceProviderDocument doc = randomDocument(i);
            writeDocument(doc);
            documents.add(doc);
        }

        final IndexMetadata indexMetadata = clusterService.state().metadata().index(SamlServiceProviderIndex.INDEX_NAME);
        assertThat(indexMetadata, notNullValue());
        assertThat(indexMetadata.getSettings().get("index.format"), equalTo("1"));
        assertThat(indexMetadata.getAliases().size(), equalTo(1));
        assertThat(indexMetadata.getAliases().keySet().toArray(), arrayContainingInAnyOrder(SamlServiceProviderIndex.ALIAS_NAME));

        refresh();

        final Set<SamlServiceProviderDocument> allDocs = getAllDocs();
        assertThat(allDocs, iterableWithSize(count));
        for (SamlServiceProviderDocument doc : documents) {
            assertThat(allDocs, hasItem(Matchers.equalTo(doc)));
        }

        final SamlServiceProviderDocument readDoc = randomFrom(documents);
        assertThat(readDocument(readDoc.docId), equalTo(readDoc));

        final SamlServiceProviderDocument findDoc = randomFrom(documents);
        assertThat(findByEntityId(findDoc.entityId), equalTo(findDoc));

        final SamlServiceProviderDocument deleteDoc = randomFrom(documents);
        final DeleteResponse deleteResponse = deleteDocument(deleteDoc);
        assertThat(deleteResponse.getId(), equalTo(deleteDoc.docId));
        assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));

        refresh();

        assertThat(readDocument(deleteDoc.docId), nullValue());
        assertThat(findAllByEntityId(deleteDoc.entityId), emptyIterable());
    }

    public void testWritesViaAliasIfItExists() {
        assertTrue(installTemplate());

        // Create an index that will trigger the template, but isn't the standard index name
        final String customIndexName = SamlServiceProviderIndex.INDEX_NAME + "-test";
        indicesAdmin().create(new CreateIndexRequest(customIndexName)).actionGet();

        final IndexMetadata indexMetadata = clusterService.state().metadata().index(customIndexName);
        assertThat(indexMetadata, notNullValue());
        assertThat(indexMetadata.getSettings().get("index.format"), equalTo("1"));
        assertThat(indexMetadata.getAliases().size(), equalTo(1));
        assertThat(indexMetadata.getAliases().keySet().toArray(), arrayContainingInAnyOrder(SamlServiceProviderIndex.ALIAS_NAME));

        SamlServiceProviderDocument document = randomDocument(1);
        writeDocument(document);

        // Index should not exist because we created an alternate index, and the alias points to that.
        assertThat(clusterService.state().metadata().index(SamlServiceProviderIndex.INDEX_NAME), nullValue());

        refresh();

        final Set<SamlServiceProviderDocument> allDocs = getAllDocs();
        assertThat(allDocs, iterableWithSize(1));
        assertThat(allDocs, hasItem(Matchers.equalTo(document)));

        assertThat(readDocument(document.docId), equalTo(document));
    }

    public void testInstallTemplateAutomaticallyOnClusterChange() throws Exception {
        // Create an index that will trigger a cluster state change
        final String indexName = randomAlphaOfLength(7).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(indexName)).actionGet();

        ensureGreen(indexName);

        IndexTemplateMetadata templateMeta = clusterService.state().metadata().templates().get(SamlServiceProviderIndex.TEMPLATE_NAME);

        assertBusy(() -> assertThat("template should have been installed", templateMeta, notNullValue()));

        assertFalse("Template is already installed, should not install again", installTemplate());
    }

    public void testInstallTemplateAutomaticallyOnDocumentWrite() {
        final SamlServiceProviderDocument doc = randomDocument(1);
        writeDocument(doc);

        assertThat(readDocument(doc.docId), equalTo(doc));

        IndexTemplateMetadata templateMeta = clusterService.state().metadata().templates().get(SamlServiceProviderIndex.TEMPLATE_NAME);
        assertThat("template should have been installed", templateMeta, notNullValue());

        assertFalse("Template is already installed, should not install again", installTemplate());
    }

    private boolean installTemplate() {
        final PlainActionFuture<Boolean> installTemplate = new PlainActionFuture<>();
        serviceProviderIndex.installIndexTemplate(assertListenerIsOnlyCalledOnce(installTemplate));
        return installTemplate.actionGet();
    }

    private Set<SamlServiceProviderDocument> getAllDocs() {
        final PlainActionFuture<Set<SamlServiceProviderDocument>> future = new PlainActionFuture<>();
        serviceProviderIndex.findAll(
            assertListenerIsOnlyCalledOnce(
                ActionListener.wrap(
                    set -> future.onResponse(set.stream().map(doc -> doc.document.get()).collect(Collectors.toUnmodifiableSet())),
                    future::onFailure
                )
            )
        );
        return future.actionGet();
    }

    private SamlServiceProviderDocument readDocument(String docId) {
        final PlainActionFuture<SamlServiceProviderIndex.DocumentSupplier> future = new PlainActionFuture<>();
        serviceProviderIndex.readDocument(docId, assertListenerIsOnlyCalledOnce(future));
        final SamlServiceProviderIndex.DocumentSupplier supplier = future.actionGet();
        return supplier == null ? null : supplier.getDocument();
    }

    private void writeDocument(SamlServiceProviderDocument doc) {
        final PlainActionFuture<DocWriteResponse> future = new PlainActionFuture<>();
        serviceProviderIndex.writeDocument(
            doc,
            DocWriteRequest.OpType.INDEX,
            WriteRequest.RefreshPolicy.WAIT_UNTIL,
            assertListenerIsOnlyCalledOnce(future)
        );
        doc.setDocId(future.actionGet().getId());
    }

    private DeleteResponse deleteDocument(SamlServiceProviderDocument doc) {
        final PlainActionFuture<DeleteResponse> future = new PlainActionFuture<>();
        serviceProviderIndex.readDocument(
            doc.docId,
            assertListenerIsOnlyCalledOnce(
                ActionListener.wrap(
                    info -> serviceProviderIndex.deleteDocument(info.version, WriteRequest.RefreshPolicy.IMMEDIATE, future),
                    future::onFailure
                )
            )
        );
        return future.actionGet();
    }

    private SamlServiceProviderDocument findByEntityId(String entityId) {
        final Set<SamlServiceProviderDocument> docs = findAllByEntityId(entityId);
        assertThat(docs, iterableWithSize(1));
        return docs.iterator().next();
    }

    private Set<SamlServiceProviderDocument> findAllByEntityId(String entityId) {
        final PlainActionFuture<Set<SamlServiceProviderDocument>> future = new PlainActionFuture<>();
        serviceProviderIndex.findByEntityId(
            entityId,
            assertListenerIsOnlyCalledOnce(
                ActionListener.wrap(
                    set -> future.onResponse(set.stream().map(doc -> doc.document.get()).collect(Collectors.toUnmodifiableSet())),
                    future::onFailure
                )
            )
        );
        return future.actionGet();
    }

    private void refresh() {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        serviceProviderIndex.refresh(assertListenerIsOnlyCalledOnce(future));
        future.actionGet();
    }

    private static <T> ActionListener<T> assertListenerIsOnlyCalledOnce(ActionListener<T> delegate) {
        final AtomicInteger callCount = new AtomicInteger(0);
        return ActionListener.runBefore(delegate, () -> {
            if (callCount.incrementAndGet() != 1) {
                fail("Listener was called twice");
            }
        });
    }

}
