/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.idp.IdentityProviderPlugin;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.opensaml.saml.saml2.core.NameID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
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

    @Before
    public void setupComponents() throws Exception {
        clusterService = super.getInstanceFromNode(ClusterService.class);
        serviceProviderIndex = new SamlServiceProviderIndex(client(), clusterService);
    }

    @After
    public void deleteTemplateAndIndex() {
        client().admin().indices().delete(new DeleteIndexRequest(SamlServiceProviderIndex.INDEX_NAME + "*")).actionGet();
        client().admin().indices().deleteTemplate(new DeleteIndexTemplateRequest(SamlServiceProviderIndex.TEMPLATE_NAME)).actionGet();
        serviceProviderIndex.close();
    }

    public void testWriteAndFindServiceProvidersFromIndex() {
        final int count = randomIntBetween(3, 5);
        List<SamlServiceProviderDocument> documents = new ArrayList<>(count);

        final ClusterService clusterService = super.getInstanceFromNode(ClusterService.class);
        // Install the template
        assertTrue("Template should have been installed", installTemplate());
        // No need to install it again
        assertFalse("Template should not have been installed a second time", installTemplate());

        // Index should not exist yet
        assertThat(clusterService.state().metaData().index(SamlServiceProviderIndex.INDEX_NAME), nullValue());

        for (int i = 0; i < count; i++) {
            final SamlServiceProviderDocument doc = randomDocument(i);
            writeDocument(serviceProviderIndex, doc);
            documents.add(doc);
        }

        final IndexMetaData indexMetaData = clusterService.state().metaData().index(SamlServiceProviderIndex.INDEX_NAME);
        assertThat(indexMetaData, notNullValue());
        assertThat(indexMetaData.getSettings().get("index.format"), equalTo("1"));
        assertThat(indexMetaData.getAliases().size(), equalTo(1));
        assertThat(indexMetaData.getAliases().keys().toArray(), arrayContainingInAnyOrder(SamlServiceProviderIndex.ALIAS_NAME));

        refresh(serviceProviderIndex);

        final Set<SamlServiceProviderDocument> allDocs = getAllDocs(serviceProviderIndex);
        assertThat(allDocs, iterableWithSize(count));
        for (SamlServiceProviderDocument doc : documents) {
            assertThat(allDocs, hasItem(Matchers.equalTo(doc)));
        }

        final SamlServiceProviderDocument readDoc = randomFrom(documents);
        assertThat(readDocument(serviceProviderIndex, readDoc.docId), equalTo(readDoc));

        final SamlServiceProviderDocument findDoc = randomFrom(documents);
        assertThat(findByEntityId(serviceProviderIndex, findDoc.entityId), equalTo(findDoc));
    }

    public void testWritesViaAliasIfItExists() {
        final PlainActionFuture<Boolean> installTemplate = new PlainActionFuture<>();
        serviceProviderIndex.installIndexTemplate(installTemplate);
        assertTrue(installTemplate.actionGet());

        // Create an index that will trigger the template, but isn't the standard index name
        final String customIndexName = SamlServiceProviderIndex.INDEX_NAME + "-test";
        client().admin().indices().create(new CreateIndexRequest(customIndexName)).actionGet();

        final IndexMetaData indexMetaData = clusterService.state().metaData().index(customIndexName);
        assertThat(indexMetaData, notNullValue());
        assertThat(indexMetaData.getSettings().get("index.format"), equalTo("1"));
        assertThat(indexMetaData.getAliases().size(), equalTo(1));
        assertThat(indexMetaData.getAliases().keys().toArray(), arrayContainingInAnyOrder(SamlServiceProviderIndex.ALIAS_NAME));

        SamlServiceProviderDocument document = randomDocument(1);
        writeDocument(serviceProviderIndex, document);

        // Index should not exist because we created an alternate index, and the alias points to that.
        assertThat(clusterService.state().metaData().index(SamlServiceProviderIndex.INDEX_NAME), nullValue());

        refresh(serviceProviderIndex);

        final Set<SamlServiceProviderDocument> allDocs = getAllDocs(serviceProviderIndex);
        assertThat(allDocs, iterableWithSize(1));
        assertThat(allDocs, hasItem(Matchers.equalTo(document)));

        assertThat(readDocument(serviceProviderIndex, document.docId), equalTo(document));
    }

    private boolean installTemplate() {
        final PlainActionFuture<Boolean> installTemplate = new PlainActionFuture<>();
        serviceProviderIndex.installIndexTemplate(installTemplate);
        return installTemplate.actionGet();
    }

    private Set<SamlServiceProviderDocument> getAllDocs(SamlServiceProviderIndex index) {
        final PlainActionFuture<Set<SamlServiceProviderDocument>> future = new PlainActionFuture<>();
        index.findAll(future);
        return future.actionGet();
    }

    private SamlServiceProviderDocument readDocument(SamlServiceProviderIndex index, String docId) {
        final PlainActionFuture<SamlServiceProviderDocument> future = new PlainActionFuture<>();
        index.readDocument(docId, future);
        return future.actionGet();
    }

    private void writeDocument(SamlServiceProviderIndex index, SamlServiceProviderDocument doc) {
        final PlainActionFuture<String> future = new PlainActionFuture<>();
        index.writeDocument(doc, future);
        doc.setDocId(future.actionGet());
    }


    private SamlServiceProviderDocument findByEntityId(SamlServiceProviderIndex index, String entityId) {
        final PlainActionFuture<Set<SamlServiceProviderDocument>> future = new PlainActionFuture<>();
        index.findByEntityId(entityId, future);
        final Set<SamlServiceProviderDocument> docs = future.actionGet();
        assertThat(docs, iterableWithSize(1));
        return docs.iterator().next();
    }

    private void refresh(SamlServiceProviderIndex index) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        index.refresh(future);
        future.actionGet();
    }

    private SamlServiceProviderDocument randomDocument(int index) {
        final SamlServiceProviderDocument document = new SamlServiceProviderDocument();
        document.setName(randomAlphaOfLengthBetween(5, 12));
        document.setEntityId(randomUri() + index);
        document.setAcs(randomUri("https") + index + "/saml/acs");

        document.setEnabled(randomBoolean());
        document.setCreatedMillis(System.currentTimeMillis() - TimeValue.timeValueDays(randomIntBetween(2, 90)).millis());
        document.setLastModifiedMillis(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(1, 36)).millis());

        if (randomBoolean()) {
            document.setNameIdFormats(randomSubsetOf(List.of(NameID.TRANSIENT, NameID.PERSISTENT)));
        }
        if (randomBoolean()) {
            document.setAuthenticationExpiryMillis(TimeValue.timeValueMinutes(randomIntBetween(1, 15)).millis());
        }

        document.privileges.setResource("app:" + randomAlphaOfLengthBetween(3, 6) + ":" + Math.abs(randomLong()));
        if (randomBoolean()) {
            document.privileges.setApplication(randomAlphaOfLengthBetween(4, 12));
        }
        if (randomBoolean()) {
            document.privileges.setLoginAction(randomAlphaOfLengthBetween(3, 6) + ":" + randomAlphaOfLengthBetween(3, 6));
        }
        final int groupCount = randomIntBetween(0, 4);
        final Map<String, String> groups = new HashMap<>();
        for (int i = 0; i < groupCount; i++) {
            groups.put(randomAlphaOfLengthBetween(4, 8), randomAlphaOfLengthBetween(3, 6) + ":" + randomAlphaOfLengthBetween(3, 6));
        }
        document.privileges.setGroupActions(groups);

        document.attributeNames.setPrincipal(randomUri());
        if (randomBoolean()) {
            document.attributeNames.setName(randomUri());
        }
        if (randomBoolean()) {
            document.attributeNames.setEmail(randomUri());
        }
        if (groups.isEmpty() == false) {
            document.attributeNames.setGroups(randomUri());
        }

        assertThat(document.validate(), nullValue());
        return document;
    }

    private String randomUri() {
        return randomUri(randomFrom("urn", "http", "https"));
    }

    private String randomUri(String scheme) {
        return scheme + "://" + randomAlphaOfLengthBetween(2, 6) + "."
            + randomAlphaOfLengthBetween(4, 8) + "." + randomAlphaOfLengthBetween(2, 4) + "/";
    }

}
