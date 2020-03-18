/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderDocument;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.DocumentVersion;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndexTests;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensaml.saml.saml2.core.NameIDType.EMAIL;
import static org.opensaml.saml.saml2.core.NameIDType.PERSISTENT;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public class TransportPutSamlServiceProviderActionTests extends ESTestCase {

    private SamlServiceProviderIndex index;
    private TransportPutSamlServiceProviderAction action;
    private SamlIdentityProvider idp;
    private Instant now;

    @Before
    public void setupMocks() {
        index = mock(SamlServiceProviderIndex.class);
        idp = mock(SamlIdentityProvider.class);
        when(idp.getAllowedNameIdFormats()).thenReturn(Set.of(TRANSIENT));

        now = Instant.ofEpochMilli(System.currentTimeMillis() + randomLongBetween(-500_000, 500_000));
        final Clock clock = Clock.fixed(now, randomZone());
        action = new TransportPutSamlServiceProviderAction(
            mock(TransportService.class), mock(ActionFilters.class), index, idp, clock);
    }

    public void testRegisterNewServiceProvider() throws Exception {
        final SamlServiceProviderDocument document = SamlServiceProviderIndexTests.randomDocument();

        mockExistingDocuments(document.entityId, Set.of());

        final AtomicReference<DocWriteResponse> writeResponse = mockWriteResponse(document, true);

        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(document, randomFrom(RefreshPolicy.values()));
        final PlainActionFuture<PutSamlServiceProviderResponse> future = new PlainActionFuture<>();
        action.doExecute(null, request, future);

        final PutSamlServiceProviderResponse response = future.actionGet();
        assertThat(response, notNullValue());
        assertThat(response.getDocId(), equalTo(writeResponse.get().getId()));
        assertThat(response.getEntityId(), equalTo(document.entityId));
        assertThat(response.isCreated(), equalTo(true));
        assertThat(response.isEnabled(), equalTo(document.enabled));
        assertThat(response.getPrimaryTerm(), equalTo(writeResponse.get().getPrimaryTerm()));
        assertThat(response.getSeqNo(), equalTo(writeResponse.get().getSeqNo()));

        assertThat(document.created, equalTo(now));
        assertThat(document.lastModified, equalTo(now));
    }

    public void testUpdateExistingServiceProvider() throws Exception {
        final SamlServiceProviderDocument document = SamlServiceProviderIndexTests.randomDocument();

        final SamlServiceProviderDocument existingDocument = SamlServiceProviderIndexTests.randomDocument();
        existingDocument.entityId = document.entityId;
        existingDocument.docId = randomAlphaOfLength(42);
        mockExistingDocuments(document.entityId, Set.of(existingDocument));

        final AtomicReference<DocWriteResponse> writeResponse = mockWriteResponse(document, false);

        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(document, randomFrom(RefreshPolicy.values()));
        final PlainActionFuture<PutSamlServiceProviderResponse> future = new PlainActionFuture<>();
        action.doExecute(null, request, future);

        final PutSamlServiceProviderResponse response = future.actionGet();
        assertThat(response, notNullValue());
        assertThat(response.getDocId(), equalTo(existingDocument.docId));
        assertThat(response.getDocId(), equalTo(writeResponse.get().getId()));
        assertThat(response.getEntityId(), equalTo(document.entityId));
        assertThat(response.isCreated(), equalTo(false));
        assertThat(response.isEnabled(), equalTo(document.enabled));
        assertThat(response.getPrimaryTerm(), equalTo(writeResponse.get().getPrimaryTerm()));
        assertThat(response.getSeqNo(), equalTo(writeResponse.get().getSeqNo()));

        assertThat(document.created, equalTo(existingDocument.created));
        assertThat(document.lastModified, equalTo(now));
    }

    public void testUnsupportedNameIDFormat() throws Exception {
        final SamlServiceProviderDocument document = SamlServiceProviderIndexTests.randomDocument();
        final String invalidFormat = randomFrom(PERSISTENT, EMAIL, randomAlphaOfLength(12));
        document.setNameIdFormat(invalidFormat);

        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(document, randomFrom(RefreshPolicy.values()));
        final PlainActionFuture<PutSamlServiceProviderResponse> future = new PlainActionFuture<>();
        action.doExecute(null, request, future);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("NameID format [" + invalidFormat + "] is not supported"));
    }

    public AtomicReference<DocWriteResponse> mockWriteResponse(SamlServiceProviderDocument document, boolean created) {
        final AtomicReference<DocWriteResponse> writeResponse = new AtomicReference<>();
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, Matchers.arrayWithSize(4));

            SamlServiceProviderDocument doc = (SamlServiceProviderDocument) args[0];
            assertThat(doc.getDocId(), notNullValue());
            assertThat(doc, sameInstance(document));

            final DocWriteResponse docWriteResponse = new IndexResponse(
                new ShardId(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLength(24), randomIntBetween(1, 10)),
                doc.docId, randomLong(), randomLong(), randomLong(), created);
            writeResponse.set(docWriteResponse);

            ActionListener listener = (ActionListener) args[args.length - 1];
            listener.onResponse(docWriteResponse);

            return null;
        }).when(index).writeDocument(any(SamlServiceProviderDocument.class), any(DocWriteRequest.OpType.class),
            any(WriteRequest.RefreshPolicy.class), any(ActionListener.class));

        return writeResponse;
    }

    public void mockExistingDocuments(String expectedEntityId, Set<SamlServiceProviderDocument> documents) {
        final Set<SamlServiceProviderIndex.DocumentSupplier> documentSuppliers = documents.stream()
            .map(doc -> new SamlServiceProviderIndex.DocumentSupplier(
                new DocumentVersion(randomAlphaOfLength(24), randomLong(), randomLong()),
                () -> doc))
            .collect(Collectors.toUnmodifiableSet());
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, Matchers.arrayWithSize(2));

            String entityId = (String) args[0];
            assertThat(entityId, equalTo(expectedEntityId));

            ActionListener<Set<SamlServiceProviderIndex.DocumentSupplier>> listener = (ActionListener) args[args.length - 1];
            listener.onResponse(documentSuppliers);

            return null;
        }).when(index).findByEntityId(anyString(), any(ActionListener.class));
    }
}
