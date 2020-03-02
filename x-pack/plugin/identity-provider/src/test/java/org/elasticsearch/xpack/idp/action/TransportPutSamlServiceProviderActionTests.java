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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderDocument;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndexTests;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TransportPutSamlServiceProviderActionTests extends ESTestCase {

    private SamlServiceProviderIndex index;
    private TransportPutSamlServiceProviderAction action;
    private Instant now;

    @Before
    public void setupMocks() {
        index = mock(SamlServiceProviderIndex.class);
        now = Instant.ofEpochMilli(System.currentTimeMillis() + randomLongBetween(-500_000, 500_000));
        final Clock clock = Clock.fixed(now, randomZone());
        action = new TransportPutSamlServiceProviderAction(
            mock(TransportService.class), mock(ActionFilters.class), index, clock);
    }

    public void testRegisterNewServiceProvider() throws Exception {
        final SamlServiceProviderDocument document = SamlServiceProviderIndexTests.randomDocument();

        mockExistingDocuments(document.entityId, Set.of());

        final AtomicReference<DocWriteResponse> writeResponse = mockWriteResponse(document, true);

        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(document);
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

        final PutSamlServiceProviderRequest request = new PutSamlServiceProviderRequest(document);
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

    public AtomicReference<DocWriteResponse> mockWriteResponse(SamlServiceProviderDocument document, boolean created) {
        final AtomicReference<DocWriteResponse> writeResponse = new AtomicReference<>();
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, Matchers.arrayWithSize(3));

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
        }).when(index).writeDocument(any(SamlServiceProviderDocument.class), any(DocWriteRequest.OpType.class), any(ActionListener.class));

        return writeResponse;
    }

    public void mockExistingDocuments(String expectedEntityId, Set<SamlServiceProviderDocument> documents) {
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, Matchers.arrayWithSize(2));

            String entityId = (String) args[0];
            assertThat(entityId, equalTo(expectedEntityId));

            ActionListener listener = (ActionListener) args[args.length - 1];
            listener.onResponse(documents);

            return null;
        }).when(index).findByEntityId(anyString(), any(ActionListener.class));
    }

}
