/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderDocument;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.stream.Collectors;

public class TransportPutSamlServiceProviderAction extends HandledTransportAction<
    PutSamlServiceProviderRequest,
    PutSamlServiceProviderResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutSamlServiceProviderAction.class);
    private final SamlServiceProviderIndex index;
    private final SamlIdentityProvider identityProvider;
    private final Clock clock;

    @Inject
    public TransportPutSamlServiceProviderAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SamlServiceProviderIndex index,
        SamlIdentityProvider identityProvider
    ) {
        this(transportService, actionFilters, index, identityProvider, Clock.systemUTC());
    }

    TransportPutSamlServiceProviderAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SamlServiceProviderIndex index,
        SamlIdentityProvider identityProvider,
        Clock clock
    ) {
        super(
            PutSamlServiceProviderAction.NAME,
            transportService,
            actionFilters,
            PutSamlServiceProviderRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.index = index;
        this.identityProvider = identityProvider;
        this.clock = clock;
    }

    @Override
    protected void doExecute(
        Task task,
        final PutSamlServiceProviderRequest request,
        final ActionListener<PutSamlServiceProviderResponse> listener
    ) {
        final SamlServiceProviderDocument document = request.getDocument();
        if (document.docId != null) {
            listener.onFailure(new IllegalArgumentException("request document must not have an id [" + document.docId + "]"));
            return;
        }
        if (document.nameIdFormat != null && identityProvider.getAllowedNameIdFormats().contains(document.nameIdFormat) == false) {
            listener.onFailure(new IllegalArgumentException("NameID format [" + document.nameIdFormat + "] is not supported."));
            return;
        }
        logger.trace("Searching for existing ServiceProvider with id [{}] for [{}]", document.entityId, request);
        index.findByEntityId(document.entityId, listener.delegateFailureAndWrap((delegate, matchingDocuments) -> {
            if (matchingDocuments.isEmpty()) {
                // derive a document id from the entity id so that don't accidentally create duplicate entities due to a race condition
                document.docId = deriveDocumentId(document);
                // force a create in case there are concurrent requests. This way, if two nodes/threads are trying to create the SP at
                // the same time, one will fail. That's not ideal, but it's better than having 1 silently overwrite the other.
                logger.trace("No existing ServiceProvider for EntityID=[{}], writing new doc [{}]", document.entityId, document.docId);
                writeDocument(document, DocWriteRequest.OpType.CREATE, request.getRefreshPolicy(), delegate);
            } else if (matchingDocuments.size() == 1) {
                final SamlServiceProviderDocument existingDoc = Iterables.get(matchingDocuments, 0).getDocument();
                assert existingDoc.docId != null : "Loaded document with no doc id";
                assert existingDoc.entityId.equals(document.entityId) : "Loaded document with non-matching entity-id";
                document.setDocId(existingDoc.docId);
                document.setCreated(existingDoc.created);
                logger.trace("Found existing ServiceProvider for EntityID=[{}], writing to doc [{}]", document.entityId, document.docId);
                writeDocument(document, DocWriteRequest.OpType.INDEX, request.getRefreshPolicy(), delegate);
            } else {
                logger.warn(
                    "Found multiple existing service providers in [{}] with entity id [{}] - [{}]",
                    index,
                    document.entityId,
                    matchingDocuments.stream().map(d -> d.getDocument().docId).collect(Collectors.joining(","))
                );
                delegate.onFailure(
                    new IllegalStateException("Multiple service providers already exist with entity id [" + document.entityId + "]")
                );
            }
        }));
    }

    private void writeDocument(
        SamlServiceProviderDocument document,
        DocWriteRequest.OpType opType,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<PutSamlServiceProviderResponse> listener
    ) {

        final Instant now = clock.instant();
        if (document.created == null || opType == DocWriteRequest.OpType.CREATE) {
            document.created = now;
        }
        document.lastModified = now;
        final ValidationException validationException = document.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        logger.debug("[{}] service provider [{}] in document [{}] of [{}]", opType, document.entityId, document.docId, index);
        index.writeDocument(
            document,
            opType,
            refreshPolicy,
            listener.delegateFailureAndWrap(
                (l, response) -> l.onResponse(
                    new PutSamlServiceProviderResponse(
                        response.getId(),
                        response.getResult() == DocWriteResponse.Result.CREATED,
                        response.getSeqNo(),
                        response.getPrimaryTerm(),
                        document.entityId,
                        document.enabled
                    )
                )
            )
        );
    }

    private static String deriveDocumentId(SamlServiceProviderDocument document) {
        final byte[] sha256 = MessageDigests.sha256().digest(document.entityId.getBytes(StandardCharsets.UTF_8));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(sha256);
    }

}
