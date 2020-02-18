/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderDocument;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.stream.Collectors;

public class TransportPutSamlServiceProviderAction
    extends HandledTransportAction<PutSamlServiceProviderRequest, PutSamlServiceProviderResponse> {

    private final Logger logger = LogManager.getLogger();
    private final SamlServiceProviderIndex index;
    private final Clock clock;

    @Inject
    public TransportPutSamlServiceProviderAction(TransportService transportService, ActionFilters actionFilters,
                                                 SamlServiceProviderIndex index) {
        this(transportService, actionFilters, index, Clock.systemUTC());
    }

    TransportPutSamlServiceProviderAction(TransportService transportService, ActionFilters actionFilters,
                                                 SamlServiceProviderIndex index, Clock clock) {
        super(PutSamlServiceProviderAction.NAME, transportService, actionFilters, PutSamlServiceProviderRequest::new);
        this.index = index;
        this.clock = clock;
    }

    @Override
    protected void doExecute(Task task, final PutSamlServiceProviderRequest request,
                             final ActionListener<PutSamlServiceProviderResponse> listener) {
        final SamlServiceProviderDocument document = request.getDocument();
        if (document.docId != null) {
            listener.onFailure(new IllegalArgumentException("request document must not have an id [" + document.docId + "]"));
            return;
        }
        index.findByEntityId(document.entityId, ActionListener.wrap(matchingDocuments -> {
            if (matchingDocuments.isEmpty()) {
                // derive a document id from the entity id so that don't accidentally create duplicate entities due to a race condition
                document.docId = deriveDocumentId(document);
                // force a create in case there are concurrent requests
                writeDocument(document, DocWriteRequest.OpType.CREATE, listener);
            } else if (matchingDocuments.size() == 1) {
                SamlServiceProviderDocument existingDoc = Iterables.get(matchingDocuments, 0);
                assert existingDoc.docId != null : "Loaded document with no doc id";
                assert existingDoc.entityId.equals(document.entityId) : "Loaded document with non-matching entity-id";
                document.setDocId(existingDoc.docId);
                document.setCreated(existingDoc.created);
                writeDocument(document, DocWriteRequest.OpType.INDEX, listener);
            } else {
                logger.warn("Found multiple existing service providers in [{}] with entity id [{}] - [{}]",
                    index, document.entityId, matchingDocuments.stream().map(d -> d.docId).collect(Collectors.joining(",")));
                listener.onFailure(new IllegalStateException(
                    "Multiple service providers already exist with entity id [" + document.entityId + "]"));
            }
        }, listener::onFailure));
    }

    private void writeDocument(SamlServiceProviderDocument document, DocWriteRequest.OpType opType,
                               ActionListener<PutSamlServiceProviderResponse> listener) {
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
        index.writeDocument(document, opType, ActionListener.wrap(
            response -> listener.onResponse(new PutSamlServiceProviderResponse(
                response.getId(),
                response.getResult() == DocWriteResponse.Result.CREATED,
                response.getSeqNo(),
                response.getPrimaryTerm(),
                document.entityId,
                document.enabled)),
            listener::onFailure
        ));
    }

    private String deriveDocumentId(SamlServiceProviderDocument document) {
        final byte[] sha256 = MessageDigests.sha256().digest(document.entityId.getBytes(StandardCharsets.UTF_8));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(sha256);
    }

}
