/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderDocument;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutSamlServiceProviderRequest extends ActionRequest {
    public static final WriteRequest.RefreshPolicy DEFAULT_REFRESH_POLICY = WriteRequest.RefreshPolicy.NONE;

    private final SamlServiceProviderDocument document;
    private final WriteRequest.RefreshPolicy refreshPolicy;

    public static PutSamlServiceProviderRequest fromXContent(
        String entityId,
        WriteRequest.RefreshPolicy refreshPolicy,
        XContentParser parser
    ) throws IOException {
        final SamlServiceProviderDocument document = SamlServiceProviderDocument.fromXContent(null, parser);
        if (document.entityId == null) {
            document.setEntityId(entityId);
        } else if (entityId != null) {
            if (entityId.equals(document.entityId) == false) {
                throw new ElasticsearchParseException(
                    "Entity id [{}] inside request body and entity id [{}] from parameter do not match",
                    document.entityId,
                    entityId
                );
            }
        }
        if (document.created != null) {
            throw new ElasticsearchParseException(
                "Field [{}] may not be specified in a request",
                SamlServiceProviderDocument.Fields.CREATED_DATE
            );
        }
        if (document.lastModified != null) {
            throw new ElasticsearchParseException(
                "Field [{}] may not be specified in a request",
                SamlServiceProviderDocument.Fields.LAST_MODIFIED
            );
        }
        document.setCreatedMillis(System.currentTimeMillis());
        document.setLastModifiedMillis(System.currentTimeMillis());
        return new PutSamlServiceProviderRequest(document, refreshPolicy);
    }

    public PutSamlServiceProviderRequest(SamlServiceProviderDocument document, WriteRequest.RefreshPolicy refreshPolicy) {
        this.document = document;
        this.refreshPolicy = refreshPolicy;
    }

    public PutSamlServiceProviderRequest(StreamInput in) throws IOException {
        this.document = new SamlServiceProviderDocument(in);
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        document.writeTo(out);
        refreshPolicy.writeTo(out);
    }

    public SamlServiceProviderDocument getDocument() {
        return document;
    }

    @Override
    public ActionRequestValidationException validate() {
        final ValidationException docException = document.validate();
        ActionRequestValidationException validationException = null;
        if (docException != null) {
            validationException = new ActionRequestValidationException();
            validationException.addValidationErrors(docException.validationErrors());
        }

        if (Strings.hasText(document.acs)) { // if this is blank the document validation will fail
            try {
                final URL url = new URL(document.acs);
                if (url.getProtocol().equals("https") == false) {
                    validationException = addValidationError(
                        "[" + SamlServiceProviderDocument.Fields.ACS + "] must use the [https] protocol",
                        validationException
                    );
                }
            } catch (MalformedURLException e) {
                String error = "[" + SamlServiceProviderDocument.Fields.ACS + "] must be a valid URL";
                if (e.getMessage() != null) {
                    error += " - " + e.getMessage();
                }
                validationException = addValidationError(error, validationException);
            }
        }

        if (document.certificates.identityProviderSigning.isEmpty() == false) {
            validationException = addValidationError(
                "[" + SamlServiceProviderDocument.Fields.Certificates.IDP_SIGNING + "] certificates may not be specified",
                validationException
            );
        }

        if (document.certificates.identityProviderMetadataSigning.isEmpty() == false) {
            validationException = addValidationError(
                "[" + SamlServiceProviderDocument.Fields.Certificates.IDP_METADATA + "] certificates may not be specified",
                validationException
            );
        }

        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PutSamlServiceProviderRequest that = (PutSamlServiceProviderRequest) o;
        return Objects.equals(document, that.document);
    }

    @Override
    public int hashCode() {
        return Objects.hash(document);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + document + "}";
    }
}
