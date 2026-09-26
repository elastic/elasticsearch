/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/** Deletes a named credential by name. */
public final class DeleteNamedCredentialAction {

    public static final String NAME = "cluster:admin/xpack/security/named_credentials/delete";
    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>(NAME);

    private DeleteNamedCredentialAction() {/* no instances */}

    public static final class Request extends UntypedActionRequest {

        private final String credentialName;

        public Request(String credentialName) {
            this.credentialName = credentialName;
        }

        public String credentialName() {
            return credentialName;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (Strings.isNullOrEmpty(credentialName)) {
                return ValidateActions.addValidationError("credential name must not be empty", null);
            }
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
