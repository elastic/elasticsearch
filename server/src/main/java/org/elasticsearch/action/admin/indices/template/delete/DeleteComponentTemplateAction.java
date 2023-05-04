/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.dlm.AuthorizeDlmConfigurationRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteComponentTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteComponentTemplateAction INSTANCE = new DeleteComponentTemplateAction();
    public static final String NAME = "cluster:admin/component_template/delete";

    private DeleteComponentTemplateAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> implements AuthorizeDlmConfigurationRequest {

        private final String[] names;

        public Request(StreamInput in) throws IOException {
            super(in);
            names = in.readStringArray();
        }

        /**
         * Constructs a new delete index request for the specified name.
         */
        public Request(String... names) {
            this.names = Objects.requireNonNull(names, "component templates to delete must not be null");
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Arrays.stream(names).anyMatch(Strings::hasLength) == false) {
                validationException = addValidationError("no component template names specified", validationException);
            }
            return validationException;
        }

        /**
         * The index template names to delete.
         */
        public String[] names() {
            return names;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
        }

        @Override
        public String[] dataStreamPatterns(ClusterState state) {
            final Predicate<String> predicate;
            if (names.length > 1) {
                predicate = name -> Arrays.asList(names).contains(name);
            } else {
                predicate = name -> Regex.simpleMatch(names[0], name);
            }
            final boolean anyTemplatesWithLifecycles = state.metadata()
                .componentTemplates()
                .entrySet()
                .stream()
                .anyMatch(it -> predicate.test(it.getKey()) && it.getValue().hasDataLifecycle());
            // We don't need to check any composable templates; it's impossible to delete a component template that's still in use
            return anyTemplatesWithLifecycles ? new String[0] : null;
        }
    }
}
