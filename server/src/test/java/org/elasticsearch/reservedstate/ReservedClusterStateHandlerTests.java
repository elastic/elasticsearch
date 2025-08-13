/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.indices.settings.InternalOrPrivateSettingsPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class ReservedClusterStateHandlerTests extends ESTestCase {
    public void testValidation() {
        ReservedClusterStateHandler<ValidRequest> handler = new ReservedClusterStateHandler<>() {
            @Override
            public String name() {
                return "handler";
            }

            @Override
            public TransformState transform(ValidRequest source, TransformState prevState) throws Exception {
                return prevState;
            }

            @Override
            public ValidRequest fromXContent(XContentParser parser) throws IOException {
                return new ValidRequest();
            }
        };

        handler.validate(new ValidRequest());
        assertThat(
            expectThrows(IllegalStateException.class, () -> handler.validate(new InvalidRequest())).getMessage(),
            is("Validation error")
        );
    }

    static class ValidRequest extends MasterNodeRequest<InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request> {
        ValidRequest() {
            super(TEST_REQUEST_TIMEOUT);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    static class InvalidRequest extends ValidRequest {
        @Override
        public ActionRequestValidationException validate() {
            return new ActionRequestValidationException();
        }
    }
}
