/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class TransportValidateQueryActionTests extends ESSingleNodeTestCase {

    /*
     * This test covers a fallthrough bug that we had, where if the index we were validating against did not exist, we would invoke the
     * failure listener, and then fallthrough and invoke the success listener too. This would cause problems when the listener was
     * ultimately wrapping sending a response on the channel, as it could lead to us sending both a failure or success responses, and having
     * them garbled together, or trying to write one after the channel had closed, etc.
     */
    public void testListenerOnlyInvokedOnceWhenIndexDoesNotExist() {
        final AtomicBoolean invoked = new AtomicBoolean();
        final ActionListener<ValidateQueryResponse> listener = new ActionListener<>() {

            @Override
            public void onResponse(final ValidateQueryResponse validateQueryResponse) {
                fail("onResponse should not be invoked in this failure case");
            }

            @Override
            public void onFailure(final Exception e) {
                if (invoked.compareAndSet(false, true) == false) {
                    fail("onFailure invoked more than once");
                }
            }

        };
        client().admin().indices().validateQuery(new ValidateQueryRequest("non-existent-index"), listener);
        assertThat(invoked.get(), equalTo(true)); // ensure that onFailure was invoked
    }

}
