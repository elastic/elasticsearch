/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.instanceOf;

public class TransportValidateQueryActionTests extends ESSingleNodeTestCase {

    /*
     * This test covers a fallthrough bug that we had, where if the index we were validating against did not exist, we would invoke the
     * failure listener, and then fallthrough and invoke the success listener too. This would cause problems when the listener was
     * ultimately wrapping sending a response on the channel, as it could lead to us sending both a failure or success responses, and having
     * them garbled together, or trying to write one after the channel had closed, etc.
     */
    public void testListenerOnlyInvokedOnceWhenIndexDoesNotExist() {
        assertThat(
            safeAwaitFailure(
                ValidateQueryResponse.class,
                listener -> client().admin()
                    .indices()
                    .validateQuery(new ValidateQueryRequest("non-existent-index"), ActionListener.assertOnce(listener))
            ),
            instanceOf(IndexNotFoundException.class)
        );
    }

}
