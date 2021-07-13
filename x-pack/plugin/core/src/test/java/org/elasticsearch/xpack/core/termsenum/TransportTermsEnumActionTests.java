/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class TransportTermsEnumActionTests extends ESSingleNodeTestCase {

    /*
     * Copy of test that tripped up similarly broadcast ValidateQuery
     */
    public void testListenerOnlyInvokedOnceWhenIndexDoesNotExist() {
        final AtomicBoolean invoked = new AtomicBoolean();
        final ActionListener<TermsEnumResponse> listener = new ActionListener<>() {

            @Override
            public void onResponse(final TermsEnumResponse validateQueryResponse) {
                fail("onResponse should not be invoked in this failure case");
            }

            @Override
            public void onFailure(final Exception e) {
                if (invoked.compareAndSet(false, true) == false) {
                    fail("onFailure invoked more than once");
                }
            }

        };
        client().execute(TermsEnumAction.INSTANCE, new TermsEnumRequest("non-existent-index"),listener);
        assertThat(invoked.get(), equalTo(true)); // ensure that onFailure was invoked
    }

}
