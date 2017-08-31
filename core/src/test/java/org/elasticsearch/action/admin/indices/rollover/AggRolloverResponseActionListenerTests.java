/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;

public class AggRolloverResponseActionListenerTests extends ESTestCase {

    private ActionListener<RolloverResponse> listener;
    private TransportRolloverAction.AggRolloverResponseActionListener agg;

    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        listener = mock(ActionListener.class);
    }

    public void testNestedListenerNotNofified() {
        createAggListener(2);

        agg.onResponse(getSingleAliasResponse());

        verify(listener, never()).onResponse(anyObject());
        verify(listener, never()).onFailure(anyObject());
    }

    public void testAllAliasesSuccessful() {
        createAggListener(2);

        agg.onResponse(getSingleAliasResponse());
        agg.onResponse(getSingleAliasResponse());

        verify(listener, only()).onResponse(anyObject());
    }

    public void testOneFailedOneSuccessful() {
        createAggListener(2);

        agg.onFailure(getSingleAliasFailure("alias1"));
        agg.onResponse(getSingleAliasResponse());

        verify(listener, only()).onResponse(anyObject());
    }

    public void testOneSuccessfulOneFailed() {
        createAggListener(2);

        agg.onResponse(getSingleAliasResponse());
        agg.onFailure(getSingleAliasFailure("alias1"));

        verify(listener, only()).onResponse(anyObject());
    }

    public void testAllFailed() {
        createAggListener(2);

        agg.onFailure(getSingleAliasFailure("alias1"));
        agg.onFailure(getSingleAliasFailure("alias2"));

        verify(listener, only()).onResponse(anyObject());
    }

    public void testSingleFailed() {
        createAggListener(1);

        agg.onFailure(getSingleAliasFailure("alias1"));

        verify(listener, only()).onFailure(anyObject());
        verify(listener, never()).onResponse(anyObject());
    }

    public void testSingleSuccessful() {
        createAggListener(1);

        agg.onResponse(getSingleAliasResponse());

        verify(listener, only()).onResponse(anyObject());
    }

    private RolloverResponse.SingleAliasRolloverResponse getSingleAliasResponse() {
        return new RolloverResponse.SingleAliasRolloverResponse("alias", "oldIndex", "newIndex",
            Collections.emptySet(), false, false, false, false);
    }

    private Exception getSingleAliasFailure(String alias) {
        return new TransportRolloverAction.RolloverFailureException(alias, new Exception());
    }

    private void createAggListener(int aliasesCount) {
        agg = new TransportRolloverAction.AggRolloverResponseActionListener(aliasesCount, listener);
    }
}
