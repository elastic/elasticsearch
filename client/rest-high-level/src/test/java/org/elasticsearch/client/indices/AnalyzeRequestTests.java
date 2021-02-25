/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.client.AbstractRequestTestCase;

public abstract class AnalyzeRequestTests extends AbstractRequestTestCase<AnalyzeRequest, AnalyzeAction.Request> {

    @Override
    protected void assertInstances(AnalyzeAction.Request serverInstance, AnalyzeRequest clientTestInstance) {
        assertEquals(serverInstance.index(), clientTestInstance.index());
        assertArrayEquals(serverInstance.text(), clientTestInstance.text());
        assertEquals(serverInstance.analyzer(), clientTestInstance.analyzer());
        assertEquals(serverInstance.normalizer(), clientTestInstance.normalizer());
        assertEquals(serverInstance.charFilters().size(), clientTestInstance.charFilters().size());
        for (int i = 0; i < serverInstance.charFilters().size(); i++) {
            assertEquals(serverInstance.charFilters().get(i).name, clientTestInstance.charFilters().get(i).name);
            assertEquals(serverInstance.charFilters().get(i).definition, clientTestInstance.charFilters().get(i).definition);
        }
        assertEquals(serverInstance.tokenFilters().size(), clientTestInstance.tokenFilters().size());
        for (int i = 0; i < serverInstance.tokenFilters().size(); i++) {
            assertEquals(serverInstance.tokenFilters().get(i).name, clientTestInstance.tokenFilters().get(i).name);
            assertEquals(serverInstance.tokenFilters().get(i).definition, clientTestInstance.tokenFilters().get(i).definition);
        }
        if (serverInstance.tokenizer() != null) {
            assertEquals(serverInstance.tokenizer().name, clientTestInstance.tokenizer().name);
            assertEquals(serverInstance.tokenizer().definition, clientTestInstance.tokenizer().definition);
        }
        else {
            assertNull(clientTestInstance.tokenizer());
        }
        assertEquals(serverInstance.field(), clientTestInstance.field());
        assertEquals(serverInstance.explain(), clientTestInstance.explain());
        assertArrayEquals(serverInstance.attributes(), clientTestInstance.attributes());
    }
}
