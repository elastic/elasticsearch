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
