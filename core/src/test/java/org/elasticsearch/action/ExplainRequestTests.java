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
package org.elasticsearch.action;

import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExplainRequestTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    public void setUp() throws Exception {
        super.setUp();
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testSerialize() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            ExplainRequest request = new ExplainRequest("index", "type", "id");
            request.fetchSourceContext(new FetchSourceContext(true, new String[]{"field1.*"}, new String[] {"field2.*"}));
            request.filteringAlias(new AliasFilter(QueryBuilders.termQuery("filter_field", "value"), new String[] {"alias0", "alias1"}));
            request.preference("the_preference");
            request.query(QueryBuilders.termQuery("field", "value"));
            request.storedFields(new String[] {"field1", "field2"});
            request.routing("some_routing");
            request.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                ExplainRequest readRequest = new ExplainRequest();
                readRequest.readFrom(in);
                assertEquals(request.filteringAlias(), readRequest.filteringAlias());
                assertArrayEquals(request.storedFields(), readRequest.storedFields());
                assertEquals(request.preference(), readRequest.preference());
                assertEquals(request.query(), readRequest.query());
                assertEquals(request.routing(), readRequest.routing());
                assertEquals(request.fetchSourceContext(), readRequest.fetchSourceContext());
            }
        }
    }
}
