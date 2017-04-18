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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.validate.query.ShardValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class ShardValidateQueryRequestTests extends ESTestCase {
    protected NamedWriteableRegistry namedWriteableRegistry;

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
            ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest("indices");
            validateQueryRequest.query(QueryBuilders.termQuery("field", "value"));
            validateQueryRequest.rewrite(true);
            validateQueryRequest.explain(false);
            validateQueryRequest.types("type1", "type2");
            ShardValidateQueryRequest request = new ShardValidateQueryRequest(new ShardId("index", "foobar", 1),
                new AliasFilter(QueryBuilders.termQuery("filter_field", "value"), new String[] {"alias0", "alias1"}), validateQueryRequest);
            request.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                ShardValidateQueryRequest readRequest = new ShardValidateQueryRequest();
                readRequest.readFrom(in);
                assertEquals(request.filteringAliases(), readRequest.filteringAliases());
                assertArrayEquals(request.types(), readRequest.types());
                assertEquals(request.explain(), readRequest.explain());
                assertEquals(request.query(), readRequest.query());
                assertEquals(request.rewrite(), readRequest.rewrite());
                assertEquals(request.shardId(), readRequest.shardId());
            }
        }
    }

    // BWC test for changes from #20916
    public void testSerialize50Request() throws IOException {
        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest("indices");
        validateQueryRequest.query(QueryBuilders.termQuery("field", "value"));
        validateQueryRequest.rewrite(true);
        validateQueryRequest.explain(false);
        validateQueryRequest.types("type1", "type2");
        ShardValidateQueryRequest request = new ShardValidateQueryRequest(new ShardId("index", "foobar", 1),
            new AliasFilter(QueryBuilders.termQuery("filter_field", "value"), new String[] {"alias0", "alias1"}), validateQueryRequest);
        BytesArray requestBytes = new BytesArray(Base64.getDecoder()
            // this is a base64 encoded request generated with the same input
            .decode("AAVpbmRleAZmb29iYXIBAQdpbmRpY2VzBAR0ZXJtP4AAAAAFZmllbGQVBXZhbHVlAgV0eXBlMQV0eXBlMgIGYWxpYXMwBmFsaWFzMQABAA"));
        try (StreamInput in = new NamedWriteableAwareStreamInput(requestBytes.streamInput(), namedWriteableRegistry)) {
            in.setVersion(Version.V_5_0_0);
            ShardValidateQueryRequest readRequest = new ShardValidateQueryRequest();
            readRequest.readFrom(in);
            assertEquals(0, in.available());
            assertArrayEquals(request.filteringAliases().getAliases(), readRequest.filteringAliases().getAliases());
            expectThrows(IllegalStateException.class, () -> readRequest.filteringAliases().getQueryBuilder());
            assertArrayEquals(request.types(), readRequest.types());
            assertEquals(request.explain(), readRequest.explain());
            assertEquals(request.query(), readRequest.query());
            assertEquals(request.rewrite(), readRequest.rewrite());
            assertEquals(request.shardId(), readRequest.shardId());
            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.V_5_0_0);
            readRequest.writeTo(output);
            assertEquals(output.bytes().toBytesRef(), requestBytes.toBytesRef());
        }
    }
}
