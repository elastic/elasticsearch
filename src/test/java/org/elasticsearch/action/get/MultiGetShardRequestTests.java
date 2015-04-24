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

package org.elasticsearch.action.get;

import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class MultiGetShardRequestTests extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        if (randomBoolean()) {
            multiGetRequest.preference(randomAsciiOfLength(randomIntBetween(1, 10)));
        }
        if (randomBoolean()) {
            multiGetRequest.realtime(false);
        }
        if (randomBoolean()) {
            multiGetRequest.refresh(true);
        }
        multiGetRequest.ignoreErrorsOnGeneratedFields(randomBoolean());

        MultiGetShardRequest multiGetShardRequest = new MultiGetShardRequest(multiGetRequest, "index", 0);
        int numItems = iterations(10, 30);
        for (int i = 0; i < numItems; i++) {
            MultiGetRequest.Item item = new MultiGetRequest.Item("alias-" + randomAsciiOfLength(randomIntBetween(1, 10)), "type", "id-" + i);
            if (randomBoolean()) {
                int numFields = randomIntBetween(1, 5);
                String[] fields = new String[numFields];
                for (int j = 0; j < fields.length; j++) {
                    fields[j] = randomAsciiOfLength(randomIntBetween(1, 10));
                }
                item.fields(fields);
            }
            if (randomBoolean()) {
                item.version(randomIntBetween(1, Integer.MAX_VALUE));
                item.versionType(randomFrom(VersionType.values()));
            }
            if (randomBoolean()) {
                item.fetchSourceContext(new FetchSourceContext(randomBoolean()));
            }
            multiGetShardRequest.add(0, item);
        }

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(randomVersion(random()));
        multiGetShardRequest.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        in.setVersion(out.getVersion());
        MultiGetShardRequest multiGetShardRequest2 = new MultiGetShardRequest();
        multiGetShardRequest2.readFrom(in);

        assertThat(multiGetShardRequest2.index(), equalTo(multiGetShardRequest.index()));
        assertThat(multiGetShardRequest2.preference(), equalTo(multiGetShardRequest.preference()));
        assertThat(multiGetShardRequest2.realtime(), equalTo(multiGetShardRequest.realtime()));
        assertThat(multiGetShardRequest2.refresh(), equalTo(multiGetShardRequest.refresh()));
        assertThat(multiGetShardRequest2.ignoreErrorsOnGeneratedFields(), equalTo(multiGetShardRequest.ignoreErrorsOnGeneratedFields()));
        assertThat(multiGetShardRequest2.items.size(), equalTo(multiGetShardRequest.items.size()));
        for (int i = 0; i < multiGetShardRequest2.items.size(); i++) {
            MultiGetRequest.Item item = multiGetShardRequest.items.get(i);
            MultiGetRequest.Item item2 = multiGetShardRequest2.items.get(i);
                assertThat(item2.index(), equalTo(item.index()));
            assertThat(item2.type(), equalTo(item.type()));
            assertThat(item2.id(), equalTo(item.id()));
            assertThat(item2.fields(), equalTo(item.fields()));
            assertThat(item2.version(), equalTo(item.version()));
            assertThat(item2.versionType(), equalTo(item.versionType()));
            assertThat(item2.fetchSourceContext(), equalTo(item.fetchSourceContext()));
        }
        assertThat(multiGetShardRequest2.indices(), equalTo(multiGetShardRequest.indices()));
        assertThat(multiGetShardRequest2.indicesOptions(), equalTo(multiGetShardRequest.indicesOptions()));
    }
}
