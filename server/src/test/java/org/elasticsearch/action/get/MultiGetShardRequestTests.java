/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class MultiGetShardRequestTests extends ESTestCase {
    public void testSerialization() throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        if (randomBoolean()) {
            multiGetRequest.preference(randomAlphaOfLength(randomIntBetween(1, 10)));
        }
        if (randomBoolean()) {
            multiGetRequest.realtime(false);
        }
        if (randomBoolean()) {
            multiGetRequest.refresh(true);
        }
        MultiGetShardRequest multiGetShardRequest = new MultiGetShardRequest(multiGetRequest, "index", 0);
        int numItems = iterations(10, 30);
        for (int i = 0; i < numItems; i++) {
            MultiGetRequest.Item item = new MultiGetRequest.Item("alias-" + randomAlphaOfLength(randomIntBetween(1, 10)), "id-" + i);
            if (randomBoolean()) {
                int numFields = randomIntBetween(1, 5);
                String[] fields = new String[numFields];
                for (int j = 0; j < fields.length; j++) {
                    fields[j] = randomAlphaOfLength(randomIntBetween(1, 10));
                }
                item.storedFields(fields);
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

        StreamInput in = out.bytes().streamInput();
        in.setVersion(out.getVersion());
        MultiGetShardRequest multiGetShardRequest2 = new MultiGetShardRequest(in);
        assertThat(multiGetShardRequest2.index(), equalTo(multiGetShardRequest.index()));
        assertThat(multiGetShardRequest2.preference(), equalTo(multiGetShardRequest.preference()));
        assertThat(multiGetShardRequest2.realtime(), equalTo(multiGetShardRequest.realtime()));
        assertThat(multiGetShardRequest2.refresh(), equalTo(multiGetShardRequest.refresh()));
        assertThat(multiGetShardRequest2.items.size(), equalTo(multiGetShardRequest.items.size()));
        for (int i = 0; i < multiGetShardRequest2.items.size(); i++) {
            MultiGetRequest.Item item = multiGetShardRequest.items.get(i);
            MultiGetRequest.Item item2 = multiGetShardRequest2.items.get(i);
                assertThat(item2.index(), equalTo(item.index()));
            assertThat(item2.id(), equalTo(item.id()));
            assertThat(item2.storedFields(), equalTo(item.storedFields()));
            assertThat(item2.version(), equalTo(item.version()));
            assertThat(item2.versionType(), equalTo(item.versionType()));
            assertThat(item2.fetchSourceContext(), equalTo(item.fetchSourceContext()));
        }
        assertThat(multiGetShardRequest2.indices(), equalTo(multiGetShardRequest.indices()));
        assertThat(multiGetShardRequest2.indicesOptions(), equalTo(multiGetShardRequest.indicesOptions()));
    }
}
