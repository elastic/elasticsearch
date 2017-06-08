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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class CreateIndexResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        CreateIndexResponse response = new CreateIndexResponse(true, true, "foo");

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                CreateIndexResponse serialized = new CreateIndexResponse();
                serialized.readFrom(in);
                assertEquals(response.isShardsAcked(), serialized.isShardsAcked());
                assertEquals(response.isAcknowledged(), serialized.isAcknowledged());
                assertEquals(response.index(), serialized.index());
            }
        }
    }

    public void testSerializationWithOldVersion() throws IOException {
        Version oldVersion = Version.V_5_4_0;
        CreateIndexResponse response = new CreateIndexResponse(true, true, "foo");

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(oldVersion);
            response.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(oldVersion);
                CreateIndexResponse serialized = new CreateIndexResponse();
                serialized.readFrom(in);
                assertEquals(response.isShardsAcked(), serialized.isShardsAcked());
                assertEquals(response.isAcknowledged(), serialized.isAcknowledged());
                assertNull(serialized.index());
            }
        }
    }
}
