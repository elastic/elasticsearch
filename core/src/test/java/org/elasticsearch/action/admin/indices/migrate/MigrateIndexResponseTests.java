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

package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class MigrateIndexResponseTests extends ESTestCase {
    public void testRoundTripThroughTransport() throws IOException {
        MigrateIndexResponse original = randomResponse();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                MigrateIndexResponse read = new MigrateIndexResponse();
                read.readFrom(in);
                assertEquals(original, read);
            }
        }
    }

    public void testToStringIsSane() {
        String string = randomResponse().toString();
        assertThat(string, containsString("\"acknowledged\":"));
        assertThat(string, containsString("\"noop\":"));
    }

    private MigrateIndexResponse randomResponse() {
        return new MigrateIndexResponse(randomBoolean(), randomBoolean());
    }
}
