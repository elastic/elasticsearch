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

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;

public class CloseIndexResponseTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final CloseIndexResponse response = randomResponse();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);

            final CloseIndexResponse deserializedResponse = new CloseIndexResponse();
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedResponse.readFrom(in);
            }
            assertCloseIndexResponse(deserializedResponse, response);
        }
    }

    public void testBwcSerialization() throws Exception {
        {
            final CloseIndexResponse response = randomResponse();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(randomVersionBetween(random(), Version.V_6_0_0, VersionUtils.getPreviousVersion(Version.V_7_2_0)));
                response.writeTo(out);

                final AcknowledgedResponse deserializedResponse = new AcknowledgedResponse();
                try (StreamInput in = out.bytes().streamInput()) {
                    deserializedResponse.readFrom(in);
                }
                assertThat(deserializedResponse.isAcknowledged(), equalTo(response.isAcknowledged()));
            }
        }
        {
            final AcknowledgedResponse response = new AcknowledgedResponse(randomBoolean());
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                response.writeTo(out);

                final CloseIndexResponse deserializedResponse = new CloseIndexResponse();
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(randomVersionBetween(random(), Version.V_6_0_0, VersionUtils.getPreviousVersion(Version.V_7_2_0)));
                    deserializedResponse.readFrom(in);
                }
                assertThat(deserializedResponse.isAcknowledged(), equalTo(response.isAcknowledged()));
            }
        }
    }

    private CloseIndexResponse randomResponse() {
        final boolean acknowledged = randomBoolean();
        final boolean shardsAcknowledged = acknowledged ? randomBoolean() : false;
        return new CloseIndexResponse(acknowledged, shardsAcknowledged);
    }

    private static void assertCloseIndexResponse(final CloseIndexResponse actual, final CloseIndexResponse expected) {
        assertThat(actual.isAcknowledged(), equalTo(expected.isAcknowledged()));
        assertThat(actual.isShardsAcknowledged(), equalTo(expected.isShardsAcknowledged()));
    }
}
