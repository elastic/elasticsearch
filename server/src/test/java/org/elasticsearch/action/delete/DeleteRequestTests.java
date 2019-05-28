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
package org.elasticsearch.action.delete;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;

public class DeleteRequestTests extends ESTestCase {

    public void testValidation() {
        {
            final DeleteRequest request = new DeleteRequest("index4", "_doc", "0");
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, nullValue());
        }

        {
            final DeleteRequest request = new DeleteRequest("index4", randomBoolean() ? "" : null, randomBoolean() ? "" : null);
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, not(nullValue()));
            assertThat(validate.validationErrors(), hasItems("type is missing", "id is missing"));
        }
    }

    public void testSerializeWithFallbackCAS() throws Exception {
        long seqNo = randomNonNegativeLong();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        long version = randomNonNegativeLong();
        VersionType versionType = randomFrom(VersionType.values());
        DeleteRequest request = new DeleteRequest("test", "_doc", "1");
        request.setIfSeqNo(seqNo);
        request.setIfPrimaryTerm(primaryTerm);
        request.setFallbackCASUsingVersion(version, versionType);
        assertNull(request.validate());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Version channelVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.V_6_5_4);
            out.setVersion(channelVersion);
            request.writeTo(out);
            DeleteRequest fallbackRequest = new DeleteRequest();
            StreamInput in = out.bytes().streamInput();
            in.setVersion(channelVersion);
            fallbackRequest.readFrom(in);
            assertThat(fallbackRequest.version(), equalTo(version));
            assertThat(fallbackRequest.versionType(), equalTo(versionType));
            assertThat(fallbackRequest.ifSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
            assertThat(fallbackRequest.ifPrimaryTerm(), equalTo(SequenceNumbers.UNASSIGNED_PRIMARY_TERM));
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Version channelVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_6_0, Version.CURRENT);
            out.setVersion(channelVersion);
            request.writeTo(out);
            DeleteRequest fallbackRequest = new DeleteRequest();
            StreamInput in = out.bytes().streamInput();
            in.setVersion(channelVersion);
            fallbackRequest.readFrom(in);
            assertThat(fallbackRequest.version(), equalTo(Versions.MATCH_ANY));
            assertThat(fallbackRequest.versionType(), equalTo(VersionType.INTERNAL));
            assertThat(fallbackRequest.ifSeqNo(), equalTo(seqNo));
            assertThat(fallbackRequest.ifPrimaryTerm(), equalTo(primaryTerm));
        }
    }
}
