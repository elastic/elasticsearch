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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.anyString;

public class QueryExplanationTests extends AbstractStreamableXContentTestCase<QueryExplanation> {

    static QueryExplanation createRandomQueryExplanation(boolean isValid) {
        String index = "index_" + randomInt(1000);
        int shard = randomInt(100);
        Boolean valid = isValid;
        String errorField = null;
        if (!valid) {
            errorField = randomAlphaOfLength(randomIntBetween(10, 100));
        }
        String explanation = randomAlphaOfLength(randomIntBetween(10, 100));
        return new QueryExplanation(index, shard, valid, explanation, errorField);
    }

    static QueryExplanation createRandomQueryExplanation() {
        return createRandomQueryExplanation(randomBoolean());
    }

    @Override
    protected QueryExplanation doParseInstance(XContentParser parser) throws IOException {
        return QueryExplanation.fromXContent(parser);
    }

    @Override
    protected QueryExplanation createBlankInstance() {
        return new QueryExplanation();
    }

    @Override
    protected QueryExplanation createTestInstance() {
        return createRandomQueryExplanation();
    }

    public void testCompatabilityOfStreamInputApi_V_6_4_0() throws IOException {
        QueryExplanation queryExplanation = createRandomQueryExplanation(true);
        StreamInput in = mock(StreamInput.class);
        when(in.getVersion()).thenReturn(Version.V_6_4_0);
        queryExplanation.readFrom(in);
        verify(in, times(3)).readOptionalString();
    }

    public void testCompatabilityOfStreamInputApi_V_5_4_0() throws IOException {
        QueryExplanation queryExplanation = createRandomQueryExplanation(true);
        StreamInput in = mock(StreamInput.class);
        when(in.getVersion()).thenReturn(Version.V_5_4_0);
        queryExplanation.readFrom(in);
        verify(in).readString();
        verify(in).readInt();
    }

    public void testCompatabilityOfStreamOutputApi_V_6_4_0() throws IOException {
        QueryExplanation queryExplanation = createRandomQueryExplanation(true);
        StreamOutput out = mock(StreamOutput.class);
        when(out.getVersion()).thenReturn(Version.V_6_4_0);
        queryExplanation.writeTo(out);
        verify(out, times(3)).writeOptionalString(anyString());
    }

    public void testCompatabilityOfStreamOutputApi_V_5_4_0() throws IOException {
        QueryExplanation queryExplanation = createRandomQueryExplanation(true);
        StreamOutput out = mock(StreamOutput.class);
        when(out.getVersion()).thenReturn(Version.V_5_4_0);
        queryExplanation.writeTo(out);
        verify(out).writeString(anyString());
    }
}
