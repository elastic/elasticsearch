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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class UnknownNamedObjectExceptionTests extends ESTestCase {
    public void testRoundTrip() throws IOException {
        XContentLocation location = new XContentLocation(between(1, 1000), between(1, 1000));
        UnknownNamedObjectException created = new UnknownNamedObjectException(location, UnknownNamedObjectExceptionTests.class,
                randomAlphaOfLength(5));
        UnknownNamedObjectException roundTripped;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            created.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                roundTripped = new UnknownNamedObjectException(in);
            }
        }
        assertEquals(created.getMessage(), roundTripped.getMessage());
        assertEquals(created.getLineNumber(), roundTripped.getLineNumber());
        assertEquals(created.getColumnNumber(), roundTripped.getColumnNumber());
        assertEquals(created.getCategoryClass(), roundTripped.getCategoryClass());
        assertEquals(created.getName(), roundTripped.getName());
    }

    public void testStatusCode() {
        XContentLocation location = new XContentLocation(between(1, 1000), between(1, 1000));
        UnknownNamedObjectException e = new UnknownNamedObjectException(location, UnknownNamedObjectExceptionTests.class,
                randomAlphaOfLength(5));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }
}
