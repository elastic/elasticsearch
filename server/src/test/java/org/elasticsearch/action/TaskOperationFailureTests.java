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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class TaskOperationFailureTests extends AbstractWireSerializingTestCase<TaskOperationFailure> {

    @Override
    protected TaskOperationFailure createTestInstance() {
        return new TaskOperationFailure(randomAlphaOfLength(5), randomNonNegativeLong(), new IllegalStateException("message"));
    }

    @Override
    protected Writeable.Reader<TaskOperationFailure> instanceReader() {
        return TaskOperationFailure::new;
    }

    public void testXContent() throws IOException {
        TaskOperationFailure failure = createTestInstance();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference serialized = XContentHelper.toXContent(failure, xContentType, false);
        XContentParser parser = createParser(XContentFactory.xContent(xContentType), serialized);
        TaskOperationFailure parsed = TaskOperationFailure.fromXContent(parser);
        BytesReference serializedAgain = XContentHelper.toXContent(parsed, xContentType, false);

        TaskOperationFailure expected = new TaskOperationFailure(failure.getNodeId(), failure.getTaskId(),
                // XContent loses the original exception and wraps it as a message
                new ElasticsearchException("Elasticsearch exception [type=illegal_state_exception, reason=message]"));
        BytesReference xContentExpected = XContentHelper.toXContent(expected, xContentType, false);
        assertToXContentEquivalent(xContentExpected, serializedAgain, xContentType);
    }
}
