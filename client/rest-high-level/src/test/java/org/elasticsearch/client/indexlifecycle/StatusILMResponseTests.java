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

package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.indexlifecycle.OperationMode;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.EnumSet;
import java.util.stream.Collectors;

public class StatusILMResponseTests extends ESTestCase {

    public void testClientServerStatuses() {
        assertEquals(
            EnumSet.allOf(StatusILMResponse.OperationMode.class).stream().map(Enum::name).collect(Collectors.toSet()),
            EnumSet.allOf(OperationMode.class).stream().map(Enum::name).collect(Collectors.toSet()));
    }

    public void testFromName() {
        EnumSet.allOf(StatusILMResponse.OperationMode.class)
            .forEach(e -> assertEquals(StatusILMResponse.OperationMode.fromString(e.name()), e));
    }

    public void testInvalidStatus() {
        String invalidName = randomAlphaOfLength(10);
        Exception e = expectThrows(IllegalArgumentException.class, () -> StatusILMResponse.OperationMode.fromString(invalidName));
        assertThat(e.getMessage(), CoreMatchers.containsString(invalidName + " is not a valid operation_mode"));
    }

    public void testValidStatuses() {
        EnumSet.allOf(StatusILMResponse.OperationMode.class)
            .forEach(e -> assertEquals(new StatusILMResponse(e.name()).getOperationMode(), e));
    }

    public void testXContent() throws IOException {
        XContentType xContentType = XContentType.JSON;
        String mode = randomFrom(EnumSet.allOf(StatusILMResponse.OperationMode.class)
            .stream().map(Enum::name).collect(Collectors.toList()));
        XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{\"operation_mode\" : \"" + mode + "\"}");
        assertEquals(StatusILMResponse.fromXContent(parser).getOperationMode(), StatusILMResponse.OperationMode.fromString(mode));
    }
}
