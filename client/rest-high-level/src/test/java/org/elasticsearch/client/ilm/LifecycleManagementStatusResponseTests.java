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

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.EnumSet;
import java.util.stream.Collectors;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class LifecycleManagementStatusResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            LifecycleManagementStatusResponseTests::createTestInstance,
            LifecycleManagementStatusResponseTests::toXContent,
            LifecycleManagementStatusResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertToXContentEquivalence(false)
            .test();
    }

    private static XContentBuilder toXContent(LifecycleManagementStatusResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("operation_mode", response.getOperationMode());
        builder.endObject();
        return builder;
    }

    private static LifecycleManagementStatusResponse createTestInstance() {
        return new LifecycleManagementStatusResponse(randomFrom(OperationMode.values()).name());
    }

    public void testAllValidStatuses() {
        EnumSet.allOf(OperationMode.class)
            .forEach(e -> assertEquals(new LifecycleManagementStatusResponse(e.name()).getOperationMode(), e));
    }

    public void testXContent() throws IOException {
        XContentType xContentType = XContentType.JSON;
        String mode = randomFrom(EnumSet.allOf(OperationMode.class)
            .stream().map(Enum::name).collect(Collectors.toList()));
        XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{\"operation_mode\" : \"" + mode + "\"}");
        assertEquals(LifecycleManagementStatusResponse.fromXContent(parser).getOperationMode(), OperationMode.fromString(mode));
    }

    public void testXContentInvalid() throws IOException {
        XContentType xContentType = XContentType.JSON;
        String mode = randomAlphaOfLength(10);
        XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{\"operation_mode\" : \"" + mode + "\"}");
        Exception e = expectThrows(IllegalArgumentException.class, () -> LifecycleManagementStatusResponse.fromXContent(parser));
        assertThat(e.getMessage(), CoreMatchers.containsString("failed to parse field [operation_mode]"));
    }
}
