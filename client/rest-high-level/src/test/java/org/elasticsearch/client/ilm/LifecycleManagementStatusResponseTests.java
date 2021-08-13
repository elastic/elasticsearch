/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
