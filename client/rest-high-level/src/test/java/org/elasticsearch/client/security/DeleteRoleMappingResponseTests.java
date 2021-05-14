/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class DeleteRoleMappingResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String json = "{ \"found\" : \"true\" }";
        final DeleteRoleMappingResponse response = DeleteRoleMappingResponse.fromXContent(XContentType.JSON.xContent().createParser(
                new NamedXContentRegistry(Collections.emptyList()), DeprecationHandler.IGNORE_DEPRECATIONS, json));
        final DeleteRoleMappingResponse expectedResponse = new DeleteRoleMappingResponse(true);
        assertThat(response, equalTo(expectedResponse));
    }

    public void testEqualsHashCode() {
        final boolean found = randomBoolean();
        final DeleteRoleMappingResponse deleteRoleMappingResponse = new DeleteRoleMappingResponse(found);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(deleteRoleMappingResponse, (original) -> {
            return new DeleteRoleMappingResponse(original.isFound());
        });

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(deleteRoleMappingResponse, (original) -> {
            return new DeleteRoleMappingResponse(original.isFound());
        }, DeleteRoleMappingResponseTests::mutateTestItem);

    }

    private static DeleteRoleMappingResponse mutateTestItem(DeleteRoleMappingResponse original) {
        return new DeleteRoleMappingResponse(original.isFound() == false);
    }
}
