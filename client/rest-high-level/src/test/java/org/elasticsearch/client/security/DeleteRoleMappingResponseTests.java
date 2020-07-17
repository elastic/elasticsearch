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
        return new DeleteRoleMappingResponse(!original.isFound());
    }
}
