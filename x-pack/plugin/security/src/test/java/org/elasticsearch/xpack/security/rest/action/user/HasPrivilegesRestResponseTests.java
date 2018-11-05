/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.security.rest.action.user.RestHasPrivilegesAction.HasPrivilegesRestResponseBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class HasPrivilegesRestResponseTests extends ESTestCase {

    public void testBuildValidJsonResponse() throws Exception {
        final HasPrivilegesRestResponseBuilder response = new HasPrivilegesRestResponseBuilder("daredevil", mock(RestChannel.class));
        final HasPrivilegesResponse actionResponse = new HasPrivilegesResponse(false,
                Collections.singletonMap("manage", true),
                Arrays.asList(
                        new HasPrivilegesResponse.ResourcePrivileges("staff",
                                MapBuilder.<String, Boolean>newMapBuilder(new LinkedHashMap<>())
                                        .put("read", true).put("index", true).put("delete", false).put("manage", false).map()),
                        new HasPrivilegesResponse.ResourcePrivileges("customers",
                                MapBuilder.<String, Boolean>newMapBuilder(new LinkedHashMap<>())
                                        .put("read", true).put("index", true).put("delete", true).put("manage", false).map())
                ), Collections.emptyMap());
        final XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        final RestResponse rest = response.buildResponse(actionResponse, builder);

        assertThat(rest, instanceOf(BytesRestResponse.class));

        final String json = rest.content().utf8ToString();
        assertThat(json, equalTo("{" +
                "\"username\":\"daredevil\"," +
                "\"has_all_requested\":false," +
                "\"cluster\":{\"manage\":true}," +
                "\"index\":{" +
                "\"staff\":{\"read\":true,\"index\":true,\"delete\":false,\"manage\":false}," +
                "\"customers\":{\"read\":true,\"index\":true,\"delete\":true,\"manage\":false}" +
                "}," +
                "\"application\":{}" +
                "}"));
    }
}