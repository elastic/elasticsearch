/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.role;

import junit.framework.TestCase;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestGetAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.security.action.role.TransportPutRoleAction;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class RestPutRoleActionTests extends RestActionTestCase {

    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));
    final List<String> jsonContentType = Collections.singletonList(XContentType.JSON.toParsedMediaType().responseContentTypeHeader());

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestPutRoleAction(Settings.EMPTY, new XPackLicenseState(Settings.EMPTY, ()->0)));
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(PutRoleRequest.class));
            return Mockito.mock(PutRoleResponse.class);
        });
    }


    public void testCreationOfRoleWithMalformedQueryJsonFails() {

        PutRoleRequest request = new PutRoleRequest();
        request.name("test");
        String[] malformedQueryJson = new String[]{"{ \"match_all\": { \"unknown_field\": \"\" } }",
            "{ malformed JSON }",
            "{ \"unknown\": {\"\"} }",
            "{}"};
        BytesReference query = new BytesArray(randomFrom(malformedQueryJson));
        request.addIndex(new String[]{"idx1"}, new String[]{"read"}, null, null, query, randomBoolean());

        FakeRestRequest.Builder deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Content-Type", jsonContentType, "Accept", jsonContentType))
            .withPath("_security/role/rolename")
            .withMethod(RestRequest.Method.POST)
            .withContent(query, null);
        dispatchRequest(deprecatedRequest.build());

//        Throwable t = throwableRef.get();
//        assertThat(t, instanceOf(ElasticsearchParseException.class));
//        assertThat(t.getMessage(), containsString("failed to parse field 'query' for indices [" +
//            Strings.arrayToCommaDelimitedString(new String[]{"idx1"}) +
//            "] at index privilege [0] of role descriptor"));
    }
//    public void testValidation() {
//
//        FakeRestRequest.Builder deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
//            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
//        ).withPath("/some_index/some_type/some_id");
//        dispatchRequest(deprecatedRequest.withMethod(method).build());
//        assertWarnings(RestGetAction.TYPES_DEPRECATION_MESSAGE);
//    }

}
