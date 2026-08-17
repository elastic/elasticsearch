/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PutManagedServiceAccountRequestTests extends ESTestCase {

    public void testParseRequestWithRolesAndEnabled() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "roles": ["role-a", "role-b"],
              "enabled": false
            }
            """)) {
            final PutManagedServiceAccountRequest request = PutManagedServiceAccountRequest.parse("my-team", "worker", parser);
            assertThat(request.getNamespace(), equalTo("my-team"));
            assertThat(request.getServiceName(), equalTo("worker"));
            assertThat(request.getRoles(), equalTo(List.of("role-a", "role-b")));
            assertThat(request.getRunAsFromRoles(), equalTo(List.of()));
            assertThat(request.isEnabled(), is(false));
        }
    }

    public void testParseRequestWithRunAsFromRoles() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "roles": ["role-a"],
              "run_as_from_roles": ["kibana_system", "elastic/kibana"]
            }
            """)) {
            final PutManagedServiceAccountRequest request = PutManagedServiceAccountRequest.parse("my-team", "worker", parser);
            assertThat(request.getRoles(), equalTo(List.of("role-a")));
            assertThat(request.getRunAsFromRoles(), equalTo(List.of("kibana_system", "elastic/kibana")));
            assertThat(request.isEnabled(), is(true));
        }
    }

    public void testValidateRejectsWildcardRunAsFromRoles() {
        final PutManagedServiceAccountRequest request = new PutManagedServiceAccountRequest(
            "my-team",
            "worker",
            List.of("role-a"),
            List.of("*"),
            true
        );
        assertThat(request.validate().validationErrors().toString(), containsString("wildcard"));
    }

    public void testValidateAcceptsServiceAccountDescriptorName() {
        final PutManagedServiceAccountRequest request = new PutManagedServiceAccountRequest(
            "my-team",
            "worker",
            List.of("role-a"),
            List.of("elastic/kibana"),
            true
        );
        assertThat(request.validate(), equalTo(null));
    }

    public void testParseRequestWithRolesOnlyDefaultsEnabledTrue() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "roles": ["role-a"]
            }
            """)) {
            final PutManagedServiceAccountRequest request = PutManagedServiceAccountRequest.parse("my-team", "worker", parser);
            assertThat(request.getRoles(), equalTo(List.of("role-a")));
            assertThat(request.isEnabled(), is(true));
        }
    }
}
