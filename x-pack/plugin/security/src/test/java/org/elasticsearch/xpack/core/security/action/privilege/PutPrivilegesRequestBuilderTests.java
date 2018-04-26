/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.iterableWithSize;

public class PutPrivilegesRequestBuilderTests extends ESTestCase {

    public void testBuildRequestWithMultipleElements() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        builder.source(new BytesArray("{ "
                + "\"foo\":{"
                + "  \"read\":{ \"application\":\"foo\", \"name\":\"read\", \"actions\":[ \"data:/read/*\", \"admin:/read/*\" ] },"
                + "  \"write\":{ \"application\":\"foo\", \"name\":\"write\", \"actions\":[ \"data:/write/*\", \"admin:*\" ] },"
                + "  \"all\":{ \"application\":\"foo\", \"name\":\"all\", \"actions\":[ \"*\" ] }"
                + " }, "
                + "\"bar\":{"
                + "  \"read\":{ \"application\":\"bar\", \"name\":\"read\", \"actions\":[ \"read/*\" ] },"
                + "  \"write\":{ \"application\":\"bar\", \"name\":\"write\", \"actions\":[ \"write/*\" ] },"
                + "  \"all\":{ \"application\":\"bar\", \"name\":\"all\", \"actions\":[ \"*\" ] }"
                + " } "
                + "}"), XContentType.JSON);
        final List<ApplicationPrivilege> privileges = builder.request().getPrivileges();
        assertThat(privileges, iterableWithSize(6));
        assertThat(privileges, contains(
                new ApplicationPrivilege("foo", "read", "data:/read/*", "admin:/read/*"),
                new ApplicationPrivilege("foo", "write", "data:/write/*", "admin:*"),
                new ApplicationPrivilege("foo", "all", "*"),
                new ApplicationPrivilege("bar", "read", "read/*"),
                new ApplicationPrivilege("bar", "write", "write/*"),
                new ApplicationPrivilege("bar", "all", "*")
        ));
    }

    public void testBuildRequestFromJsonObject() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        builder.source("foo", "read", new BytesArray(
                "{  \"application\":\"foo\", \"name\":\"read\", \"actions\":[ \"data:/read/*\", \"admin:/read/*\" ] }"
        ), XContentType.JSON);
        final List<ApplicationPrivilege> privileges = builder.request().getPrivileges();
        assertThat(privileges, iterableWithSize(1));
        assertThat(privileges, contains(
                new ApplicationPrivilege("foo", "read", "data:/read/*", "admin:/read/*")
        ));
    }

    public void testPrivilegeNameValidationOfSingleElement() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
                builder.source("foo", "write", new BytesArray(
                        "{ \"application\":\"foo\", \"name\":\"read\", \"actions\":[ \"data:/read/*\", \"admin:/read/*\" ] }"
                ), XContentType.JSON));
        assertThat(exception.getMessage(), containsString("write"));
        assertThat(exception.getMessage(), containsString("read"));
    }

    public void testApplicationNameValidationOfSingleElement() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
                builder.source("bar", "read", new BytesArray(
                        "{ \"application\":\"foo\", \"name\":\"read\", \"actions\":[ \"data:/read/*\", \"admin:/read/*\" ] }"
                ), XContentType.JSON));
        assertThat(exception.getMessage(), containsString("foo"));
        assertThat(exception.getMessage(), containsString("bar"));
    }

    public void testPrivilegeNameValidationOfMultipleElement() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
                builder.source(new BytesArray("{ \"foo\":{"
                        + "\"write\":{ \"application\":\"foo\", \"name\":\"read\", \"actions\":[\"data:/read/*\",\"admin:/read/*\"] },"
                        + "\"all\":{ \"application\":\"foo\", \"name\":\"all\", \"actions\":[ \"/*\" ] }"
                        + "} }"), XContentType.JSON)
        );
        assertThat(exception.getMessage(), containsString("write"));
        assertThat(exception.getMessage(), containsString("read"));
    }

    public void testApplicationNameValidationOfMultipleElement() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
                builder.source(new BytesArray("{ \"bar\":{"
                        + "\"read\":{ \"application\":\"foo\", \"name\":\"read\", \"actions\":[ \"data:/read/*\", \"admin:/read/*\" ] },"
                        + "\"write\":{ \"application\":\"foo\", \"name\":\"write\", \"actions\":[ \"data:/write/*\", \"admin:/*\" ] },"
                        + "\"all\":{ \"application\":\"foo\", \"name\":\"all\", \"actions\":[ \"/*\" ] }"
                        + "} }"), XContentType.JSON)
        );
        assertThat(exception.getMessage(), containsString("bar"));
        assertThat(exception.getMessage(), containsString("foo"));
    }

}
