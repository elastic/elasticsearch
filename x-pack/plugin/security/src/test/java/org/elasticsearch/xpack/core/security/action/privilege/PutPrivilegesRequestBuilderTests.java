/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;

public class PutPrivilegesRequestBuilderTests extends ESTestCase {

    public void testBuildRequestWithMultipleElements() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        builder.source(new BytesArray("""
            {
              "foo": {
                "read": {
                  "application": "foo",
                  "name": "read",
                  "actions": [ "data:/read/*", "admin:/read/*" ]
                },
                "write": {
                  "application": "foo",
                  "name": "write",
                  "actions": [ "data:/write/*", "admin:*" ]
                },
                "all": {
                  "application": "foo",
                  "name": "all",
                  "actions": [ "*" ]
                }
              },
              "bar": {
                "read": {
                  "application": "bar",
                  "name": "read",
                  "actions": [ "read/*" ]
                },
                "write": {
                  "application": "bar",
                  "name": "write",
                  "actions": [ "write/*" ]
                },
                "all": {
                  "application": "bar",
                  "name": "all",
                  "actions": [ "*" ]
                }
              }
            }"""), XContentType.JSON);
        final List<ApplicationPrivilegeDescriptor> privileges = builder.request().getPrivileges();
        assertThat(privileges, iterableWithSize(6));
        assertThat(
            privileges,
            contains(
                descriptor("foo", "read", "data:/read/*", "admin:/read/*"),
                descriptor("foo", "write", "data:/write/*", "admin:*"),
                descriptor("foo", "all", "*"),
                descriptor("bar", "read", "read/*"),
                descriptor("bar", "write", "write/*"),
                descriptor("bar", "all", "*")
            )
        );
    }

    private ApplicationPrivilegeDescriptor descriptor(String app, String name, String... actions) {
        return new ApplicationPrivilegeDescriptor(app, name, Sets.newHashSet(actions), Collections.emptyMap());
    }

    public void testPrivilegeNameValidationOfMultipleElement() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> builder.source(new BytesArray("""
            {
              "foo": {
                "write": {
                  "application": "foo",
                  "name": "read",
                  "actions": [ "data:/read/*", "admin:/read/*" ]
                },
                "all": {
                  "application": "foo",
                  "name": "all",
                  "actions": [ "/*" ]
                }
              }
            }"""), XContentType.JSON));
        assertThat(exception.getMessage(), containsString("write"));
        assertThat(exception.getMessage(), containsString("read"));
    }

    public void testApplicationNameValidationOfMultipleElement() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> builder.source(new BytesArray("""
            {
              "bar": {
                "read": {
                  "application": "foo",
                  "name": "read",
                  "actions": [ "data:/read/*", "admin:/read/*" ]
                },
                "write": {
                  "application": "foo",
                  "name": "write",
                  "actions": [ "data:/write/*", "admin:/*" ]
                },
                "all": {
                  "application": "foo",
                  "name": "all",
                  "actions": [ "/*" ]
                }
              }
            }"""), XContentType.JSON));
        assertThat(exception.getMessage(), containsString("bar"));
        assertThat(exception.getMessage(), containsString("foo"));
    }

    public void testInferApplicationNameAndPrivilegeName() throws Exception {
        final PutPrivilegesRequestBuilder builder = new PutPrivilegesRequestBuilder(null);
        builder.source(new BytesArray("""
            {
              "foo": {
                "read": {
                  "actions": [ "data:/read/*", "admin:/read/*" ]
                },
                "write": {
                  "actions": [ "data:/write/*", "admin:/*" ]
                },
                "all": {
                  "actions": [ "*" ]
                }
              }
            }"""), XContentType.JSON);
        assertThat(builder.request().getPrivileges(), iterableWithSize(3));
        for (ApplicationPrivilegeDescriptor p : builder.request().getPrivileges()) {
            assertThat(p.getApplication(), equalTo("foo"));
            assertThat(p.getName(), notNullValue());
        }
        assertThat(builder.request().getPrivileges().get(0).getName(), equalTo("read"));
        assertThat(builder.request().getPrivileges().get(1).getName(), equalTo("write"));
        assertThat(builder.request().getPrivileges().get(2).getName(), equalTo("all"));
    }

}
