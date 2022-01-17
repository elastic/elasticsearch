/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class UserTests extends ESTestCase {

    public void testUserToString() {
        User user = new User("u1", "r1");
        assertThat(user.toString(), is("User[username=u1,roles=[r1],fullName=null,email=null,metadata={}]"));
        user = new User("u1", new String[] { "r1", "r2" }, "user1", "user1@domain.com", Map.of("key", "val"), true);
        assertThat(user.toString(), is("User[username=u1,roles=[r1,r2],fullName=user1,email=user1@domain.com,metadata={key=val}]"));
        user = new User("u1", new String[] { "r1" }, new User("u2", "r2", "r3"));
        assertThat(
            user.toString(),
            is(
                "User[username=u1,roles=[r1],fullName=null,email=null,metadata={},"
                    + "authenticatedUser=[User[username=u2,roles=[r2,r3],fullName=null,email=null,metadata={}]]]"
            )
        );
    }
}
