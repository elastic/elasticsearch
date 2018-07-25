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

package org.elasticsearch.protocol.xpack.security;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class UserTests extends ESTestCase {

    public void testUserToString() {
        User user = new User("u1", "r1");
        assertThat(user.toString(), is("User[username=u1,roles=[r1],fullName=null,email=null,metadata={}]"));
        user = new User("u1", new String[] { "r1", "r2" }, "user1", "user1@domain.com", Collections.singletonMap("key", "val"), true);
        assertThat(user.toString(), is("User[username=u1,roles=[r1,r2],fullName=user1,email=user1@domain.com,metadata={key=val}]"));
        user = new User("u1", new String[] {"r1"}, new User("u2", "r2", "r3"));
        assertThat(user.toString(), is("User[username=u1,roles=[r1],fullName=null,email=null,metadata={}," +
            "authenticatedUser=[User[username=u2,roles=[r2,r3],fullName=null,email=null,metadata={}]]]"));
    }
}
