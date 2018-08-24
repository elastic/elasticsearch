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

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.protocol.xpack.security.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;

import java.util.Collections;
import java.util.Date;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutUserRequestTests extends ESTestCase {

    public void testValidateReturnsNullForCorrectData() throws Exception {
        final PutUserRequest request = new PutUserRequest();
        request.username("foo");
        request.roles("bar");
        request.metadata(Collections.singletonMap("created", new Date()));
        final ActionRequestValidationException validation = request.validate();
        assertThat(validation, is(nullValue()));
    }

    public void testValidateRejectsNullUserName() throws Exception {
        final PutUserRequest request = new PutUserRequest();
        request.username(null);
        request.roles("bar");
        final ActionRequestValidationException validation = request.validate();
        assertThat(validation, is(notNullValue()));
        assertThat(validation.validationErrors(), contains(is("user is missing")));
        assertThat(validation.validationErrors().size(), is(1));
    }

    public void testValidateRejectsMetaDataWithLeadingUnderscore() throws Exception {
        final PutUserRequest request = new PutUserRequest();
        request.username("foo");
        request.roles("bar");
        request.metadata(Collections.singletonMap("_created", new Date()));
        final ActionRequestValidationException validation = request.validate();
        assertThat(validation, is(notNullValue()));
        assertThat(validation.validationErrors(), contains(containsString("metadata keys")));
        assertThat(validation.validationErrors().size(), is(1));
    }
}
