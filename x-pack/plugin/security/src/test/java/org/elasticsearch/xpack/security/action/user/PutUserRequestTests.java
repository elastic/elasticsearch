/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
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

    public void testValidateRejectsMetadataWithLeadingUnderscore() throws Exception {
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
