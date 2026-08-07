/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class GetServiceAccountRequestValidationTests extends ESTestCase {

    public void testAllowsBuiltInNamespaceAndServiceName() {
        assertThat(new GetServiceAccountRequest("elastic", "fleet-server").validate(), nullValue());
        assertThat(new GetServiceAccountRequest("elastic", null).validate(), nullValue());
    }

    public void testAllowsUnfilteredRequest() {
        assertThat(new GetServiceAccountRequest(null, null).validate(), nullValue());
    }

    public void testRejectsInvalidManagedNamespace() {
        final ActionRequestValidationException validation = new GetServiceAccountRequest("my*", "worker").validate();
        assertThat(validation.validationErrors().get(0), containsString("namespace"));
    }

    public void testRejectsInvalidServiceName() {
        final ActionRequestValidationException validation = new GetServiceAccountRequest("my-team", "worker*").validate();
        assertThat(validation.validationErrors().get(0), containsString("service name"));
    }

    public void testAllowsElasticNamespaceWithServiceName() {
        assertThat(new GetServiceAccountRequest("elastic", "custom").validate(), nullValue());
    }
}
