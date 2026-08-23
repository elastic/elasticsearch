/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.nullValue;

public class GetServiceAccountRequestValidationTests extends ESTestCase {

    public void testAllowsBuiltInNamespaceAndServiceName() {
        assertThat(new GetServiceAccountRequest("elastic", "fleet-server").validate(), nullValue());
        assertThat(new GetServiceAccountRequest("elastic", null).validate(), nullValue());
    }

    public void testAllowsUnfilteredRequest() {
        assertThat(new GetServiceAccountRequest(null, null).validate(), nullValue());
    }

    public void testAllowsInvalidManagedNamespace() {
        assertThat(new GetServiceAccountRequest("my*", "worker").validate(), nullValue());
    }

    public void testAllowsInvalidServiceName() {
        assertThat(new GetServiceAccountRequest("my-team", "worker*").validate(), nullValue());
    }

    public void testAllowsElasticNamespaceWithServiceName() {
        assertThat(new GetServiceAccountRequest("elastic", "custom").validate(), nullValue());
    }
}
