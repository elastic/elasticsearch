/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class ManagedServiceAccountIdValidatorTests extends ESTestCase {

    public void testRejectsElasticNamespace() {
        assertThat(ManagedServiceAccountIdValidator.validateNamespace("elastic"), containsString("reserved for built-in service accounts"));
    }

    public void testRejectsInvalidDelimiters() {
        assertThat(ManagedServiceAccountIdValidator.validateServiceName("bad/name"), containsString("must not contain"));
        assertThat(ManagedServiceAccountIdValidator.validateServiceName("bad:name"), containsString("must not contain"));
    }

    public void testAcceptsValidManagedPrincipal() {
        assertThat(ManagedServiceAccountIdValidator.validatePrincipal("my-team/my-service"), nullValue());
    }

    public void testRunAsFromRejectsWildcardsAndEmpty() {
        assertThat(ManagedServiceAccountIdValidator.validateRunAsFromPrincipal("*"), containsString("wildcard"));
        assertThat(ManagedServiceAccountIdValidator.validateRunAsFromPrincipal("acme/*"), containsString("wildcard"));
        assertThat(ManagedServiceAccountIdValidator.validateRunAsFromPrincipal(""), containsString("must not be empty"));
        assertThat(ManagedServiceAccountIdValidator.validateRunAsFromPrincipal("elastic/kibana"), nullValue());
        assertThat(ManagedServiceAccountIdValidator.validateRunAsFromPrincipal("acme/other-workflow"), nullValue());
        assertThat(ManagedServiceAccountIdValidator.validateRunAsFromPrincipal("native_user"), containsString("must be a service account"));
    }

    public void testRejectsWhitespace() {
        assertThat(ManagedServiceAccountIdValidator.validateNamespace(" leading"), containsString("whitespace"));
    }
}
