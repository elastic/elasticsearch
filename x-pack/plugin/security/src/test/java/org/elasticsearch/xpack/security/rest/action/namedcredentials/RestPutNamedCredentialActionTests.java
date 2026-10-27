/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.namedcredentials;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class RestPutNamedCredentialActionTests extends ESTestCase {

    public void testImplementsRestRequestFilter() {
        RestPutNamedCredentialAction action = new RestPutNamedCredentialAction(Settings.EMPTY, new XPackLicenseState(() -> 0));
        assertThat(action, instanceOf(RestRequestFilter.class));
    }

    public void testFilteredFieldsContainsAuth() {
        RestPutNamedCredentialAction action = new RestPutNamedCredentialAction(Settings.EMPTY, new XPackLicenseState(() -> 0));
        Set<String> filtered = action.getFilteredFields();
        assertThat(filtered, hasItem("auth"));
    }
}
