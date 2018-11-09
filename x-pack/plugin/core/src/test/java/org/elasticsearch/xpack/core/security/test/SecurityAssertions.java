/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.test;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackField;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SecurityAssertions {

    public static void assertContainsWWWAuthenticateHeader(ElasticsearchSecurityException e) {
        assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
        assertThat(e.getHeaderKeys(), hasSize(1));
        assertThat(e.getHeader("WWW-Authenticate"), notNullValue());
        assertThat(e.getHeader("WWW-Authenticate"), contains("Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\""));
    }
}
