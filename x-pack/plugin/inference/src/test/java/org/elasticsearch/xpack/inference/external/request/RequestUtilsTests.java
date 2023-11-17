/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.hamcrest.Matchers.is;

public class RequestUtilsTests extends ESTestCase {
    public void testCreateAuthBearerHeader() {
        var header = createAuthBearerHeader(new SecureString("abc".toCharArray()));

        assertThat(header.getName(), is("Authorization"));
        assertThat(header.getValue(), is("Bearer abc"));
    }
}
