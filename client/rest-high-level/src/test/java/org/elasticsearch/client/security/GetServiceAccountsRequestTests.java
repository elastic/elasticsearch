/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountsRequestTests extends ESTestCase {

    public void testNewInstance() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);

        final GetServiceAccountsRequest request1 = new GetServiceAccountsRequest(namespace, serviceName);
        assertThat(request1.getNamespace(), equalTo(namespace));
        assertThat(request1.getServiceName(), equalTo(serviceName));

        final GetServiceAccountsRequest request2 = new GetServiceAccountsRequest(namespace);
        assertThat(request2.getNamespace(), equalTo(namespace));
        assertNull(request2.getServiceName());

        final GetServiceAccountsRequest request3 = new GetServiceAccountsRequest();
        assertNull(request3.getNamespace());
        assertNull(request3.getServiceName());

        final GetServiceAccountsRequest request4 = new GetServiceAccountsRequest(null, namespace);
        final Optional<ValidationException> validationException = request4.validate();
        assertTrue(validationException.isPresent());
        assertThat(validationException.get().getMessage(), containsString("cannot specify service-name without namespace"));
    }
}
