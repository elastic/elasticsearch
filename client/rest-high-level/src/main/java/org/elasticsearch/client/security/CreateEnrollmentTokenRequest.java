/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Validatable;

public class CreateEnrollmentTokenRequest implements Validatable {

    public CreateEnrollmentTokenRequest() {
    }

    public static final CreateEnrollmentTokenRequest INSTANCE = new CreateEnrollmentTokenRequest();


    public Request getRequest() {
        return new Request(HttpPut.METHOD_NAME, "/_security/enrollment_token");
    }
}
