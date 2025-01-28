/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;

import java.io.IOException;

public class AuthenticateResponseTests extends AbstractWireSerializingTestCase<AuthenticateResponse> {

    @Override
    protected Writeable.Reader<AuthenticateResponse> instanceReader() {
        return AuthenticateResponse::new;
    }

    @Override
    protected AuthenticateResponse createTestInstance() {
        return new AuthenticateResponse(AuthenticationTestHelper.builder().build(), randomBoolean());
    }

    @Override
    protected AuthenticateResponse mutateInstance(AuthenticateResponse instance) throws IOException {
        if (randomBoolean()) {
            return new AuthenticateResponse(
                randomValueOtherThanMany(instance::equals, () -> AuthenticationTestHelper.builder().build()),
                instance.isOperator()
            );
        } else {
            return new AuthenticateResponse(instance.authentication(), instance.isOperator() == false);
        }
    }
}
