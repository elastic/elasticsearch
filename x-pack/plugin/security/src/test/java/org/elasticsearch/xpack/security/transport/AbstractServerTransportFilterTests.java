/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.arrayWithSize;

public abstract class AbstractServerTransportFilterTests extends ESTestCase {

    protected static Answer<Class<Void>> getAnswer(Authentication authentication) {
        return getAnswer(authentication, false);
    }

    protected static Answer<Class<Void>> getAnswer(Authentication authentication, boolean crossClusterAccess) {
        return i -> {
            final Object[] args = i.getArguments();
            assertThat(args, arrayWithSize(crossClusterAccess ? 3 : 4));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> callback = (ActionListener<Authentication>) args[args.length - 1];
            callback.onResponse(authentication);
            return Void.TYPE;
        };
    }

}
