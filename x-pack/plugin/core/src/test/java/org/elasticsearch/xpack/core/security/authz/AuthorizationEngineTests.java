/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class AuthorizationEngineTests extends ESTestCase {

    public void testIndexAuthorizationResultFailureMessage() {
        assertThat(
            AuthorizationEngine.IndexAuthorizationResult.getFailureDescription(
                new IndicesAccessControl.DeniedIndices(List.of("index-1", "index-2"), List.of("index-3"))
            ),
            is("on indices [index-1,index-2] and restricted indices [index-3]")
        );

        assertThat(
            AuthorizationEngine.IndexAuthorizationResult.getFailureDescription(
                new IndicesAccessControl.DeniedIndices(List.of("index-1"), List.of())
            ),
            is("on indices [index-1]")
        );

        assertThat(
            AuthorizationEngine.IndexAuthorizationResult.getFailureDescription(
                new IndicesAccessControl.DeniedIndices(List.of(), List.of("index-1", "index-2", "index-3"))
            ),
            is("on restricted indices [index-1,index-2,index-3]")
        );
    }

}
