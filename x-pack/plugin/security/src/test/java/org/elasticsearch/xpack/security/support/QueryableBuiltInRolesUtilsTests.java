/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import static org.hamcrest.Matchers.equalTo;

public class QueryableBuiltInRolesUtilsTests extends ESTestCase {

    public void testCalculateHash() {
        assertThat(
            QueryableBuiltInRolesUtils.calculateHash(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR),
            equalTo("bWEFdFo4WX229wdhdecfiz5QHMYEssh3ex8hizRgg+Q=")
        );
    }
}
