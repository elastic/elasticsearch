/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.junit.BeforeClass;

import static org.hamcrest.Matchers.equalTo;

public class QueryableRolesUtilsTests extends ESTestCase {

    @BeforeClass
    public static void setupReservedRoles() {
        new ReservedRolesStore();
    }


    public void testCalculateHash() {
        assertThat(
            QueryableRolesUtils.calculateHash(ReservedRolesStore.roleDescriptors()),
            equalTo("qQIXQCYQOdBCJo0IfWCxmkH12Mq2wdELrpnIx/+eRRc=")
        );
    }
}
