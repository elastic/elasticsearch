/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

public class RollupSearchActionTests extends ESTestCase {

    public void testIndexReadPrivilegeCanPerformRollupSearchAction() {
        assertTrue(Automatons.predicate(IndexPrivilege.READ.getAutomaton()).test(RollupSearchAction.NAME));
    }
}
