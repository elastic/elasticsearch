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

import static org.elasticsearch.test.LambdaMatchers.trueWith;

public class RollupSearchActionTests extends ESTestCase {

    public void testIndexReadPrivilegeCanPerformRollupSearchAction() {
        assertThat(Automatons.predicate(IndexPrivilege.READ.getAutomaton()), trueWith(RollupSearchAction.NAME));
    }
}
