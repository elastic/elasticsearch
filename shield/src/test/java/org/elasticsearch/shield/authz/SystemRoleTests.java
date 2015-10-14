/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

/**
 *
 */
public class SystemRoleTests extends ESTestCase {
    public void testCheck() throws Exception {
        assertThat(SystemRole.INSTANCE.check("indices:monitor/whatever"), is(true));
        assertThat(SystemRole.INSTANCE.check("cluster:monitor/whatever"), is(true));
        assertThat(SystemRole.INSTANCE.check("internal:whatever"), is(true));
        assertThat(SystemRole.INSTANCE.check("cluster:admin/reroute"), is(true));
        assertThat(SystemRole.INSTANCE.check("cluster:admin/whatever"), is(false));
        assertThat(SystemRole.INSTANCE.check("indices:whatever"), is(false));
        assertThat(SystemRole.INSTANCE.check("cluster:whatever"), is(false));
        assertThat(SystemRole.INSTANCE.check("whatever"), is(false));
    }
}
