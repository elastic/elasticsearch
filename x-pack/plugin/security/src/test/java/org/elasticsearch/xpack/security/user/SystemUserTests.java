/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.user.SystemUser;

import static org.hamcrest.Matchers.is;

public class SystemUserTests extends ESTestCase {

    public void testIsAuthorized() throws Exception {
        assertThat(SystemUser.isAuthorized("indices:monitor/whatever"), is(true));
        assertThat(SystemUser.isAuthorized("cluster:monitor/whatever"), is(true));
        assertThat(SystemUser.isAuthorized("internal:whatever"), is(true));
        assertThat(SystemUser.isAuthorized("cluster:admin/reroute"), is(true));
        assertThat(SystemUser.isAuthorized("cluster:admin/whatever"), is(false));
        assertThat(SystemUser.isAuthorized("indices:whatever"), is(false));
        assertThat(SystemUser.isAuthorized("cluster:whatever"), is(false));
        assertThat(SystemUser.isAuthorized("whatever"), is(false));
    }
}
