/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

/**
 *
 */
public class SystemInternalUserTests extends ESTestCase {

    public void testIsAuthorized() throws Exception {
        assertThat(InternalSystemUser.isAuthorized("indices:monitor/whatever"), is(true));
        assertThat(InternalSystemUser.isAuthorized("cluster:monitor/whatever"), is(true));
        assertThat(InternalSystemUser.isAuthorized("internal:whatever"), is(true));
        assertThat(InternalSystemUser.isAuthorized("cluster:admin/reroute"), is(true));
        assertThat(InternalSystemUser.isAuthorized("cluster:admin/whatever"), is(false));
        assertThat(InternalSystemUser.isAuthorized("indices:whatever"), is(false));
        assertThat(InternalSystemUser.isAuthorized("cluster:whatever"), is(false));
        assertThat(InternalSystemUser.isAuthorized("whatever"), is(false));
    }
}
