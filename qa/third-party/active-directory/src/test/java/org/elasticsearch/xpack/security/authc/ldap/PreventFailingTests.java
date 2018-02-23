/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.test.ESTestCase;

public class PreventFailingTests extends ESTestCase {

    public void testPreventFailingWithNetworkingDisabled() {
        // Noop
        // This is required because if network tests are not enabled no tests will be run in the entire project and all tests will fail.
    }

}
