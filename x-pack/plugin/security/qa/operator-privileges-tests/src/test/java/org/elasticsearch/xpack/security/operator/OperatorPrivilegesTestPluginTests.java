/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.test.ESTestCase;

// This test class is really to pass the testingConventions test
public class OperatorPrivilegesTestPluginTests extends ESTestCase {

    public void testPluginWillInstantiate() {
        final OperatorPrivilegesTestPlugin operatorPrivilegesTestPlugin = new OperatorPrivilegesTestPlugin();
    }

}
