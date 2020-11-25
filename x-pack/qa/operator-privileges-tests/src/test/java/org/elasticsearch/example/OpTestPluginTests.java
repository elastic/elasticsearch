/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.example;

import org.elasticsearch.test.ESTestCase;

// This test class is really to pass the testingConventions test
public class OpTestPluginTests extends ESTestCase {

    public void testPluginWillInstantiate() {
        final OpTestPlugin opTestPlugin = new OpTestPlugin();
    }

}
