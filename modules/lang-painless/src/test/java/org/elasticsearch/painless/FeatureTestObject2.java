/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

/** Currently just a dummy class for testing a few features not yet exposed by whitelist! */
public class FeatureTestObject2 {
    public FeatureTestObject2() {
        super();
    }

    public static int staticNumberArgument(int injected, int userArgument) {
        return injected * userArgument;
    }

    public static int staticNumberArgument2(int userArgument1, int userArgument2) {
        return userArgument1 * userArgument2;
    }
}
