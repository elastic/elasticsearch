/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local.distribution;

public enum DistributionType {
    INTEG_TEST("tests.integ-test.distribution"),
    DEFAULT("tests.default.distribution");

    private final String systemProperty;

    DistributionType(String systemProperty) {
        this.systemProperty = systemProperty;
    }

    public String getSystemProperty() {
        return systemProperty;
    }
}
