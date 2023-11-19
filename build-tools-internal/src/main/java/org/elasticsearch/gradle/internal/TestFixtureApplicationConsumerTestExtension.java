/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import java.util.ArrayList;
import java.util.List;

public class TestFixtureApplicationConsumerTestExtension {

    private final List<String> fixtureApplications = new ArrayList<>();

    public TestFixtureApplicationConsumerTestExtension() {}

    public void use(String fixtureApplicationName) {
        fixtureApplications.add(fixtureApplicationName);
    }

    public Iterable<String> getFixtureApplications() {
        return fixtureApplications;
    }
}
