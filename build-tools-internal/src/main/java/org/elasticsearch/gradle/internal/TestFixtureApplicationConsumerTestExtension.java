/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class TestFixtureApplicationConsumerTestExtension {

    private final Multimap<String, String> fixtureApplications = ArrayListMultimap.create();

    public TestFixtureApplicationConsumerTestExtension() {}

    public void use(String fixtureApplicationName, String serviceName) {
        fixtureApplications.put(fixtureApplicationName, serviceName);
    }

    public Multimap<String, String> getFixtureApplications() {
        return fixtureApplications;
    }
}
