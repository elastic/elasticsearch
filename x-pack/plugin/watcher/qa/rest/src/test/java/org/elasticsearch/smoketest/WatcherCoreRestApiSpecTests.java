/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.xpack.watcher.WatcherRestApiSpecTests;

/**
 * Runs the YAML rest tests against an external cluster
 */
public class WatcherCoreRestApiSpecTests extends WatcherRestApiSpecTests {
    public WatcherCoreRestApiSpecTests(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }
}
