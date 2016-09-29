/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;

/** Runs rest tests against external cluster */
public class WatcherWithGroovyIT extends WatcherRestTestCase {

    public WatcherWithGroovyIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

}
