/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.test.rest.RestTestCandidate;

/** Runs rest tests against external cluster */
public class WatcherWithPainlessIT extends WatcherRestTestCase {

    public WatcherWithPainlessIT(RestTestCandidate testCandidate) {
        super(testCandidate);
    }

}
