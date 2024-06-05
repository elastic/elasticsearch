/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.mixed;

import org.elasticsearch.test.rest.ESRestTestCase;

public class DataTierMixedIT extends ESRestTestCase {

    public void testMixedTierCompatibility() throws Exception {
        createIndex("test-index", indexSettings(1, 0).build());
        ensureGreen("test-index");
    }
}
