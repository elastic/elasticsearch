/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.test.rest.ESRestTestCase;

public class AsyncSearchRestTestCase extends ESRestTestCase {
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }
}
