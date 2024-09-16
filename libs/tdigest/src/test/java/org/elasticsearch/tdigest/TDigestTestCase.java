/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tdigest;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.search.aggregations.metrics.WrapperTDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.test.ESTestCase;

abstract class TDigestTestCase extends ESTestCase {
    protected TDigestArrays arrays() {
        return new WrapperTDigestArrays(newLimitedBreaker(ByteSizeValue.ofMb(100)));
    }
}
