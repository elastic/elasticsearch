/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.action.util;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.util.ForecastStats;



public class ForecastStatsTest extends AbstractWireSerializingTestCase<ForecastStats> {

    @Override
    protected ForecastStats createTestInstance() {

        return null;
    }

    @Override
    protected Reader<ForecastStats> instanceReader() {
        return null;
    }
}
