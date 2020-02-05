/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.session.Configuration;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomZone;

public final class EqlTestUtils {

    private EqlTestUtils() {}

    public static final Configuration TEST_CFG = new Configuration(new String[] { "none" }, org.elasticsearch.xpack.ql.util.DateUtils.UTC,
            "nobody", "cluster", null, TimeValue.timeValueSeconds(30), false, "");

    public static Configuration randomConfiguration() {
        return new Configuration(new String[] {randomAlphaOfLength(16)},
                randomZone(),
                randomAlphaOfLength(16),
                randomAlphaOfLength(16),
                null,
                new TimeValue(randomNonNegativeLong()),
                randomBoolean(),
                randomAlphaOfLength(16));
    }
}
