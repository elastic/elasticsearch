/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.session.Configuration;

import java.util.Collections;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomZone;

public final class EqlTestUtils {

    private EqlTestUtils() {
    }

    public static final Configuration TEST_CFG = new Configuration(new String[]{"none"}, org.elasticsearch.xpack.ql.util.DateUtils.UTC,
            "nobody", "cluster", null, TimeValue.timeValueSeconds(30), -1, false, "",
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()), () -> false);

    public static Configuration randomConfiguration() {
        return new Configuration(new String[]{randomAlphaOfLength(16)},
            randomZone(),
            randomAlphaOfLength(16),
            randomAlphaOfLength(16),
            null,
            new TimeValue(randomNonNegativeLong()),
            randomIntBetween(5, 100),
            randomBoolean(),
            randomAlphaOfLength(16),
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            () -> false);
    }

    public static EqlSearchTask randomTask() {
        return new EqlSearchTask(randomLong(), "transport", EqlSearchAction.NAME, () -> "", null, Collections.emptyMap());
    }
}
