/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;

import java.util.Collections;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomZone;

public final class EqlTestUtils {

    private EqlTestUtils() {
    }

    public static final EqlConfiguration TEST_CFG_CASE_INSENSITIVE = new EqlConfiguration(new String[] {"none"},
            org.elasticsearch.xpack.ql.util.DateUtils.UTC, "nobody", "cluster", null, TimeValue.timeValueSeconds(30), false, false, 
            "", new TaskId("test", 123), null);

    public static final EqlConfiguration TEST_CFG_CASE_SENSITIVE = new EqlConfiguration(new String[] {"none"},
            org.elasticsearch.xpack.ql.util.DateUtils.UTC, "nobody", "cluster", null, TimeValue.timeValueSeconds(30), false, true, 
            "", new TaskId("test", 123), null);

    public static EqlConfiguration randomConfiguration() {
        return internalRandomConfiguration(randomBoolean());
    }

    public static EqlConfiguration randomConfigurationWithCaseSensitive(boolean isCaseSensitive) {
        return internalRandomConfiguration(isCaseSensitive);
    }

    private static EqlConfiguration internalRandomConfiguration(boolean isCaseSensitive) {
        return new EqlConfiguration(new String[]{randomAlphaOfLength(16)},
            randomZone(),
            randomAlphaOfLength(16),
            randomAlphaOfLength(16),
            null,
            new TimeValue(randomNonNegativeLong()),
            randomBoolean(),
            isCaseSensitive,
            randomAlphaOfLength(16),
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomTask());
    }

    public static EqlSearchTask randomTask() {
        return new EqlSearchTask(randomLong(), "transport", EqlSearchAction.NAME, "", null, Collections.emptyMap(), Collections.emptyMap(),
            new AsyncExecutionId("", new TaskId(randomAlphaOfLength(10), 1)), TimeValue.timeValueDays(5));
    }
}
