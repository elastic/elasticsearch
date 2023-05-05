/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm.dataperiods;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DefaultDataPeriodsTests extends AbstractXContentTestCase<DefaultDataPeriods> {

    public void testEmptySetting() {
        DefaultDataPeriods defaultDataPeriods = DefaultDataPeriods.parseSetting("{\"data_periods\":[]}");
        assertThat(defaultDataPeriods, equalTo(new DefaultDataPeriods(List.of())));
    }

    public void testValidSetting() {
        DefaultDataPeriods defaultDataPeriods = DefaultDataPeriods.parseSetting("""
            {
              "data_periods":[
                {
                  "name_pattern": "my-short-lived-data-stream-*",
                  "interactive": "7d",
                  "retention": "14d",
                  "priority": 500
                },
                {
                  "name_pattern": ".logs-short-*",
                  "interactive": "7d",
                  "retention": "7d",
                  "priority": 500
                },
                {
                  "name_pattern": ".logs-*",
                  "interactive": "7d",
                  "retention": "90d",
                  "priority": 200
                },
                {
                  "name_pattern": "not-interactive",
                  "retention": "365d",
                  "priority": 500
                },
                {
                  "name_pattern": "*",
                  "interactive": "7d",
                  "priority": 0
                }
              ]
            }
            """);
        assertThat(
            defaultDataPeriods.getDataPeriods(),
            equalTo(
                List.of(
                    new DataPeriod(".logs-short-*", TimeValue.timeValueDays(7), TimeValue.timeValueDays(7), 500),
                    new DataPeriod("my-short-lived-data-stream-*", TimeValue.timeValueDays(7), TimeValue.timeValueDays(14), 500),
                    new DataPeriod("not-interactive", null, TimeValue.timeValueDays(365), 500),
                    new DataPeriod(".logs-*", TimeValue.timeValueDays(7), TimeValue.timeValueDays(90), 200),
                    new DataPeriod("*", TimeValue.timeValueDays(7), null, 0)
                )
            )
        );
    }

    public void testInvalidOrder() {
        List<DataPeriod> dataPeriods = List.of(
            new DataPeriod("*", null, null, 10),
            new DataPeriod("unreachable*", null, null, 20),
            new DataPeriod("unreachable", null, null, 1)
        );
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> new DefaultDataPeriods(dataPeriods));
        assertThat(
            error.getMessage(),
            containsString("name pattern 'unreachable' is unreachable because of preceding name patterns: [*, unreachable*]")
        );
    }

    @Override
    protected DefaultDataPeriods createTestInstance() {
        List<DataPeriod> dataPeriodList = new ArrayList<>();
        for (int i = 0; i < randomInt(10); i++) {
            dataPeriodList.add(DataPeriodTests.randomDataPeriod());
        }
        return new DefaultDataPeriods(dataPeriodList);
    }

    @Override
    protected DefaultDataPeriods doParseInstance(XContentParser parser) throws IOException {
        return DefaultDataPeriods.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
