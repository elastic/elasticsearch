/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class CronScheduleTests extends ScheduleTestCase {

    @Test(expected = CronSchedule.ValidationException.class)
    public void testInvalid() throws Exception {
        new CronSchedule("0 * * *");
        fail("expecting a validation error to be thrown when creating a cron schedule with invalid cron expression");
    }

    @Test
    public void testParse_Single() throws Exception {
        XContentBuilder builder = jsonBuilder().value("0 0/5 * * * ?");
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        CronSchedule schedule = new CronSchedule.Parser().parse(parser);
        assertThat(schedule.crons(), arrayWithSize(1));
        assertThat(schedule.crons()[0].expression(), is("0 0/5 * * * ?"));
    }

    @Test
    public void testParse_Multiple() throws Exception {
        XContentBuilder builder = jsonBuilder().value(new String[] {
                "0 0/1 * * * ?",
                "0 0/2 * * * ?",
                "0 0/3 * * * ?"
        });
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        CronSchedule schedule = new CronSchedule.Parser().parse(parser);
        String[] crons = expressions(schedule);
        assertThat(crons, arrayWithSize(3));
        assertThat(crons, hasItemInArray("0 0/1 * * * ?"));
        assertThat(crons, hasItemInArray("0 0/2 * * * ?"));
        assertThat(crons, hasItemInArray("0 0/3 * * * ?"));
    }

    @Test
    public void testParse_Invalid_BadExpression() throws Exception {
        XContentBuilder builder = jsonBuilder().value("0 0/5 * * ?");
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        try {
            new CronSchedule.Parser().parse(parser);
            fail("expected cron parsing to fail when using invalid cron expression");
        } catch (WatcherSettingsException ase) {
            // expected
            assertThat(ase.getCause(), instanceOf(CronSchedule.ValidationException.class));
        }
    }

    @Test(expected = WatcherSettingsException.class)
    public void testParse_Invalid_Empty() throws Exception {
        XContentBuilder builder = jsonBuilder();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        new CronSchedule.Parser().parse(parser);
    }

    @Test(expected = WatcherSettingsException.class)
    public void testParse_Invalid_Object() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        new CronSchedule.Parser().parse(parser);
    }

    @Test(expected = WatcherSettingsException.class)
    public void testParse_Invalid_EmptyArray() throws Exception {
        XContentBuilder builder = jsonBuilder().value(new String[0]);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        new CronSchedule.Parser().parse(parser);
    }
}
