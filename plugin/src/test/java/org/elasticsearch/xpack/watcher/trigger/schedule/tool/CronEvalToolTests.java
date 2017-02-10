/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.tool;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;

public class CronEvalToolTests extends CommandTestCase {
    @Override
    protected Command newCommand() {
        return new CronEvalTool();
    }

    public void testParse() throws Exception {
        String countOption = randomBoolean() ? "-c" : "--count";
        int count = randomIntBetween(1, 100);
        String output = execute(countOption, Integer.toString(count), "0 0 0 1-6 * ?");
        assertTrue(output, output.contains("Here are the next " + count + " times this cron expression will trigger"));
    }
}
