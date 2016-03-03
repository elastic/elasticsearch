/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.tool;

import org.elasticsearch.test.ESTestCase;

public class CronEvalToolTests extends ESTestCase {
    public void testParse() throws Exception {
        String countOption = randomBoolean() ? "-c" : "--count";
        int count = randomIntBetween(1, 100);
        /*
        CliTool.Command command = new CronEvalTool().parse("eval", new String[] { "0 0 0 1-6 * ?", countOption, String.valueOf(count) });
        assertThat(command, instanceOf(CronEvalTool.Eval.class));
        CronEvalTool.Eval eval = (CronEvalTool.Eval) command;
        assertThat(eval.expression, is("0 0 0 1-6 * ?"));
        assertThat(eval.count, is(count));
        */
    }
}
