/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.tool;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class CronEvalToolTests extends CliToolTestCase {

    @Test @Repeat(iterations = 10)
    public void testParse() throws Exception {
        String countOption = randomBoolean() ? "-c" : "--count";
        int count = randomIntBetween(1, 100);
        CliTool.Command command = new CronEvalTool().parse("eval", new String[] { "0 0 0 1-6 * ?", countOption, String.valueOf(count) });
        assertThat(command, instanceOf(CronEvalTool.Eval.class));
        CronEvalTool.Eval eval = (CronEvalTool.Eval) command;
        assertThat(eval.expression, is("0 0 0 1-6 * ?"));
        assertThat(eval.count, is(count));
    }
}
