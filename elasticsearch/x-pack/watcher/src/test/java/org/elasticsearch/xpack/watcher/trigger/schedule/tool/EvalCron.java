/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.tool;

import org.elasticsearch.cli.Terminal;

/**
 * A small executable tool that can eval crons
 */
public class EvalCron {

    public static void main(String[] args) throws Exception {
        String expression = Terminal.DEFAULT.readText("cron: ");
        CronEvalTool.main(new String[] { expression });
    }
}
