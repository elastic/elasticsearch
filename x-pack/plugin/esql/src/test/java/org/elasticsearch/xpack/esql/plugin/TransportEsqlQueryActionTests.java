/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TransportEsqlQueryActionTests extends ComputeTestCase {

    /** Nothing upstream releases the pages once the helper throws; {@link ComputeTestCase#allBreakersEmpty()} catches a leak. */
    public void testFailIfCancelledReleasesPages() {
        Result result = resultWithTwoPages();
        CancellableTask task = newTask();
        TaskCancelHelper.cancel(task, "by user request");
        var e = expectThrows(TaskCancelledException.class, () -> TransportEsqlQueryAction.failIfCancelled(task, result));
        assertThat(e.getMessage(), equalTo("by user request"));
    }

    public void testFailIfCancelledKeepsPagesWhenNotCancelled() {
        Result result = resultWithTwoPages();
        TransportEsqlQueryAction.failIfCancelled(newTask(), result);
        for (Page page : result.pages()) {
            page.releaseBlocks();
        }
    }

    private Result resultWithTwoPages() {
        BlockFactory blockFactory = blockFactory();
        List<Page> pages = List.of(
            new Page(blockFactory.newConstantIntBlockWith(1, 10)),
            new Page(blockFactory.newConstantIntBlockWith(2, 10))
        );
        return new Result(List.of(), pages, DriverCompletionInfo.EMPTY, null);
    }

    private static CancellableTask newTask() {
        return new CancellableTask(1, "transport", EsqlQueryAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
    }
}
