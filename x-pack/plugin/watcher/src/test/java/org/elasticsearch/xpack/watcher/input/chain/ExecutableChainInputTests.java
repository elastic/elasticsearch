/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input.chain;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.elasticsearch.xpack.core.watcher.input.Input.Result.Status;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
import static org.hamcrest.Matchers.is;

public class ExecutableChainInputTests extends ESTestCase {

    public void testFailedResultHandling() throws Exception {
        WatchExecutionContext ctx = createWatchExecutionContext();
        ChainInput chainInput = new ChainInput(Arrays.asList(new Tuple<>("whatever", new SimpleInput(Payload.EMPTY))));

        Tuple<String, ExecutableInput> tuple = new Tuple<>("whatever", new FailingExecutableInput());
        ExecutableChainInput executableChainInput = new ExecutableChainInput(chainInput, Arrays.asList(tuple));
        ChainInput.Result result = executableChainInput.execute(ctx, Payload.EMPTY);
        assertThat(result.status(), is(Status.SUCCESS));
    }

    private class FailingExecutableInput extends ExecutableInput<SimpleInput, Input.Result> {

        protected FailingExecutableInput() {
            super(new SimpleInput(Payload.EMPTY));
        }

        @Override
        public Input.Result execute(WatchExecutionContext ctx, @Nullable Payload payload) {
            return new FailingExecutableInputResult(new RuntimeException("foo"));
        }
    }

    private static class FailingExecutableInputResult extends Input.Result {

        protected FailingExecutableInputResult(Exception e) {
            super("failing", e);
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    private WatchExecutionContext createWatchExecutionContext() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Wid wid = new Wid(randomAlphaOfLength(5), now);
        return mockExecutionContextBuilder(wid.watchId())
                .wid(wid)
                .payload(new Payload.Simple())
                .time(wid.watchId(), now)
                .buildMock();
    }

}
