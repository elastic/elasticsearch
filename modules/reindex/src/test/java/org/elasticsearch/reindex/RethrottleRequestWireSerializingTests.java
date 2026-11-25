/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class RethrottleRequestWireSerializingTests extends AbstractWireSerializingTestCase<RethrottleRequestWireSerializingTests.Wrapper> {

    @Override
    protected Writeable.Reader<Wrapper> instanceReader() {
        return Wrapper::new;
    }

    @Override
    protected Wrapper createTestInstance() {
        RethrottleRequest request = new RethrottleRequest();
        request.setRequestsPerSecond((float) randomDoubleBetween(0.001d, Float.POSITIVE_INFINITY, false));
        if (randomBoolean()) {
            request.setParentTask(new TaskId(randomAlphaOfLength(6), randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setTargetTaskId(new TaskId(randomAlphaOfLength(6), randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setTargetParentTaskId(new TaskId(randomAlphaOfLength(6), randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setNodes(randomAlphaOfLength(5));
        }
        if (randomBoolean()) {
            request.setActions(randomFrom(ReindexAction.NAME, UpdateByQueryAction.NAME, DeleteByQueryAction.NAME));
        }
        if (randomBoolean()) {
            request.setTimeout(TimeValue.timeValueMillis(between(1, 600_000)));
        }
        return new Wrapper(request);
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        RethrottleRequest orig = instance.request;
        RethrottleRequest copy = copyInstance(instance).request;
        switch (between(0, 6)) {
            case 0 -> copy.setParentTask(
                randomValueOtherThan(orig.getParentTask(), () -> new TaskId(randomAlphaOfLength(9), randomNonNegativeLong()))
            );
            case 1 -> copy.setTargetTaskId(
                randomValueOtherThan(orig.getTargetTaskId(), () -> new TaskId(randomAlphaOfLength(9), randomNonNegativeLong()))
            );
            case 2 -> copy.setTargetParentTaskId(
                randomValueOtherThan(orig.getTargetParentTaskId(), () -> new TaskId(randomAlphaOfLength(9), randomNonNegativeLong()))
            );
            case 3 -> copy.setNodes(
                randomArrayOtherThan(orig.getNodes(), () -> new String[] { randomAlphaOfLength(8), randomAlphaOfLength(8) })
            );
            case 4 -> copy.setActions(
                randomArrayOtherThan(
                    orig.getActions(),
                    () -> randomFrom(
                        new String[] { ReindexAction.NAME },
                        new String[] { UpdateByQueryAction.NAME },
                        new String[] { DeleteByQueryAction.NAME },
                        new String[] { ReindexAction.NAME, UpdateByQueryAction.NAME }
                    )
                )
            );
            case 5 -> copy.setTimeout(randomValueOtherThan(orig.getTimeout(), () -> TimeValue.timeValueMillis(between(1, 900_000))));
            case 6 -> copy.setRequestsPerSecond(
                randomValueOtherThan(orig.getRequestsPerSecond(), () -> (float) randomDoubleBetween(0.001d, 1000d, false))
            );
            default -> throw new AssertionError();
        }
        return new Wrapper(copy);
    }

    static final class Wrapper implements Writeable {
        private final RethrottleRequest request;

        Wrapper(RethrottleRequest request) {
            this.request = request;
        }

        Wrapper(StreamInput in) throws IOException {
            this.request = new RethrottleRequest(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            request.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Wrapper wrapper = (Wrapper) o;
            RethrottleRequest a = request;
            RethrottleRequest b = wrapper.request;
            return Objects.equals(a.getParentTask(), b.getParentTask())
                && Objects.equals(a.getTargetTaskId(), b.getTargetTaskId())
                && Objects.equals(a.getTargetParentTaskId(), b.getTargetParentTaskId())
                && Arrays.equals(a.getNodes(), b.getNodes())
                && Arrays.equals(a.getActions(), b.getActions())
                && Objects.equals(a.getTimeout(), b.getTimeout())
                && Float.compare(a.getRequestsPerSecond(), b.getRequestsPerSecond()) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                request.getParentTask(),
                request.getTargetTaskId(),
                request.getTargetParentTaskId(),
                Arrays.hashCode(request.getNodes()),
                Arrays.hashCode(request.getActions()),
                request.getTimeout(),
                request.getRequestsPerSecond()
            );
        }
    }
}
