/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Objects;

public class ResumeBulkByScrollResponseWireSerializingTests extends AbstractWireSerializingTestCase<
    ResumeBulkByScrollResponseWireSerializingTests.Wrapper> {

    @Override
    protected Writeable.Reader<Wrapper> instanceReader() {
        return Wrapper::new;
    }

    @Override
    protected Wrapper createTestInstance() {
        return new Wrapper(new ResumeBulkByScrollResponse(new TaskId(randomAlphaOfLength(8), randomNonNegativeLong())));
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        TaskId origId = instance.response.getTaskId();
        TaskId newId = randomValueOtherThan(origId, () -> new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()));
        return new Wrapper(new ResumeBulkByScrollResponse(newId));
    }

    static final class Wrapper implements Writeable {
        private final ResumeBulkByScrollResponse response;

        Wrapper(ResumeBulkByScrollResponse response) {
            this.response = response;
        }

        Wrapper(StreamInput in) throws IOException {
            this.response = new ResumeBulkByScrollResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            response.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Wrapper wrapper = (Wrapper) o;
            return Objects.equals(response.getTaskId(), wrapper.response.getTaskId());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(response.getTaskId());
        }
    }
}
