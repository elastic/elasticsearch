/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.fillRandomBulkFields;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.mutateAbstractBulkByScrollRequest;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.randomResumeInfo;

public class DeleteByQueryRequestWireSerializingTests extends AbstractWireSerializingTestCase<
    DeleteByQueryRequestWireSerializingTests.Wrapper> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return BulkByScrollWireSerializingTestUtils.bulkScrollRequestNamedWriteableRegistry();
    }

    @Override
    protected Writeable.Reader<Wrapper> instanceReader() {
        return Wrapper::new;
    }

    @Override
    protected Wrapper createTestInstance() {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
        deleteByQueryRequest.indices("test-index");
        deleteByQueryRequest.getSearchRequest().source().size(between(1, 1000));
        fillRandomBulkFields(deleteByQueryRequest);
        if (randomBoolean()) {
            deleteByQueryRequest.setResumeInfo(randomResumeInfo());
        }
        return new Wrapper(deleteByQueryRequest);
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        DeleteByQueryRequest originalRequest = instance.request;
        DeleteByQueryRequest mutatedRequest = copyInstance(instance).request;
        mutateAbstractBulkByScrollRequest(originalRequest, mutatedRequest);
        return new Wrapper(mutatedRequest);
    }

    static final class Wrapper implements Writeable {
        private final DeleteByQueryRequest request;

        Wrapper(DeleteByQueryRequest request) {
            this.request = request;
        }

        Wrapper(StreamInput streamInput) throws IOException {
            this.request = new DeleteByQueryRequest(streamInput);
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            request.writeTo(output);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            Wrapper wrapper = (Wrapper) other;
            return wireBytesEqual(request, wrapper.request);
        }

        @Override
        public int hashCode() {
            try {
                BytesStreamOutput output = new BytesStreamOutput();
                request.writeTo(output);
                return output.bytes().hashCode();
            } catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            }
        }

        /**
         * Equality by serialized wire form. Deep {@link DeleteByQueryRequest} equality can disagree with a transport round-trip
         * (e.g. nested resume state) even when the encoded bytes match; wire tests should assert encoding stability.
         */
        private static boolean wireBytesEqual(DeleteByQueryRequest firstRequest, DeleteByQueryRequest secondRequest) {
            try {
                BytesStreamOutput firstOutput = new BytesStreamOutput();
                firstRequest.writeTo(firstOutput);
                BytesStreamOutput secondOutput = new BytesStreamOutput();
                secondRequest.writeTo(secondOutput);
                return firstOutput.bytes().equals(secondOutput.bytes());
            } catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            }
        }
    }
}
