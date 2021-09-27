/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

class FieldCapabilitiesNodeResponse extends ActionResponse implements Writeable {
    private final String[] indices;
    private final List<FieldCapabilitiesFailure> failures;
    private final List<FieldCapabilitiesIndexResponse> indexResponses;

    FieldCapabilitiesNodeResponse(String[] indices,
                                  List<FieldCapabilitiesIndexResponse> indexResponses,
                                  List<FieldCapabilitiesFailure> failures) {
        this.indexResponses = Objects.requireNonNull(indexResponses);
        this.indices = indices;
        this.failures = failures;
    }

    FieldCapabilitiesNodeResponse(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indexResponses = in.readList(FieldCapabilitiesIndexResponse::new);
        this.failures = in.readList(FieldCapabilitiesFailure::new);
    }

    public List<FieldCapabilitiesFailure> getFailures() {
        return failures;
    }

    public List<FieldCapabilitiesIndexResponse> getIndexResponses() {
        return indexResponses;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        out.writeList(indexResponses);
        out.writeList(failures);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesNodeResponse that = (FieldCapabilitiesNodeResponse) o;
        return Arrays.equals(indices, that.indices) &&
            Objects.equals(indexResponses, that.indexResponses) &&
            Objects.equals(failures, that.failures);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash( indexResponses, failures);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }
}
