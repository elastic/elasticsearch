/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FieldCapabilitiesFailure implements Writeable, ToXContentObject {

    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField FAILURE_FIELD = new ParseField("failure");
    private final List<String> indices;
    private final Exception exception;

    public FieldCapabilitiesFailure(String[] indices, Exception exception) {
        this.indices = new ArrayList<>(Arrays.asList(Objects.requireNonNull(indices)));
        this.exception = Objects.requireNonNull(exception);
    }

    public FieldCapabilitiesFailure(StreamInput in) throws IOException {
        this.indices = in.readStringCollectionAsList();
        this.exception = in.readException();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.stringListField(INDICES_FIELD.getPreferredName(), indices);
            builder.startObject(FAILURE_FIELD.getPreferredName());
            {
                ElasticsearchException.generateFailureXContent(builder, params, exception, true);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(indices);
        out.writeException(exception);
    }

    public String[] getIndices() {
        return indices.toArray(String[]::new);
    }

    public Exception getException() {
        return exception;
    }

    FieldCapabilitiesFailure addIndex(String index) {
        this.indices.add(index);
        return this;
    }
}
