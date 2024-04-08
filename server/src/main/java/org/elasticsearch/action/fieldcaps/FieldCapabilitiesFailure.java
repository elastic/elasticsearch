/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FieldCapabilitiesFailure implements Writeable, ToXContentObject {

    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField FAILURE_FIELD = new ParseField("failure");
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

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldCapabilitiesFailure, Void> PARSER = new ConstructingObjectParser<>(
        "field_capabilities_failure",
        true,
        a -> {
            return new FieldCapabilitiesFailure(((List<String>) a[0]).toArray(String[]::new), (Exception) a[1]);
        }
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDICES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.currentToken(), p);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.nextToken(), p);
            Exception e = ElasticsearchException.failureFromXContent(p);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, p.nextToken(), p);
            return e;
        }, FAILURE_FIELD);
    }

    public static FieldCapabilitiesFailure fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
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
