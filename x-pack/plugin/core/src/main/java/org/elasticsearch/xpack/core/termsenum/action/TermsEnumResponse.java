/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The response of the _terms_enum action.
 */
public class TermsEnumResponse extends BroadcastResponse {

    public static final String TERMS_FIELD = "terms";
    public static final String COMPLETE_FIELD = "complete";

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<TermsEnumResponse, Void> PARSER = new ConstructingObjectParser<>(
        "term_enum_results",
        true,
        arg -> {
            BroadcastResponse response = (BroadcastResponse) arg[0];
            return new TermsEnumResponse(
                (List<String>) arg[1],
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getFailedShards(),
                Arrays.asList(response.getShardFailures()),
                (Boolean) arg[2]
            );
        }
    );
    static {
        declareBroadcastFields(PARSER);
        PARSER.declareStringArray(optionalConstructorArg(), new ParseField(TERMS_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(COMPLETE_FIELD));
    }

    private final List<String> terms;

    private boolean complete;

    TermsEnumResponse(StreamInput in) throws IOException {
        super(in);
        terms = in.readStringList();
        complete = in.readBoolean();
    }

    public TermsEnumResponse(
        List<String> terms,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        boolean complete
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.terms = terms == null ? Collections.emptyList() : terms;
        this.complete = complete;
    }

    /**
     * The list of terms.
     */
    public List<String> getTerms() {
        return terms;
    }

    public boolean isComplete() {
        return complete;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(terms);
        out.writeBoolean(complete);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TERMS_FIELD);
        if (getTerms() != null && getTerms().isEmpty() == false) {
            for (String term : getTerms()) {
                builder.value(term);
            }
        }
        builder.endArray();
        builder.field(COMPLETE_FIELD, complete);
    }

    public static TermsEnumResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
