/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for the cursor-aware autocomplete endpoint {@code POST /_esql/suggestions}.
 *
 * <p>{@code cursor} is a character offset into {@code query}.
 *
 * <p>{@code includeSampleValues} selects between two modes:
 * <ul>
 *     <li>{@code false} (default): coordinator-only, field-name/type completion, no data-node visit.</li>
 *     <li>{@code true}: additionally samples {@code values}/{@code range} from data nodes (deferred
 *     — see the suggestions API spec).</li>
 * </ul>
 */
public class EsqlSuggestionsRequest extends ActionRequest {

    public static final int DEFAULT_SIZE = 10;

    private String query;
    private int cursor;
    private int size = DEFAULT_SIZE;
    private boolean includeSampleValues = false;

    public EsqlSuggestionsRequest() {}

    public EsqlSuggestionsRequest(StreamInput in) throws IOException {
        super(in);
        this.query = in.readString();
        this.cursor = in.readVInt();
        this.size = in.readVInt();
        this.includeSampleValues = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(query == null ? "" : query);
        out.writeVInt(cursor);
        out.writeVInt(size);
        out.writeBoolean(includeSampleValues);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (query == null || query.isEmpty()) {
            validationException = addValidationError("[query] is required", validationException);
        } else if (cursor < 0 || cursor > query.length()) {
            validationException = addValidationError(
                "[cursor] must be within [0, " + query.length() + "], got [" + cursor + "]",
                validationException
            );
        }
        if (size <= 0) {
            validationException = addValidationError("[size] must be greater than 0, got [" + size + "]", validationException);
        }
        return validationException;
    }

    public String query() {
        return query;
    }

    public EsqlSuggestionsRequest query(String query) {
        this.query = query;
        return this;
    }

    public int cursor() {
        return cursor;
    }

    public EsqlSuggestionsRequest cursor(int cursor) {
        this.cursor = cursor;
        return this;
    }

    public int size() {
        return size;
    }

    public EsqlSuggestionsRequest size(int size) {
        this.size = size;
        return this;
    }

    public boolean includeSampleValues() {
        return includeSampleValues;
    }

    public EsqlSuggestionsRequest includeSampleValues(boolean includeSampleValues) {
        this.includeSampleValues = includeSampleValues;
        return this;
    }
}
