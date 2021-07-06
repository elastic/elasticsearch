/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.fieldsenum.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to gather terms for a given field matching a string prefix
 */
public class FieldsEnumRequest extends BroadcastRequest<FieldsEnumRequest> implements ToXContentObject {

    public static int DEFAULT_SIZE = 10;
    public static TimeValue DEFAULT_TIMEOUT = new TimeValue(1000);

    private String string = null;
    private int size = DEFAULT_SIZE;
    private boolean caseInsensitive;
    private QueryBuilder indexFilter;

    public FieldsEnumRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    /**
     * Constructs a new term enum request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public FieldsEnumRequest(String... indices) {
        super(indices);
        indicesOptions(IndicesOptions.fromOptions(false, false, true, false));
        timeout(DEFAULT_TIMEOUT);
    }

    public FieldsEnumRequest(FieldsEnumRequest clone) {
        this.string = clone.string;
        this.caseInsensitive = clone.caseInsensitive;
        this.size = clone.size;
        this.indexFilter = clone.indexFilter;
        indices(clone.indices);
        indicesOptions(clone.indicesOptions());
        timeout(clone.timeout());
        setParentTask(clone.getParentTask());
    }

    public FieldsEnumRequest(StreamInput in) throws IOException {
        super(in);
        string = in.readOptionalString();
        caseInsensitive = in.readBoolean();
        size = in.readVInt();
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(string);
        out.writeBoolean(caseInsensitive);
        out.writeVInt(size);
        out.writeOptionalNamedWriteable(indexFilter);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (timeout() == null) {
            validationException = ValidateActions.addValidationError("Timeout cannot be null", validationException);
        } else {
            if (timeout().getSeconds() > 60) {
                validationException = ValidateActions.addValidationError("Timeout cannot be > 1 minute",
                    validationException);
            }
        }
        return validationException;
    }


    /**
     * The string required in matching field values
     */
    public FieldsEnumRequest string(String string) {
        this.string = string;
        return this;
    }

    /**
     * The string required in matching field values
     */
    @Nullable
    public String string() {
        return string;
    }

    /**
     *  The number of terms to return
     */
    public int size() {
        return size;
    }

    /**
     * The number of terms to return
     */
    public FieldsEnumRequest size(int size) {
        this.size = size;
        return this;
    }

    /**
     * If case insensitive matching is required
     */
    public FieldsEnumRequest caseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
        return this;
    }

    /**
     * If case insensitive matching is required
     */
    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    /**
     * Allows to filter shards if the provided {@link QueryBuilder} rewrites to `match_none`.
     */
    public FieldsEnumRequest indexFilter(QueryBuilder indexFilter) {
        this.indexFilter = indexFilter;
        return this;
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    @Override
    public String toString() {
        return "[" + Arrays.toString(indices) + "], string[" + string + "] "  + " size=" + size + " timeout="
            + timeout().getMillis() + " case_insensitive="
            + caseInsensitive + " indexFilter = "+ indexFilter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (string != null) {
            builder.field("string", string);
        }
        builder.field("size", size);
        builder.field("timeout", timeout().getMillis());
        builder.field("case_insensitive", caseInsensitive);
        if (indexFilter != null) {
            builder.field("index_filter", indexFilter);
        }
        return builder.endObject();
    }
}
