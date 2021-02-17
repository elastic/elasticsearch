/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termenum.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to gather terms for a given field matching a string prefix
 */
public class TermEnumRequest extends BroadcastRequest<TermEnumRequest> implements ToXContentObject {

    public static int DEFAULT_SIZE = 10;
    public static int DEFAULT_TIMEOUT_MILLIS = 1000;

    private String field;
    private String string;
    private int size = DEFAULT_SIZE;
    private boolean caseInsensitive;
    private boolean sortByPopularity;
    long taskStartTimeMillis;
    private QueryBuilder indexFilter;

    public TermEnumRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public TermEnumRequest(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        string = in.readString();
        caseInsensitive = in.readBoolean();
        sortByPopularity = in.readBoolean();
        size = in.readVInt();
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    /**
     * Constructs a new term enum request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public TermEnumRequest(String... indices) {
        super(indices);
        indicesOptions(IndicesOptions.fromOptions(false, false, true, false));
        timeout(new TimeValue(DEFAULT_TIMEOUT_MILLIS));
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (field == null) {
            validationException = ValidateActions.addValidationError("field cannot be null", validationException);
        }
        return validationException;
    }

    /**
     * The field to look inside for values
     */
    public void field(String field) {
        this.field = field;
    }

    /**
     * Indicates if detailed information about query is requested
     */
    public String field() {
        return field;
    }

    /**
     * The string required in matching field values
     */
    public void string(String string) {
        this.string = string;
    }

    /**
     * The string required in matching field values
     */
    public String string() {
        return string;
    }

    /**
     * sort terms by popularity
     */
    public boolean sortByPopularity() {
        return sortByPopularity;
    }

    /**
     * sort terms by popularity
     */
    public void sortByPopularity(boolean sortByPopularity) {
        this.sortByPopularity = sortByPopularity;
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
    public void size(int size) {
        this.size = size;
    }

    /**
     * TThe max time in milliseconds to spend gathering terms
     */
    public void timeoutInMillis(int timeout) {
        timeout(new TimeValue(timeout));
    }

    /**
     * If case insensitive matching is required
     */
    public void caseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
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
    public void indexFilter(QueryBuilder indexFilter) {
        this.indexFilter = indexFilter;
    }    
    
    public QueryBuilder indexFilter() {
        return indexFilter;
    }    
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(field);
        out.writeString(string);
        out.writeBoolean(caseInsensitive);
        out.writeBoolean(sortByPopularity);
        out.writeVInt(size);
        out.writeOptionalNamedWriteable(indexFilter);
    }

    @Override
    public String toString() {
        return "[" + Arrays.toString(indices) + "] field[" + field + "], string[" + string + "] "  + " size=" + size + " timeout="
            + timeout().getMillis()  + " sort_by_popularity = " + sortByPopularity + " case_insensitive="
            + caseInsensitive + " indexFilter = "+ indexFilter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("field", field);
        builder.field("string", string);
        builder.field("size", size);
        builder.field("timeout", timeout().getMillis());
        builder.field("case_insensitive", caseInsensitive);
        builder.field("sort_by_popularity", sortByPopularity);
        if (indexFilter != null) {
            builder.field("index_filter", indexFilter);
        }        
        return builder.endObject();
    }
}
