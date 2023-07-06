/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A request to gather terms for a given field matching a string prefix
 */
public class TermsEnumRequest extends BroadcastRequest<TermsEnumRequest> implements ToXContentObject {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = SearchRequest.DEFAULT_INDICES_OPTIONS;
    public static int DEFAULT_SIZE = 10;
    public static TimeValue DEFAULT_TIMEOUT = new TimeValue(1000);

    private String field;
    private String string = null;
    private String searchAfter = null;
    private int size = DEFAULT_SIZE;
    private boolean caseInsensitive;
    private QueryBuilder indexFilter;

    public TermsEnumRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    /**
     * Constructs a new term enum request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public TermsEnumRequest(String... indices) {
        super(indices);
        indicesOptions(DEFAULT_INDICES_OPTIONS);
        timeout(DEFAULT_TIMEOUT);
    }

    public TermsEnumRequest(TermsEnumRequest clone) {
        this.field = clone.field;
        this.string = clone.string;
        this.searchAfter = clone.searchAfter;
        this.caseInsensitive = clone.caseInsensitive;
        this.size = clone.size;
        this.indexFilter = clone.indexFilter;
        indices(clone.indices);
        indicesOptions(clone.indicesOptions());
        timeout(clone.timeout());
        setParentTask(clone.getParentTask());
    }

    public TermsEnumRequest(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        string = in.readOptionalString();
        searchAfter = in.readOptionalString();
        caseInsensitive = in.readBoolean();
        size = in.readVInt();
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(field);
        out.writeOptionalString(string);
        out.writeOptionalString(searchAfter);
        out.writeBoolean(caseInsensitive);
        out.writeVInt(size);
        out.writeOptionalNamedWriteable(indexFilter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("field", field);
        if (string != null) {
            builder.field("string", string);
        }
        if (searchAfter != null) {
            builder.field("search_after", searchAfter);
        }
        builder.field("size", size);
        builder.field("timeout", timeout());
        builder.field("case_insensitive", caseInsensitive);
        if (indexFilter != null) {
            builder.field("index_filter", indexFilter);
        }
        return builder.endObject();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (field == null) {
            validationException = ValidateActions.addValidationError("field cannot be null", validationException);
        }
        if (timeout() == null) {
            validationException = ValidateActions.addValidationError("Timeout cannot be null", validationException);
        } else {
            if (timeout().getSeconds() > 60) {
                validationException = ValidateActions.addValidationError("Timeout cannot be > 1 minute", validationException);
            }
        }
        return validationException;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    /**
     * The field to look inside for values
     */
    public TermsEnumRequest field(String field) {
        this.field = field;
        return this;
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
    public TermsEnumRequest string(String string) {
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
     * The string after which to find matching field values (enables pagination of previous request)
     */
    @Nullable
    public String searchAfter() {
        return searchAfter;
    }

    /**
     * The string after which to find matching field values (enables pagination of previous request)
     */
    public TermsEnumRequest searchAfter(String searchAfter) {
        this.searchAfter = searchAfter;
        return this;
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
    public TermsEnumRequest size(int size) {
        this.size = size;
        return this;
    }

    /**
     * If case insensitive matching is required
     */
    public TermsEnumRequest caseInsensitive(boolean caseInsensitive) {
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
    public TermsEnumRequest indexFilter(QueryBuilder indexFilter) {
        this.indexFilter = indexFilter;
        return this;
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    @Override
    public String toString() {
        return "["
            + Arrays.toString(indices)
            + "] field["
            + field
            + "], string["
            + string
            + "] "
            + " size="
            + size
            + " timeout="
            + timeout().getMillis()
            + " case_insensitive="
            + caseInsensitive
            + " indexFilter = "
            + indexFilter
            + " searchAfter["
            + searchAfter
            + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TermsEnumRequest that = (TermsEnumRequest) o;
        return size == that.size
            && caseInsensitive == that.caseInsensitive
            && Objects.equals(field, that.field)
            && Objects.equals(string, that.string)
            && Objects.equals(searchAfter, that.searchAfter)
            && Objects.equals(indexFilter, that.indexFilter)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions(), that.indicesOptions())
            && Objects.equals(timeout(), that.timeout());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field, string, searchAfter, size, caseInsensitive, indexFilter, indicesOptions(), timeout());
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }
}
