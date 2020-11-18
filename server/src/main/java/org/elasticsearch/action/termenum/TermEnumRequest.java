/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.termenum;

import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to gather terms for a given field matching a pattern
 */
public class TermEnumRequest extends BroadcastRequest<TermEnumRequest> implements ToXContentObject {

    public static int DEFAULT_SIZE = 10;
    public static int DEFAULT_TIMEOUT_MILLIS = 1000;

    private String field;
    private String pattern;
    private int minShardDocFreq;
    private int size = DEFAULT_SIZE;
    private int timeout = DEFAULT_TIMEOUT_MILLIS;
    private boolean leadingWildcard;
    private boolean traillingWildcard = true;
    private boolean caseInsensitive;
    private boolean useRegexpSyntax;
    private boolean sortByPopularity;

    long nowInMillis;

    public TermEnumRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public TermEnumRequest(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        pattern = in.readString();
        leadingWildcard = in.readBoolean();
        traillingWildcard = in.readBoolean();
        caseInsensitive = in.readBoolean();
        useRegexpSyntax = in.readBoolean();
        sortByPopularity = in.readBoolean();
        minShardDocFreq = in.readVInt();
        size = in.readVInt();
        timeout = in.readVInt();
    }

    /**
     * Constructs a new term enum request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public TermEnumRequest(String... indices) {
        super(indices);
        indicesOptions(IndicesOptions.fromOptions(false, false, true, false));
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (field == null) {
            validationException = ValidateActions.addValidationError("field cannot be null", validationException);
        } else if (useRegexpSyntax) {
            try {
                new RegExp(pattern);
            } catch (IllegalArgumentException ia) {
                validationException = ValidateActions.addValidationError(
                    "Bad regular expression: [" + pattern + "] " + ia.getMessage(),
                    validationException
                );
            }
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
     * The pattern required in matching field values
     */
    public void pattern(String pattern) {
        this.pattern = pattern;
    }

    /**
     * The pattern required in matching field values
     */
    public String pattern() {
        return pattern;
    }

    /**
     * True if characters are allowed before pattern in string
     */
    public void leadingWildcard(boolean leadingWildcard) {
        this.leadingWildcard = leadingWildcard;
    }

    /**
     * If characters are allowed before pattern in string
     */
    public boolean leadingWildcard() {
        return leadingWildcard;
    }

    /**
     * True if characters are allowed after pattern in string
     */
    public void traillingWildcard(boolean traillingWildcard) {
        this.traillingWildcard = traillingWildcard;
    }

    /**
     * If characters are allowed after pattern in string
     */
    public boolean traillingWildcard() {
        return traillingWildcard;
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
     * The minimum number of docs on a shard having a term, defaults to 1
     */
    public int minShardDocFreq() {
        return minShardDocFreq;
    }

    /**
     * The minimum number of docs on a shard having a term, defaults to 1
     */
    public void minShardDocFreq(int minShardDocFreq) {
        this.minShardDocFreq = minShardDocFreq;
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
     *  The max time in milliseconds to spend gathering terms
     */
    public int timeout() {
        return timeout;
    }

    /**
     * TThe max time in milliseconds to spend gathering terms
     */
    public void timeout(int timeout) {
        this.timeout = timeout;
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
     * If regex syntax is used
     */
    public void useRegexpSyntax(boolean useRegexSyntax) {
        this.useRegexpSyntax = useRegexSyntax;
    }

    /**
     * If case insensitive matching is required
     */
    public boolean useRegexpSyntax() {
        return useRegexpSyntax;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(field);
        out.writeString(pattern);
        out.writeBoolean(leadingWildcard);
        out.writeBoolean(traillingWildcard);
        out.writeBoolean(caseInsensitive);
        out.writeBoolean(useRegexpSyntax);
        out.writeBoolean(sortByPopularity);
        out.writeVInt(minShardDocFreq);
        out.writeVInt(size);
        out.writeVInt(timeout);

    }

    @Override
    public String toString() {
        return "[" + Arrays.toString(indices) + "] field[" + field + "], pattern[" + pattern + "] " + " leading_wildcard=" + leadingWildcard
            + " trailling_wildcard=" + traillingWildcard + " min_shard_doc_freq=" + minShardDocFreq + " size=" + size + " timeout="
            + timeout + " use_regexp_syntax = " + useRegexpSyntax + " sort_by_popularity = " + sortByPopularity + " case_insensitive="
            + caseInsensitive;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("field", field);
        builder.field("pattern", pattern);
        builder.field("leading_wildcard", leadingWildcard);
        builder.field("trailling_wildcard", traillingWildcard);
        builder.field("min_shard_doc_freq", minShardDocFreq);
        builder.field("size", size);
        builder.field("timeout", timeout);
        builder.field("case_insensitive", caseInsensitive);
        builder.field("sort_by_popularity", sortByPopularity);
        builder.field("use_regexp_syntax", useRegexpSyntax);
        return builder.endObject();
    }
}
