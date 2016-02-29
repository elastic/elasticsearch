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

package org.elasticsearch.action.suggest;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A request to get suggestions for corrections of phrases. Best created with
 * {@link org.elasticsearch.client.Requests#suggestRequest(String...)}.
 * <p>
 * The request requires the suggest query source to be set using
 * {@link #suggest(org.elasticsearch.search.suggest.SuggestBuilder)}
 *
 * @see SuggestResponse
 * @see org.elasticsearch.client.Client#suggest(SuggestRequest)
 * @see org.elasticsearch.client.Requests#suggestRequest(String...)
 * @see org.elasticsearch.search.suggest.SuggestBuilders
 */
public final class SuggestRequest extends BroadcastRequest<SuggestRequest> {

    @Nullable
    private String routing;

    @Nullable
    private String preference;

    private SuggestBuilder suggestSource;

    public SuggestRequest() {
    }

    /**
     * Constructs a new suggest request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public SuggestRequest(String... indices) {
        super(indices);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        return validationException;
    }

    /**
     * The Phrase to get correction suggestions for
     */
    public SuggestBuilder suggest() {
        return suggestSource;
    }

    /**
     * set a new source for the suggest query
     */
    public SuggestRequest suggest(SuggestBuilder suggestSource) {
        Objects.requireNonNull(suggestSource, "suggestSource must not be null");
        this.suggestSource = suggestSource;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SuggestRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public SuggestRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    public SuggestRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        suggest(SuggestBuilder.PROTOTYPE.readFrom(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Objects.requireNonNull(suggestSource, "suggestSource must not be null");
        super.writeTo(out);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        suggestSource.writeTo(out);
    }

    @Override
    public String toString() {
        Objects.requireNonNull(suggestSource, "suggestSource must not be null");
        String sSource = "_na_";
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder = suggestSource.toXContent(builder, ToXContent.EMPTY_PARAMS);
            sSource = builder.string();
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + ", suggestSource[" + sSource + "]";
    }
}
