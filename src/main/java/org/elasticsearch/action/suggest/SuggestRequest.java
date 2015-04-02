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
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to get suggestions for corrections of phrases. Best created with
 * {@link org.elasticsearch.client.Requests#suggestRequest(String...)}.
 * <p/>
 * <p>The request requires the suggest query source to be set either using
 * {@link #suggest(org.elasticsearch.common.bytes.BytesReference)} / {@link #suggest(org.elasticsearch.common.bytes.BytesReference)}
 * or by using {@link #suggest(org.elasticsearch.search.suggest.SuggestBuilder)}
 * (Best created using the {link @org.elasticsearch.search.suggest.SuggestBuilders)}).
 *
 * @see SuggestResponse
 * @see org.elasticsearch.client.Client#suggest(SuggestRequest)
 * @see org.elasticsearch.client.Requests#suggestRequest(String...)
 * @see org.elasticsearch.search.suggest.SuggestBuilders
 */
public final class SuggestRequest extends BroadcastOperationRequest<SuggestRequest> {

    @Nullable
    private String routing;

    @Nullable
    private String preference;

    private BytesReference suggestSource;

    SuggestRequest() {
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
    public BytesReference suggest() {
        return suggestSource;
    }
    
    /**
     * set a new source for the suggest query  
     */
    public SuggestRequest suggest(BytesReference suggestSource) {
        this.suggestSource = suggestSource;
        return this;
    }

    /**
     * set a new source using a {@link org.elasticsearch.search.suggest.SuggestBuilder}
     * for phrase and term suggestion lookup
     */
    public SuggestRequest suggest(SuggestBuilder suggestBuilder) {
        return suggest(suggestBuilder.buildAsBytes(Requests.CONTENT_TYPE));
    }

    /**
     * set a new source using a {@link org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder}
     * for completion suggestion lookup
     */
    public SuggestRequest suggest(SuggestBuilder.SuggestionBuilder suggestionBuilder) {
        return suggest(suggestionBuilder.buildAsBytes(Requests.CONTENT_TYPE));
    }

    public SuggestRequest suggest(String source) {
        return suggest(new BytesArray(source));
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
        suggest(in.readBytesReference());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeBytesReference(suggestSource);
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(suggestSource, false);
        } catch (Exception e) {
            // ignore
        }
        return "[" + Arrays.toString(indices) + "]" + ", suggestSource[" + sSource + "]";
    }
}
