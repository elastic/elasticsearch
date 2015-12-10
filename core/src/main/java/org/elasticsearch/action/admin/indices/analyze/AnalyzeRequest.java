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
package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to analyze a text associated with a specific index. Allow to provide
 * the actual analyzer name to perform the analysis with.
 */
public class AnalyzeRequest extends SingleShardRequest<AnalyzeRequest> {

    private String[] text;

    private String analyzer;

    private String tokenizer;

    private String[] tokenFilters = Strings.EMPTY_ARRAY;

    private String[] charFilters = Strings.EMPTY_ARRAY;

    private String field;

    private boolean explain = false;

    private String[] attributes = Strings.EMPTY_ARRAY;

    public AnalyzeRequest() {
    }

    /**
     * Constructs a new analyzer request for the provided index.
     *
     * @param index The text to analyze
     */
    public AnalyzeRequest(String index) {
        this.index(index);
    }

    public String[] text() {
        return this.text;
    }

    public AnalyzeRequest text(String... text) {
        this.text = text;
        return this;
    }

    public AnalyzeRequest analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return this.analyzer;
    }

    public AnalyzeRequest tokenizer(String tokenizer) {
        this.tokenizer = tokenizer;
        return this;
    }

    public String tokenizer() {
        return this.tokenizer;
    }

    public AnalyzeRequest tokenFilters(String... tokenFilters) {
        if (tokenFilters == null) {
            throw new IllegalArgumentException("token filters must not be null");
        }
        this.tokenFilters = tokenFilters;
        return this;
    }

    public String[] tokenFilters() {
        return this.tokenFilters;
    }

    public AnalyzeRequest charFilters(String... charFilters) {
        if (charFilters == null) {
            throw new IllegalArgumentException("char filters must not be null");
        }
        this.charFilters = charFilters;
        return this;
    }

    public String[] charFilters() {
        return this.charFilters;
    }

    public AnalyzeRequest field(String field) {
        this.field = field;
        return this;
    }

    public String field() {
        return this.field;
    }

    public AnalyzeRequest explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public boolean explain() {
        return this.explain;
    }

    public AnalyzeRequest attributes(String... attributes) {
        if (attributes == null) {
            throw new IllegalArgumentException("attributes must not be null");
        }
        this.attributes = attributes;
        return this;
    }

    public String[] attributes() {
        return this.attributes;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (text == null || text.length == 0) {
            validationException = addValidationError("text is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        text = in.readStringArray();
        analyzer = in.readOptionalString();
        tokenizer = in.readOptionalString();
        tokenFilters = in.readStringArray();
        charFilters = in.readStringArray();
        field = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_2_2_0)) {
            explain = in.readBoolean();
            attributes = in.readStringArray();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(text);
        out.writeOptionalString(analyzer);
        out.writeOptionalString(tokenizer);
        out.writeStringArray(tokenFilters);
        out.writeStringArray(charFilters);
        out.writeOptionalString(field);
        if (out.getVersion().onOrAfter(Version.V_2_2_0)) {
            out.writeBoolean(explain);
            out.writeStringArray(attributes);
        }
    }
}
