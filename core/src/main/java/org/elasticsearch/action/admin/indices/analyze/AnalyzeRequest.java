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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to analyze a text associated with a specific index. Allow to provide
 * the actual analyzer name to perform the analysis with.
 */
public class AnalyzeRequest extends SingleShardRequest<AnalyzeRequest> {

    private String[] text;

    private String analyzer;

    private NameOrDefinition tokenizer;

    private final List<NameOrDefinition> tokenFilters = new ArrayList<>();

    private final List<NameOrDefinition> charFilters = new ArrayList<>();

    private String field;

    private boolean explain = false;

    private String[] attributes = Strings.EMPTY_ARRAY;

    public static class NameOrDefinition implements Writeable {
        // exactly one of these two members is not null
        public final String name;
        public final Settings definition;

        NameOrDefinition(String name) {
            this.name = Objects.requireNonNull(name);
            this.definition = null;
        }

        NameOrDefinition(Map<String, ?> definition) {
            this.name = null;
            Objects.requireNonNull(definition);
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(definition);
                this.definition = Settings.builder().loadFromSource(builder.string()).build();
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to parse [" + definition + "]", e);
            }
        }

        NameOrDefinition(StreamInput in) throws IOException {
            name = in.readOptionalString();
            if (in.readBoolean()) {
                definition = Settings.readSettingsFromStream(in);
            } else {
                definition = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(name);
            boolean isNotNullDefinition = this.definition != null;
            out.writeBoolean(isNotNullDefinition);
            if (isNotNullDefinition) {
                Settings.writeSettingsToStream(definition, out);
            }
        }
    }

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
        this.tokenizer = new NameOrDefinition(tokenizer);
        return this;
    }

    public AnalyzeRequest tokenizer(Map<String, ?> tokenizer) {
        this.tokenizer = new NameOrDefinition(tokenizer);
        return this;
    }

    public NameOrDefinition tokenizer() {
        return this.tokenizer;
    }

    public AnalyzeRequest addTokenFilter(String tokenFilter) {
        this.tokenFilters.add(new NameOrDefinition(tokenFilter));
        return this;
    }

    public AnalyzeRequest addTokenFilter(Map<String, ?> tokenFilter) {
        this.tokenFilters.add(new NameOrDefinition(tokenFilter));
        return this;
    }

    public List<NameOrDefinition> tokenFilters() {
        return this.tokenFilters;
    }

    public AnalyzeRequest addCharFilter(Map<String, ?> charFilter) {
        this.charFilters.add(new NameOrDefinition(charFilter));
        return this;
    }

    public AnalyzeRequest addCharFilter(String charFilter) {
        this.charFilters.add(new NameOrDefinition(charFilter));
        return this;
    }
    public List<NameOrDefinition> charFilters() {
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
        tokenizer = in.readOptionalWriteable(NameOrDefinition::new);
        tokenFilters.addAll(in.readList(NameOrDefinition::new));
        charFilters.addAll(in.readList(NameOrDefinition::new));
        field = in.readOptionalString();
        explain = in.readBoolean();
        attributes = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(text);
        out.writeOptionalString(analyzer);
        out.writeOptionalWriteable(tokenizer);
        out.writeList(tokenFilters);
        out.writeList(charFilters);
        out.writeOptionalString(field);
        out.writeBoolean(explain);
        out.writeStringArray(attributes);
    }
}
