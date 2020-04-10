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

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class NameOrDefinition implements Writeable, ToXContentFragment {
    // exactly one of these two members is not null
    public final String name;
    public final Settings definition;

    public NameOrDefinition(String name) {
        this.name = Objects.requireNonNull(name);
        this.definition = null;
    }

    public NameOrDefinition(Map<String, ?> definition) {
        this.name = null;
        Objects.requireNonNull(definition);
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(definition);
            this.definition = Settings.builder().loadFromSource(Strings.toString(builder), builder.contentType()).build();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse [" + definition + "]", e);
        }
    }

    public NameOrDefinition(StreamInput in) throws IOException {
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

    public static NameOrDefinition fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new NameOrDefinition(parser.text());
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return new NameOrDefinition(parser.map());
        }
        throw new XContentParseException(parser.getTokenLocation(),
            "Expected [VALUE_STRING] or [START_OBJECT], got " + parser.currentToken());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (definition == null) {
            builder.value(name);
        } else {
            builder.startObject();
            definition.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NameOrDefinition that = (NameOrDefinition) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, definition);
    }
}
