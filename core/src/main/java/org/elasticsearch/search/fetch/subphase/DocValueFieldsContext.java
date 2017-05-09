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
package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * All the required context to pull a field from the doc values.
 */
public class DocValueFieldsContext {

    public static final ParseField DOCVALUE_FIELDS_FIELD = new ParseField("docvalue_fields", "fielddata_fields");
    public static final ParseField DOCVALUE_FIELD_NAME = new ParseField("name");
    public static final ParseField DOCVALUE_FIELD_FORMAT = new ParseField("format");

    public static class Field implements Writeable {

        public static Field fromXContent(XContentParser parser, QueryParseContext context) throws IOException {
            if (parser.currentToken().isValue()) {
                return new Field(parser.text(), null);
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                String name = null;
                String format = null;
                String currentFieldName = null;
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (DOCVALUE_FIELD_NAME.match(currentFieldName)) {
                        name = parser.text();
                    } else if (DOCVALUE_FIELD_FORMAT.match(currentFieldName)) {
                        format = parser.textOrNull();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Unknown property under ["
                                + DOCVALUE_FIELDS_FIELD.getPreferredName() + "] : [" + currentFieldName
                                + "]", parser.getTokenLocation());
                    }
                }
                if (name == null) {
                    throw new ParsingException(parser.getTokenLocation(), "Missing name for ["
                            + DOCVALUE_FIELDS_FIELD.getPreferredName() + "]", parser.getTokenLocation());
                }
                return new Field(name, format);
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING +
                        "] or [" + XContentParser.Token.START_OBJECT + "] in [" + DOCVALUE_FIELDS_FIELD.getPreferredName() +
                        "] but found [" + parser.currentToken() + "]", parser.getTokenLocation());
            }
        }

        private final String name;
        private final String format;

        public Field(String name, String format) {
            this.name = Objects.requireNonNull(name);
            this.format = format;
        }

        public Field(StreamInput in) throws IOException {
            this(in.readString(), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeOptionalString(format);
        }

        @Override
        public String toString() {
            return "Field(name=" + name + ", format=" + format + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Field other = (Field) obj;
            return Objects.equals(name, other.name) && Objects.equals(format, other.format);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, format);
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            if (format == null) {
                builder.value(name);
            } else {
                builder.startObject();
                builder.field(DOCVALUE_FIELD_NAME.getPreferredName(), getName());
                builder.field(DOCVALUE_FIELD_FORMAT.getPreferredName(), getFormat());
                builder.endObject();
            }
        }

        /**
         * Return the name of the field to return.
         */
        public String getName() {
            return name;
        }

        /**
         * Return the format specification describing how the field should be
         * formatted. {@code null} means that the field defaults should be used.
         */
        public String getFormat() {
            return format;
        }
    }

    private final List<Field> fields;

    public DocValueFieldsContext(List<Field> fields) {
        this.fields = fields;
    }

    /**
     * Returns the required docvalue fields
     */
    public List<Field> fields() {
        return this.fields;
    }
}
