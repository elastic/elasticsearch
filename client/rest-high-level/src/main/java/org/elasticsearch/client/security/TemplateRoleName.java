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

package org.elasticsearch.client.security;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A role name that uses a dynamic template.
 */
public class TemplateRoleName implements ToXContentObject {

    private static final ConstructingObjectParser<TemplateRoleName, Void> PARSER = new ConstructingObjectParser<>("template-role-name",
        true, args -> new TemplateRoleName((String) args[0], (Format) args[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Fields.TEMPLATE);
        PARSER.declareField(optionalConstructorArg(), Format::fromXContent, Fields.FORMAT, ObjectParser.ValueType.STRING);
    }
    private final String template;
    private final Format format;

    public TemplateRoleName(String template, Format format) {
        this.template = Objects.requireNonNull(template);
        this.format = Objects.requireNonNull(format);
    }

    public TemplateRoleName(Map<String, Object> template, Format format) throws IOException {
        this(Strings.toString(XContentBuilder.builder(XContentType.JSON.xContent()).map(template)), format);
    }

    public String getTemplate() {
        return template;
    }

    public Format getFormat() {
        return format;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TemplateRoleName that = (TemplateRoleName) o;
        return Objects.equals(this.template, that.template) &&
            this.format == that.format;
    }

    @Override
    public int hashCode() {
        return Objects.hash(template, format);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field(Fields.TEMPLATE.getPreferredName(), template)
            .field(Fields.FORMAT.getPreferredName(), format.name().toLowerCase(Locale.ROOT))
            .endObject();
    }

    static TemplateRoleName fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        return PARSER.parse(parser, null);
    }


    public enum Format {
        STRING, JSON;

        private static Format fromXContent(XContentParser parser) throws IOException {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser::getTokenLocation);
            return Format.valueOf(parser.text().toUpperCase(Locale.ROOT));
        }
    }

    public interface Fields {
        ParseField TEMPLATE = new ParseField("template");
        ParseField FORMAT = new ParseField("format");
    }

}
