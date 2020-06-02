/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.support.MustacheTemplateEvaluator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Representation of a Mustache template for expressing one or more roles names in a {@link ExpressionRoleMapping}.
 */
public class TemplateRoleName implements ToXContentObject, Writeable {

    private static final ConstructingObjectParser<TemplateRoleName, Void> PARSER = new ConstructingObjectParser<>(
        "role-mapping-template", false, arr -> new TemplateRoleName((BytesReference) arr[0], (Format) arr[1]));

    static {
        PARSER.declareField(constructorArg(), TemplateRoleName::extractTemplate, Fields.TEMPLATE, ObjectParser.ValueType.OBJECT_OR_STRING);
        PARSER.declareField(optionalConstructorArg(), Format::fromXContent, Fields.FORMAT, ObjectParser.ValueType.STRING);
    }

    private final BytesReference template;
    private final Format format;

    public TemplateRoleName(BytesReference template, Format format) {
        this.template = template;
        this.format = format == null ? Format.STRING : format;
    }

    public TemplateRoleName(StreamInput in) throws IOException {
        this.template = in.readBytesReference();
        this.format = in.readEnum(Format.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(template);
        out.writeEnum(format);
    }

    public BytesReference getTemplate() {
        return template;
    }

    public Format getFormat() {
        return format;
    }

    public List<String> getRoleNames(ScriptService scriptService, ExpressionModel model) {
        try {
            final String evaluation = parseTemplate(scriptService, model.asMap());
            switch (format) {
                case STRING:
                    return Collections.singletonList(evaluation);
                case JSON:
                    return convertJsonToList(evaluation);
                default:
                    throw new IllegalStateException("Unsupported format [" + format + "]");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void validate(ScriptService scriptService) {
        try {
            parseTemplate(scriptService, Collections.emptyMap());
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private List<String> convertJsonToList(String evaluation) throws IOException {
        final XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE, evaluation);
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            return Collections.singletonList(parser.text());
        } else if (token == XContentParser.Token.START_ARRAY) {
            return parser.list().stream()
                .filter(Objects::nonNull)
                .map(o -> {
                    if (o instanceof String) {
                        return (String) o;
                    } else {
                        throw new XContentParseException(
                            "Roles array may only contain strings but found [" + o.getClass().getName() + "] [" + o + "]");
                    }
                }).collect(Collectors.toList());
        } else {
            throw new XContentParseException(
                "Roles template must generate a string or an array of strings, but found [" + token + "]");
        }
    }

    private String parseTemplate(ScriptService scriptService, Map<String, Object> parameters) throws IOException {
        final XContentParser parser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, template, XContentType.JSON);
        return MustacheTemplateEvaluator.evaluate(scriptService, parser, parameters);
    }

    private static BytesReference extractTemplate(XContentParser parser, Void ignore) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new BytesArray(parser.text());
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.generator().copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    static TemplateRoleName parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "template-" + format + "{" + template.utf8ToString() + "}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field(Fields.TEMPLATE.getPreferredName(), template.utf8ToString())
            .field(Fields.FORMAT.getPreferredName(), format.formatName())
            .endObject();
    }

    @Override
    public boolean isFragment() {
        return false;
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

    private interface Fields {
        ParseField TEMPLATE = new ParseField("template");
        ParseField FORMAT = new ParseField("format");
    }

    public enum Format {
        JSON, STRING;

        private static Format fromXContent(XContentParser parser) throws IOException {
            final XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.VALUE_STRING) {
                throw new XContentParseException(parser.getTokenLocation(),
                    "Expected [" + XContentParser.Token.VALUE_STRING + "] but found [" + token + "]");
            }
            final String text = parser.text();
            try {
                return Format.valueOf(text.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                String valueNames = Stream.of(values()).map(Format::formatName).collect(Collectors.joining(","));
                throw new XContentParseException(parser.getTokenLocation(),
                    "Invalid format [" + text + "] expected one of [" + valueNames + "]");
            }

        }

        public String formatName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
