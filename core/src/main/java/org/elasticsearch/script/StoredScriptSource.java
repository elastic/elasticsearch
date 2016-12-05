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

package org.elasticsearch.script;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StoredScriptSource extends AbstractDiffable<StoredScriptSource> implements Writeable, ToXContent {

    /**
     * Standard {@link ParseField} for outer level of stored script source.
     */
    public static final ParseField SCRIPT_PARSE_FIELD = new ParseField("script");

    /**
     * Standard {@link ParseField} for outer level of stored script source.
     */
    public static final ParseField TEMPLATE_PARSE_FIELD = new ParseField("template");

    /**
     * Standard {@link ParseField} for lang on the inner level.
     */
    public static final ParseField LANG_PARSE_FIELD = new ParseField("lang");

    /**
     * Standard {@link ParseField} for lang on the inner level.
     */
    public static final ParseField CODE_PARSE_FIELD = new ParseField("code");

    /**
     * Standard {@link ParseField} for options on the inner level.
     */
    public static final ParseField OPTIONS_PARSE_FIELD = new ParseField("options");

    private static final class MatcherSupplier implements ParseFieldMatcherSupplier {

        ParseFieldMatcher matcher = new ParseFieldMatcher(false);

        private MatcherSupplier() {}

        @Override
        public ParseFieldMatcher getParseFieldMatcher() {
            return matcher;
        }
    }

    /**
     * Helper class used by {@link ObjectParser} to store mutable {@link StoredScriptSource} variables and then
     * construct an immutable {@link StoredScriptSource} object based on parsed XContent.
     */
    private static final class Builder {
        private String lang;
        private String code;
        private Map<String, String> options;

        private Builder() {
            // This cannot default to an empty map because options are potentially added at multiple points.
            this.options = new HashMap<>();
        }

        private void setLang(String lang) {
            this.lang = lang;
        }

        /**
         * Since stored scripts can accept templates rather than just scripts, they must also be able
         * to handle template parsing, hence the need for custom parsing code.  Templates can
         * consist of either an {@link String} or a JSON object.  If a JSON object is discovered
         * then the content type option must also be saved as a compiler option.
         */
        private void setCode(XContentParser parser) {
            try {
                if (parser.currentToken() == Token.START_OBJECT) {
                    XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                    code = builder.copyCurrentStructure(parser).bytes().utf8ToString();
                    options.put(Script.CONTENT_TYPE_OPTION, parser.contentType().mediaType());
                } else {
                    code = parser.text();
                }
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }

        /**
         * Options may have already been added if a template was specified.
         * Appends the user-defined compiler options with the internal compiler options.
         */
        private void setOptions(Map<String, String> options) {
            this.options.putAll(options);
        }

        /**
         * Validates the parameters and creates an {@link StoredScriptSource}.
         */
        private StoredScriptSource build() {
            if (lang == null) {
                throw new IllegalArgumentException("must specify lang for stored script");
            } else if (lang.isEmpty()) {
                throw new IllegalArgumentException("lang cannot be empty");
            }

            if (code == null) {
                throw new IllegalArgumentException("must specify code for stored script");
            } else if (code.isEmpty()) {
                throw new IllegalArgumentException("code cannot be empty");
            }

            if (options.size() > 1 || options.size() == 1 && options.get(Script.CONTENT_TYPE_OPTION) == null) {
                throw new IllegalArgumentException("illegal compiler options [" + options + "] specified");
            }

            return new StoredScriptSource(lang, code, options);
        }
    }

    private static final ObjectParser<Builder, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("stored script source", Builder::new);

    static {
        // Defines the fields necessary to parse a Script as XContent using an ObjectParser.
        PARSER.declareString(Builder::setLang, LANG_PARSE_FIELD);
        PARSER.declareField(Builder::setCode, parser -> parser, CODE_PARSE_FIELD, ValueType.OBJECT_OR_STRING);
        PARSER.declareField(Builder::setOptions, XContentParser::mapStrings, OPTIONS_PARSE_FIELD, ValueType.OBJECT);
    }

    public static StoredScriptSource parse(String lang, BytesReference content) {
        try (XContentParser parser = XContentHelper.createParser(content)) {
            Token token = parser.nextToken();

            if (token != Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [{]");
            }

            token = parser.nextToken();

            if (token != Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + ", expected [" +
                    SCRIPT_PARSE_FIELD.getPreferredName() + ", " + TEMPLATE_PARSE_FIELD.getPreferredName());
            }

            String name = parser.currentName();

            if (SCRIPT_PARSE_FIELD.getPreferredName().equals(name)) {
                token = parser.nextToken();

                if (token == Token.VALUE_STRING) {
                    if (lang == null) {
                        throw new IllegalArgumentException("must specify lang as a url parameter when using the old stored script format");
                    }

                    return new StoredScriptSource(lang, parser.text(), Collections.emptyMap());
                } else if (token == Token.START_OBJECT) {
                    if (lang == null) {
                        return PARSER.apply(parser, new MatcherSupplier()).build();
                    } else {
                        try (XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType())) {
                            builder.copyCurrentStructure(parser);

                            return new StoredScriptSource(lang, builder.string(),
                                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, parser.contentType().mediaType()));
                        }
                    }

                } else {
                    throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [{, <code>]");
                }
            } else {
                if (lang == null) {
                    throw new IllegalArgumentException("unexpected stored script format");
                }

                if (TEMPLATE_PARSE_FIELD.getPreferredName().equals(name)) {
                    token = parser.nextToken();

                    if (token == Token.VALUE_STRING) {
                        return new StoredScriptSource(lang, parser.text(), Collections.emptyMap());
                    }
                }

                try (XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType())) {
                    if (token != Token.START_OBJECT) {
                        builder.startObject();
                        builder.copyCurrentStructure(parser);
                        builder.endObject();
                    } else {
                        builder.copyCurrentStructure(parser);
                    }

                    return new StoredScriptSource(lang, builder.string(),
                        Collections.singletonMap(Script.CONTENT_TYPE_OPTION, parser.contentType().mediaType()));
                }
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    public static StoredScriptSource fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, new MatcherSupplier()).build();
    }

    private final String lang;
    private final String code;
    private final Map<String, String> options;

    public StoredScriptSource() {
        this.lang = null;
        this.code = null;
        this.options = null;
    }

    public StoredScriptSource(String code) {
        this.lang = null;
        this.code = Objects.requireNonNull(code);
        this.options = null;
    }

    public StoredScriptSource(String lang, String code, Map<String, String> options) {
        this.lang = Objects.requireNonNull(lang);
        this.code = Objects.requireNonNull(code);
        this.options = Collections.unmodifiableMap(Objects.requireNonNull(options));
    }

    public StoredScriptSource(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            this.lang = in.readString();
            this.code = in.readString();
            @SuppressWarnings("unchecked")
            Map<String, String> options = (Map<String, String>)(Map)in.readMap();
            this.options = options;
        } else {
            this.lang = null;
            this.code = in.readBytesReference().utf8ToString();
            this.options = null;
        }
    }

    @Override
    public StoredScriptSource readFrom(StreamInput in) throws IOException {
        return new StoredScriptSource(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            out.writeString(lang);
            out.writeString(code);
            @SuppressWarnings("unchecked")
            Map<String, Object> options = (Map<String, Object>)(Map)this.options;
            out.writeMap(options);
        } else {
            out.writeBytesReference(new BytesArray(code));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LANG_PARSE_FIELD.getPreferredName(), lang);
        builder.field(CODE_PARSE_FIELD.getPreferredName(), code);
        builder.field(OPTIONS_PARSE_FIELD.getPreferredName(), options);
        builder.endObject();

        return builder;
    }

    public String getLang() {
        return lang;
    }

    public String getCode() {
        return code;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoredScriptSource that = (StoredScriptSource)o;

        if (lang != null ? !lang.equals(that.lang) : that.lang != null) return false;
        if (code != null ? !code.equals(that.code) : that.code != null) return false;
        return options != null ? options.equals(that.options) : that.options == null;

    }

    @Override
    public int hashCode() {
        int result = lang != null ? lang.hashCode() : 0;
        result = 31 * result + (code != null ? code.hashCode() : 0);
        result = 31 * result + (options != null ? options.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "StoredScriptSource{" +
            "lang='" + lang + '\'' +
            ", code='" + code + '\'' +
            ", options=" + options +
            '}';
    }
}
