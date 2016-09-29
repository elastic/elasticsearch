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

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.script.Script.ScriptType.FILE;
import static org.elasticsearch.script.Script.ScriptType.INLINE;
import static org.elasticsearch.script.Script.ScriptType.STORED;

public final class Script {

    public enum ScriptType {

        INLINE( 0 , "inline" , new ParseField("inline")       , false ),
        STORED( 1 , "stored" , new ParseField("stored", "id") , false ),
        FILE(   2 , "file"   , new ParseField("file")         , true  );

        public static ScriptType fromValue(int value) throws IOException {
            for (ScriptType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }

            throw new IllegalArgumentException("unexpected script type [" + value + "], " + "expected one of [" +
                FILE.value + "(" + FILE.name + "), " +
                STORED.value + "(" + STORED.name + "), " +
                INLINE.value + "(" + INLINE.name + ")]");
        }

        public static ScriptType fromName(String name) throws IOException {
            for (ScriptType type : values()) {
                if (type.name.equals(name)) {
                    return type;
                }
            }

            throw new IllegalArgumentException("unexpected script type [" + name + "], " + "expected one of " +
                "[" + FILE.name + ", " + STORED.name + ", " + INLINE.name + ")]");
        }

        public final int value;
        public final String name;
        public final ParseField parse;
        public final boolean enabled;

        ScriptType(int value, String name, ParseField parse, boolean enabled) {
            this.value = value;
            this.name = name;
            this.parse = parse;
            this.enabled = enabled;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static final class ScriptField {

        public static final ParseField SCRIPT = new ParseField("script");
        public static final ParseField TEMPLATE = new ParseField("template");
        public static final ParseField BIND = new ParseField("bind");
        public static final ParseField LANG = new ParseField("lang");
        public static final ParseField CODE = new ParseField("code", "script");
        public static final ParseField PARAMS = new ParseField("params");

        private ScriptField() {}
    }

    public static final class StoredScriptSource extends AbstractDiffable<StoredScriptSource> implements ToXContent {

        private static final class StoredScriptSourceParserContext implements ParseFieldMatcherSupplier {

            private final ParseFieldMatcher parseFieldMatcher;

            StoredScriptSourceParserContext() {
                this.parseFieldMatcher = new ParseFieldMatcher(true);
            }

            @Override
            public ParseFieldMatcher getParseFieldMatcher() {
                return parseFieldMatcher;
            }
        }

        private static final ConstructingObjectParser<StoredScriptSource, StoredScriptSourceParserContext> CONSTRUCTOR =
            new ConstructingObjectParser<>("StoredScriptSource", source ->
                new StoredScriptSource(
                    false,
                    (String)source[0],
                    source[1] == null ? Script.DEFAULT_SCRIPT_LANG : (String)source[1],
                    source[2] == null ? UnknownScriptBinding.NAME : (String)source[2],
                    Collections.emptyMap()
                )
            );

        static {
                CONSTRUCTOR.declareString(optionalConstructorArg(), ScriptField.BIND);
                CONSTRUCTOR.declareString(optionalConstructorArg(), ScriptField.LANG);
                CONSTRUCTOR.declareString(constructorArg(), ScriptField.CODE);
        }

        public static StoredScriptSource parse(XContentParser parser) throws IOException {
            if (parser.currentToken() == null) {
                parser.nextToken();
            }

            if (parser.currentToken() != Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                    "unexpected token [" + parser.currentToken() + "], expected start object [{]");
            }

            if (parser.nextToken() == Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                    "unexpected token [" + parser.currentToken() + "], expected [<code>]");
            }

            StoredScriptSource source;

            if (parser.currentToken() == Token.FIELD_NAME && ScriptField.SCRIPT.getPreferredName().equals(parser.currentName())) {
                if (parser.nextToken() == Token.VALUE_STRING) {
                    source = new StoredScriptSource(
                        false, UnknownScriptBinding.NAME, Script.DEFAULT_SCRIPT_LANG, parser.text(), Collections.emptyMap());
                } else if (parser.currentToken() == Token.START_OBJECT) {
                    source = CONSTRUCTOR.apply(parser, new StoredScriptSourceParserContext());
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "unexpected token [" + parser.currentToken() + "], expected [<code>]");
                }
            } else if (parser.currentToken() == Token.FIELD_NAME && ScriptField.TEMPLATE.getPreferredName().equals(parser.currentName())) {
                if (parser.nextToken() == Token.VALUE_STRING) {
                    source = new StoredScriptSource(true, UnknownScriptBinding.NAME, "mustache", parser.text(), Collections.emptyMap());
                } else if (parser.currentToken() == Token.START_OBJECT) {
                    if (parser.contentType() != XContentType.JSON) {
                        throw new IllegalArgumentException("unexpected content type [" + parser.contentType().mediaType() + "]" +
                            " for template, expected content type [" + XContentType.JSON.mediaType() + "]");
                    }

                    XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                    builder.copyCurrentStructure(parser);

                    source = new StoredScriptSource(true, UnknownScriptBinding.NAME, "mustache", builder.bytes().utf8ToString(),
                        Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()));
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "unexpected token [" + parser.currentToken() + "], expected [<template>]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "unexpected token [" + parser.currentToken() + "], " +
                        "expected [" + ScriptField.SCRIPT.getPreferredName() + ", " + ScriptField.TEMPLATE.getPreferredName() + "]");
            }

            if (parser.currentToken() == Token.END_OBJECT) {
                parser.nextToken();
            }

            return source;
        }

        public static StoredScriptSource staticReadFrom(StreamInput in) throws IOException {
            boolean template = in.readBoolean();
            String binding = in.readString();
            String lang = in.readString();
            String code = in.readString();
            Map<String, String> options = new HashMap<>();

            for (int count = in.readInt(); count > 0; --count) {
                options.put(in.readString(), in.readString());
            }

            return new StoredScriptSource(template, binding, lang, code, options);
        }

        public final boolean template;
        public final String binding;
        public final String lang;
        public final String code;
        public final Map<String, String> options;

        public StoredScriptSource(boolean template, String binding, String lang, String code, Map<String, String> options) {
            this.template = template;
            this.binding = Objects.requireNonNull(binding);
            this.lang = Objects.requireNonNull(lang);
            this.code = Objects.requireNonNull(code);
            this.options = Collections.unmodifiableMap(Objects.requireNonNull(options));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (template) {
                String content = options.get(CONTENT_TYPE_OPTION);

                if (content != null && content.equals(builder.contentType().mediaType())) {
                    builder.rawField(ScriptField.TEMPLATE.getPreferredName(), new BytesArray(code));
                } else {
                    builder.field(ScriptField.TEMPLATE.getPreferredName(), code);
                }
            } else {
                builder.startObject(ScriptField.SCRIPT.getPreferredName());
                builder.field(ScriptField.BIND.getPreferredName(), binding);
                builder.field(ScriptField.LANG.getPreferredName(), lang);
                builder.field(ScriptField.CODE.getPreferredName(), code);
                builder.endObject();
            }

            builder.endObject();

            return builder;
        }

        @Override
        public StoredScriptSource readFrom(StreamInput in) throws IOException {
            return staticReadFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(template);
            out.writeString(binding);
            out.writeString(lang);
            out.writeString(code);
            out.writeInt(options.size());

            for (Map.Entry<String, String> option : options.entrySet()) {
                out.writeString(option.getKey());
                out.writeString(option.getValue());
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StoredScriptSource that = (StoredScriptSource)o;

            if (template != that.template) return false;
            if (binding != null ? !binding.equals(that.binding) : that.binding != null) return false;
            if (!lang.equals(that.lang)) return false;
            if (!code.equals(that.code)) return false;
            return options.equals(that.options);

        }

        @Override
        public int hashCode() {
            int result = (template ? 1 : 0);
            result = 31 * result + (binding != null ? binding.hashCode() : 0);
            result = 31 * result + lang.hashCode();
            result = 31 * result + code.hashCode();
            result = 31 * result + options.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "StoredScriptSource{" +
                "template=" + template +
                ", binding='" + binding + '\'' +
                ", lang='" + lang + '\'' +
                ", code='" + code + '\'' +
                ", options=" + options +
                '}';
        }
    }

    public static final class ScriptInput implements ToXContent, Writeable {

        public static ScriptInput inline(String code) {
            return inline(DEFAULT_SCRIPT_LANG, code, Collections.emptyMap(), Collections.emptyMap());
        }

        public static ScriptInput inline(String lang, String code, Map<String, String> options, Map<String, Object> params) {
            return new ScriptInput(INLINE, new InlineScriptLookup(lang, code, options), params);
        }

        public static ScriptInput stored(String id) {
            return stored(id, Collections.emptyMap());
        }

        public static ScriptInput stored(String id, Map<String, Object> params) {
            return new ScriptInput(STORED, new StoredScriptLookup(id), params);
        }

        public static ScriptInput file(String id) {
            return file(id, Collections.emptyMap());
        }

        public static ScriptInput file(String id, Map<String, Object> params) {
            return new ScriptInput(FILE, new FileScriptLookup(id), params);
        }

        public static ScriptInput parse(XContentParser parser, ParseFieldMatcher matcher, String lang) throws IOException {
            if (lang == null) {
                lang = DEFAULT_SCRIPT_LANG;
            }

            Token token = parser.currentToken();

            if (token == null) {
                token = parser.nextToken();
            }

            if (token == Token.VALUE_STRING) {
                return inline(lang, parser.text(), Collections.emptyMap(), Collections.emptyMap());
            } else if (token != Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                    "unexpected value [" + parser.text() + "], expected [{, <code>]");
            }

            ScriptType type = null;
            String idOrCode = null;
            Map<String, String> options = null;
            Map<String, Object> params = null;

            String name = null;

            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    name = parser.currentName();
                } else if (matcher.match(name, FILE.parse)) {
                    if (type != null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected script type [" + FILE.parse.getPreferredName() + "], " +
                                "when type has already been specified [" + type.name + "]");
                    }

                    type = FILE;

                    if (token == Token.VALUE_STRING) {
                        idOrCode = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected value [" + parser.text() + "], expected [<id>]");
                    }
                } else if (matcher.match(name, STORED.parse)) {
                    if (type != null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected script type [" + STORED.parse.getPreferredName() + "], " +
                                "when type has already been specified [" + type.name + "]");
                    }

                    type = STORED;

                    if (token == Token.VALUE_STRING) {
                        idOrCode = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected value [" + parser.text() + "], expected [<id>]");
                    }
                } else if (matcher.match(name, INLINE.parse)) {
                    if (type != null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected script type [" + INLINE.parse.getPreferredName() + "], " +
                                "when type has already been specified [" + type.name + "]");
                    }

                    type = INLINE;

                    options = new HashMap<>();

                    if (parser.currentToken() == Token.START_OBJECT) {
                        options.put(CONTENT_TYPE_OPTION, parser.contentType().mediaType());
                        XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                        idOrCode = builder.copyCurrentStructure(parser).bytes().utf8ToString();
                    } else {
                        idOrCode = parser.text();
                    }
                } else if (matcher.match(name, ScriptField.LANG)) {
                    if (token == Token.VALUE_STRING) {
                        lang = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected value [" + parser.text() + "], expected [<lang>]");
                    }
                } else if (matcher.match(name, ScriptField.PARAMS)) {
                    if (token == Token.START_OBJECT) {
                        params = parser.map();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "unexpected value [" + parser.text() + "], expected [<params>]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "unexpected token [" + parser.currentToken() + "], expected [" +
                            FILE.parse.getPreferredName() + ", " +
                            STORED.parse.getPreferredName() + ", " +
                            INLINE.parse.getPreferredName() + ", " +
                            ScriptField.LANG.getPreferredName() + ", " +
                            ScriptField.PARAMS.getPreferredName() +
                        "]");
                }
            }

            return validate(type, lang, idOrCode, options, params == null ? Collections.emptyMap() : params);
        }

        @SuppressWarnings("unchecked")
        public static ScriptInput parse(Map<String, Object> parser, ParseFieldMatcher matcher, String lang) {
            if (lang == null) {
                lang = DEFAULT_SCRIPT_LANG;
            }

            ScriptType type = null;
            String idOrCode = null;
            Map<String, Object> params = null;

            Iterator<Entry<String, Object>> itr = parser.entrySet().iterator();

            while (itr.hasNext()) {
                Map.Entry<String, Object> entry = itr.next();

                String name = entry.getKey();
                Object value = entry.getValue();

                if (matcher.match(name, ScriptField.LANG)) {
                    if (value instanceof String || value == null) {
                        lang = (String)value;
                    } else {
                        throw new IllegalArgumentException("[" + name + "] must have value of type [String]");
                    }
                } else if (matcher.match(name, ScriptField.PARAMS)) {
                    if (value instanceof Map || value == null) {
                        params = (Map<String, Object>)value;
                    } else {
                        throw new IllegalArgumentException("[" + name + "] must have value of type [Map<String, Object>]");
                    }
                } else if (matcher.match(name, FILE.parse)) {
                    if (value instanceof String || value == null) {
                        type = Script.ScriptType.FILE;
                        idOrCode = (String)value;
                    } else {
                        throw new IllegalArgumentException("[" + FILE.name + "] must have value of type [String]");
                    }
                } else if (matcher.match(name, STORED.parse)) {
                    if (value instanceof String || value == null) {
                        type = Script.ScriptType.INLINE;
                        idOrCode = (String)value;
                    } else {
                        throw new IllegalArgumentException("[" + STORED.name + "] must have value of type [String]");
                    }
                } else if (matcher.match(name, INLINE.parse)) {
                    if (value instanceof String || value == null) {
                        type = Script.ScriptType.INLINE;
                        idOrCode = (String)value;
                    } else {
                        throw new IllegalArgumentException("[" + INLINE.name + "] must have value of type [String]");
                    }
                }
            }
            if (idOrCode == null) {
                throw new IllegalArgumentException(
                    "unexpected type or no type specified, expected [" +
                        FILE.parse.getPreferredName() + ", " +
                        STORED.parse.getPreferredName() + ", " +
                        INLINE.parse.getPreferredName() + ", " +
                        ScriptField.LANG.getPreferredName() + ", " +
                        ScriptField.PARAMS.getPreferredName() +
                    "]");
            }

            return validate(type, lang, idOrCode,
                type == INLINE ? Collections.emptyMap() : null,
                params == null ? Collections.emptyMap() : params);
        }

        public static ScriptInput update(ScriptInput copy, ScriptType type, String lang, String idOrCode,
                                         Map<String, String> options, Map<String, Object> params) {
            if (copy == null) {
                return validate(type, lang, idOrCode, options, params);
            }

            if (type != null && type != copy.type) {
                throw new IllegalArgumentException(
                    "updated script type [" + type + "] did not match original script type [" + copy.type + "]");
            }

            type = type == null ? copy.type : type;

            if (type == FILE) {
                idOrCode = idOrCode == null ? ((FileScriptLookup)copy.lookup).id : idOrCode;
            } else if (type == STORED) {
                idOrCode = idOrCode == null ? ((StoredScriptLookup)copy.lookup).id : idOrCode;
            } else if (type == INLINE) {
                lang = lang == null ? ((InlineScriptLookup)copy.lookup).lang : lang;
                idOrCode = idOrCode == null ? ((InlineScriptLookup)copy.lookup).code : idOrCode;
                options = options == null ? ((InlineScriptLookup)copy.lookup).options : options;
            }

            params = params == null ? copy.params : params;

            return validate(type, lang, idOrCode, options, params);
        }

        private static ScriptInput validate(ScriptType type, String lang, String idOrCode,
                                         Map<String, String> options, Map<String, Object> params) {
            Objects.requireNonNull(params);

            if (type == FILE) {
                if (lang != null) {
                    throw new IllegalArgumentException("[" + ScriptField.LANG.getPreferredName() + "]" +
                        " should not be used when specifiying a [" + FILE.name + "] script");
                }

                if (idOrCode == null) {
                    throw new IllegalArgumentException("must specify [id] when using a [" + FILE.name + "] script");
                }

                if (options != null) {
                    throw new IllegalArgumentException("[options] should not be used when specifiying a [" + FILE.name + "] script");
                }

                return file(idOrCode, params);
            } else if (type == STORED) {
                if (lang != null) {
                    throw new IllegalArgumentException("[" + ScriptField.LANG.getPreferredName() + "]" +
                        " should not be used when specifiying a [" + STORED.name + "] script");
                }

                if (idOrCode == null) {
                    throw new IllegalArgumentException("must specify [id] when using a [" + STORED.name + "] script");
                }

                if (options != null) {
                    throw new IllegalArgumentException("[options] should not be used when specifiying a [" + STORED.name + "] script");
                }

                return stored(idOrCode, params);
            } else if (type == INLINE) {
                if (lang == null) {
                    throw new IllegalArgumentException("[" + ScriptField.LANG.getPreferredName() + "]" +
                        " is required when using a [" + INLINE.name + "] script");
                }

                if (idOrCode == null) {
                    throw new IllegalArgumentException("must specify [code] when using an [" + INLINE.name + "] script");
                }

                if (options == null) {
                    throw new IllegalArgumentException("[options] is required when using an [" + INLINE.name + "] script");
                }

                return inline(lang, idOrCode, options, params);
            } else {
                throw new IllegalArgumentException("must specify [type] of script, " +
                    "expected [" + FILE.name + ", " + STORED.name + "," + INLINE.name + "]");
            }
        }

        public static ScriptInput readFrom(StreamInput in) throws IOException {
            ScriptType type = ScriptType.fromValue(in.readInt());
            ScriptLookup lookup;

            if (type == FILE) {
                lookup = FileScriptLookup.readFrom(in);
            } else if (type == STORED) {
                lookup = StoredScriptLookup.readFrom(in);
            } else if (type == INLINE) {
                lookup = InlineScriptLookup.readFrom(in);
            } else {
                throw new IllegalArgumentException("unexpected script type [" + type.name + "], " +
                    "expected [" + FILE.name + ", " + STORED.name + "," + INLINE.name + "]");
            }

            Map<String, Object> params = in.readMap();

            return new ScriptInput(type, lookup, params);
        }

        public final ScriptType type;
        public final ScriptLookup lookup;
        public final Map<String, Object> params;

        private ScriptInput(ScriptType type, ScriptLookup lookup, Map<String, Object> params) {
            this.type = Objects.requireNonNull(type);
            this.lookup = Objects.requireNonNull(lookup);
            this.params = Collections.unmodifiableMap(Objects.requireNonNull(params));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            lookup.toXContent(builder, params);
            builder.field(ScriptField.PARAMS.getPreferredName(), this.params);
            builder.endObject();

            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(type.value);
            lookup.writeTo(out);
            out.writeMap(params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ScriptInput that = (ScriptInput)o;

            if (!lookup.equals(that.lookup)) return false;
            return params.equals(that.params);
        }

        @Override
        public int hashCode() {
            int result = lookup.hashCode();
            result = 31 * result + params.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "ScriptInput{" +
                "lookup=" + lookup +
                ", params=" + params +
                '}';
        }
    }

    public interface ScriptLookup extends ToXContent, Writeable {
        CompiledScript getCompiled(ScriptService service, ScriptContext context, ScriptBinding binding);
    }

    public static final class FileScriptLookup implements ScriptLookup {

        public static FileScriptLookup readFrom(StreamInput in) throws IOException {
            return new FileScriptLookup(in.readString());
        }

        public final String id;

        private FileScriptLookup(String id) {
            this.id = Objects.requireNonNull(id);
        }

        @Override
        public CompiledScript getCompiled(ScriptService service, ScriptContext context, ScriptBinding binding) {
            return service.getFileScript(context, binding, id);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(FILE.parse.getPreferredName(), id);

            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FileScriptLookup that = (FileScriptLookup)o;

            return id.equals(that.id);

        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return "FileScriptLookup{" +
                "id='" + id + '\'' +
                '}';
        }
    }

    public static final class StoredScriptLookup implements ScriptLookup {

        public static StoredScriptLookup readFrom(StreamInput in) throws IOException {
            return new StoredScriptLookup(in.readString());
        }

        public final String id;

        private StoredScriptLookup(String id) {
            this.id = Objects.requireNonNull(id);
        }

        @Override
        public CompiledScript getCompiled(ScriptService service, ScriptContext context, ScriptBinding binding) {
            return service.getStoredScript(context, binding, id);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(STORED.parse.getPreferredName(), id);

            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StoredScriptLookup that = (StoredScriptLookup)o;

            return id.equals(that.id);

        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return "StoredScriptLookup{" +
                "id='" + id + '\'' +
                '}';
        }
    }

    public static final class InlineScriptLookup implements ScriptLookup {

        public static InlineScriptLookup readFrom(StreamInput in) throws IOException {
            String lang = in.readString();
            String code = in.readString();
            Map<String, String> options = new HashMap<>();

            for (int count = in.readInt(); count > 0; --count) {
                options.put(in.readString(), in.readString());
            }

            return new InlineScriptLookup(lang, code, options);
        }

        public final String lang;
        public final String code;
        public final Map<String, String> options;

        private InlineScriptLookup(String lang, String code, Map<String, String> options) {
            this.lang = Objects.requireNonNull(lang);
            this.code = Objects.requireNonNull(code);
            this.options = Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(options)));
        }

        @Override
        public CompiledScript getCompiled(ScriptService service, ScriptContext context, ScriptBinding binding) {
            return service.getInlineScript(context, binding, lang, code, options);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(lang);
            out.writeString(code);
            out.writeInt(options.size());

            for (Map.Entry<String, String> option : options.entrySet()) {
                out.writeString(option.getKey());
                out.writeString(option.getValue());
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(ScriptField.LANG.getPreferredName(), lang);

            String content = options.get(CONTENT_TYPE_OPTION);

            if (content != null && content.equals(builder.contentType().mediaType())) {
                builder.rawField(INLINE.parse.getPreferredName(), new BytesArray(code));
            } else {
                builder.field(INLINE.parse.getPreferredName(), code);
            }

            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InlineScriptLookup that = (InlineScriptLookup)o;

            if (!lang.equals(that.lang)) return false;
            if (!code.equals(that.code)) return false;
            return options.equals(that.options);

        }

        @Override
        public int hashCode() {
            int result = lang.hashCode();
            result = 31 * result + code.hashCode();
            result = 31 * result + options.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "InlineScriptLookup{" +
                "lang='" + lang + '\'' +
                ", code='" + code + '\'' +
                ", options=" + options +
                '}';
        }
    }

    public interface ScriptBinding {
        Object compile(ScriptEngineService engine, String id, String code, Map<String, String> options);
    }

    public static final class UnknownScriptBinding implements ScriptBinding {
        public static final String NAME = "unknown";
        public static final UnknownScriptBinding BINDING = new UnknownScriptBinding();

        private UnknownScriptBinding() {}

        @Override
        public Object compile(ScriptEngineService engine, String id, String code, Map<String, String> options) {
            return engine.compile(id, code, options);
        }
    }

    public static final class ExecutableScriptBinding implements ScriptBinding {
        public static final String NAME = "executable";
        public static final ExecutableScriptBinding BINDING = new ExecutableScriptBinding();

        public static ExecutableScript bind(ScriptService service, ScriptContext context,
                                            ScriptLookup lookup, Map<String, Object> variables) {
            CompiledScript compiled = lookup.getCompiled(service, context, ExecutableScriptBinding.BINDING);

            return bind(compiled, variables);
        }

        public static ExecutableScript bind(CompiledScript compiled, Map<String, Object> variables) {
            return compiled.engine().executable(compiled, variables);
        }

        private ExecutableScriptBinding() {}

        @Override
        public Object compile(ScriptEngineService engine, String id, String code, Map<String, String> options) {
            return engine.compile(id, code, options);
        }
    }

    public static final class SearchScriptBinding implements ScriptBinding {
        public static final String NAME = "search";
        public static final SearchScriptBinding BINDING = new SearchScriptBinding();

        public static SearchScript bind(ScriptService service, ScriptContext context, SearchLookup search,
                                        ScriptLookup lookup, Map<String, Object> variables) {
            CompiledScript compiled = lookup.getCompiled(service, context, SearchScriptBinding.BINDING);

            return bind(compiled, search, variables);
        }

        public static SearchScript bind(CompiledScript compiled, SearchLookup lookup, Map<String, Object> variables) {
            return compiled.engine().search(compiled, lookup, variables);
        }

        private SearchScriptBinding() {}

        @Override
        public Object compile(ScriptEngineService engine, String id, String code, Map<String, String> options) {
            return engine.compile(id, code, options);
        }
    }

    public static final String DEFAULT_SCRIPT_LANG = "painless";
    public static final ScriptType DEFAULT_SCRIPT_TYPE = INLINE;

    public static final String CONTENT_TYPE_OPTION = "content_type";

    private Script() {}
}
