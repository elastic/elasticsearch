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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * {@link Script} represents used-defined input that can be used to
 * compile and execute a script from the {@link ScriptService}
 * based on the {@link ScriptType}.
 *
 * There are two types of scripts specified by {@link ScriptType},
 * <code>INLINE</code>, and <code>STORED</code>.
 *
 * The following describes the expected parameters for each type of script:
 *
 * <ul>
 * <li> {@link ScriptType#INLINE}
 * <ul>
 * <li> {@link Script#lang}     - specifies the language, defaults to {@link Script#DEFAULT_SCRIPT_LANG}
 * <li> {@link Script#idOrCode} - specifies the code to be compiled, must not be {@code null}
 * <li> {@link Script#options}  - specifies the compiler options for this script; must not be {@code null},
 *                                use an empty {@link Map} to specify no options
 * <li> {@link Script#params}   - {@link Map} of user-defined parameters; must not be {@code null},
 *                                use an empty {@link Map} to specify no params
 * </ul>
 * <li> {@link ScriptType#STORED}
 * <ul>
 * <li> {@link Script#lang}     - the language will be specified when storing the script, so this should
 *                                be {@code null}
 * <li> {@link Script#idOrCode} - specifies the id of the stored script to be looked up, must not be {@code null}
 * <li> {@link Script#options}  - compiler options will be specified when a stored script is stored,
 *                                so they have no meaning here and must be {@code null}
 * <li> {@link Script#params}   - {@link Map} of user-defined parameters; must not be {@code null},
 *                                use an empty {@link Map} to specify no params
 * </ul>
 * </ul>
 */
public final class Script implements ToXContentObject, Writeable {

    /**
     * The name of the of the default scripting language.
     */
    public static final String DEFAULT_SCRIPT_LANG = "painless";

    /**
     * The name of the default template language.
     */
    public static final String DEFAULT_TEMPLATE_LANG = "mustache";

    /**
     * The default {@link ScriptType}.
     */
    public static final ScriptType DEFAULT_SCRIPT_TYPE = ScriptType.INLINE;

    /**
     * Compiler option for {@link XContentType} used for templates.
     */
    public static final String CONTENT_TYPE_OPTION = "content_type";

    /**
     * Standard {@link ParseField} for outer level of script queries.
     */
    public static final ParseField SCRIPT_PARSE_FIELD = new ParseField("script");

    /**
     * Standard {@link ParseField} for source on the inner level.
     */
    public static final ParseField SOURCE_PARSE_FIELD = new ParseField("source");

    /**
     * Standard {@link ParseField} for lang on the inner level.
     */
    public static final ParseField LANG_PARSE_FIELD = new ParseField("lang");

    /**
     * Standard {@link ParseField} for options on the inner level.
     */
    public static final ParseField OPTIONS_PARSE_FIELD = new ParseField("options");

    /**
     * Standard {@link ParseField} for params on the inner level.
     */
    public static final ParseField PARAMS_PARSE_FIELD = new ParseField("params");

    /**
     * Helper class used by {@link ObjectParser} to store mutable {@link Script} variables and then
     * construct an immutable {@link Script} object based on parsed XContent.
     */
    private static final class Builder {
        private ScriptType type;
        private String lang;
        private String idOrCode;
        private Map<String, String> options;
        private Map<String, Object> params;

        private Builder() {
            // This cannot default to an empty map because options are potentially added at multiple points.
            this.options = new HashMap<>();
            this.params = Collections.emptyMap();
        }

        /**
         * Since inline scripts can accept code rather than just an id, they must also be able
         * to handle template parsing, hence the need for custom parsing code.  Templates can
         * consist of either an {@link String} or a JSON object.  If a JSON object is discovered
         * then the content type option must also be saved as a compiler option.
         */
        private void setInline(XContentParser parser) {
            try {
                if (type != null) {
                    throwOnlyOneOfType();
                }

                type = ScriptType.INLINE;

                if (parser.currentToken() == Token.START_OBJECT) {
                    //this is really for search templates, that need to be converted to json format
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    idOrCode = Strings.toString(builder.copyCurrentStructure(parser));
                    options.put(CONTENT_TYPE_OPTION, XContentType.JSON.mediaType());
                } else {
                    idOrCode = parser.text();
                }
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }

        /**
         * Set both the id and the type of the stored script.
         */
        private void setStored(String idOrCode) {
            if (type != null) {
                throwOnlyOneOfType();
            }

            type = ScriptType.STORED;
            this.idOrCode = idOrCode;
        }

        /**
         * Helper method to throw an exception if more than one type of {@link Script} is specified.
         */
        private void throwOnlyOneOfType() {
            throw new IllegalArgumentException("must only use one of [" +
                ScriptType.INLINE.getParseField().getPreferredName() + ", " +
                ScriptType.STORED.getParseField().getPreferredName() + "]" +
                " when specifying a script");
        }

        private void setLang(String lang) {
            this.lang = lang;
        }

        /**
         * Options may have already been added if an inline template was specified.
         * Appends the user-defined compiler options with the internal compiler options.
         */
        private void setOptions(Map<String, String> options) {
            this.options.putAll(options);
        }

        private void setParams(Map<String, Object> params) {
            this.params = params;
        }

        /**
         * Validates the parameters and creates an {@link Script}.
         * @param defaultLang The default lang is not a compile-time constant and must be provided
         *                    at run-time this way in case a legacy default language is used from
         *                    previously stored queries.
         */
        private Script build(String defaultLang) {
            if (type == null) {
                throw new IllegalArgumentException("must specify either [source] for an inline script or [id] for a stored script");
            }

            if (type == ScriptType.INLINE) {
                if (lang == null) {
                    lang = defaultLang;
                }

                if (idOrCode == null) {
                    throw new IllegalArgumentException(
                        "must specify <id> for an inline script");
                }

                if (options.size() > 1 || options.size() == 1 && options.get(CONTENT_TYPE_OPTION) == null) {
                    options.remove(CONTENT_TYPE_OPTION);

                    throw new IllegalArgumentException("illegal compiler options [" + options + "] specified");
                }
            } else if (type == ScriptType.STORED) {
                if (lang != null) {
                    throw new IllegalArgumentException(
                        "illegally specified <lang> for a stored script");
                }

                if (idOrCode == null) {
                    throw new IllegalArgumentException(
                        "must specify <code> for a stored script");
                }

                if (options.isEmpty()) {
                    options = null;
                } else {
                    throw new IllegalArgumentException("field [" + OPTIONS_PARSE_FIELD.getPreferredName() + "] " +
                        "cannot be specified using a stored script");
                }
            }

            return new Script(type, lang, idOrCode, options, params);
        }
    }

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("script", Builder::new);

    static {
        // Defines the fields necessary to parse a Script as XContent using an ObjectParser.
        PARSER.declareField(Builder::setInline, parser -> parser, ScriptType.INLINE.getParseField(), ValueType.OBJECT_OR_STRING);
        PARSER.declareString(Builder::setStored, ScriptType.STORED.getParseField());
        PARSER.declareString(Builder::setLang, LANG_PARSE_FIELD);
        PARSER.declareField(Builder::setOptions, XContentParser::mapStrings, OPTIONS_PARSE_FIELD, ValueType.OBJECT);
        PARSER.declareField(Builder::setParams, XContentParser::map, PARAMS_PARSE_FIELD, ValueType.OBJECT);
    }

    /**
     * Declare a script field on an {@link ObjectParser} with the standard name ({@code script}).
     * @param <T> Whatever type the {@linkplain ObjectParser} is parsing.
     * @param parser the parser itself
     * @param consumer the consumer for the script
     */
    public static <T> void declareScript(AbstractObjectParser<T, ?> parser, BiConsumer<T, Script> consumer) {
        declareScript(parser, consumer, Script.SCRIPT_PARSE_FIELD);
    }

    /**
     * Declare a script field on an {@link ObjectParser}.
     * @param <T> Whatever type the {@linkplain ObjectParser} is parsing.
     * @param parser the parser itself
     * @param consumer the consumer for the script
     * @param parseField the field name
     */
    public static <T> void declareScript(AbstractObjectParser<T, ?> parser, BiConsumer<T, Script> consumer, ParseField parseField) {
        parser.declareField(consumer, (p, c) -> Script.parse(p), parseField, ValueType.OBJECT_OR_STRING);
    }

    /**
     * Convenience method to call {@link Script#parse(XContentParser, String)}
     * using the default scripting language.
     */
    public static Script parse(XContentParser parser) throws IOException {
        return parse(parser, DEFAULT_SCRIPT_LANG);
    }

    /**
     * Parse the script configured in the given settings.
     */
    public static Script parse(Settings settings) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()){
            builder.startObject();
            settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            try (InputStream stream = BytesReference.bytes(builder).streamInput();
                 XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, stream)) {
                return parse(parser);
            }
        } catch (IOException e) {
            // it should not happen since we are not actually reading from a stream but an in-memory byte[]
            throw new IllegalStateException(e);
        }
    }

    /**
     * This will parse XContent into a {@link Script}.  The following formats can be parsed:
     *
     * The simple format defaults to an {@link ScriptType#INLINE} with no compiler options or user-defined params:
     *
     * Example:
     * {@code
     * "return Math.log(doc.popularity) * 100;"
     * }
     *
     * The complex format where {@link ScriptType} and idOrCode are required while lang, options and params are not required.
     *
     * {@code
     * {
     *     // Exactly one of "id" or "source" must be specified
     *     "id" : "<id>",
     *     // OR
     *     "source": "<source>",
     *     "lang" : "<lang>",
     *     "options" : {
     *         "option0" : "<option0>",
     *         "option1" : "<option1>",
     *         ...
     *     },
     *     "params" : {
     *         "param0" : "<param0>",
     *         "param1" : "<param1>",
     *         ...
     *     }
     * }
     * }
     *
     * Example:
     * {@code
     * {
     *     "source" : "return Math.log(doc.popularity) * params.multiplier",
     *     "lang" : "painless",
     *     "params" : {
     *         "multiplier" : 100.0
     *     }
     * }
     * }
     *
     * This also handles templates in a special way.  If a complexly formatted query is specified as another complex
     * JSON object the query is assumed to be a template, and the format will be preserved.
     *
     * {@code
     * {
     *     "source" : { "query" : ... },
     *     "lang" : "<lang>",
     *     "options" : {
     *         "option0" : "<option0>",
     *         "option1" : "<option1>",
     *         ...
     *     },
     *     "params" : {
     *         "param0" : "<param0>",
     *         "param1" : "<param1>",
     *         ...
     *     }
     * }
     * }
     *
     * @param parser       The {@link XContentParser} to be used.
     * @param defaultLang  The default language to use if no language is specified.  The default language isn't necessarily
     *                     the one defined by {@link Script#DEFAULT_SCRIPT_LANG} due to backwards compatibility requirements
     *                     related to stored queries using previously default languages.
     *
     * @return             The parsed {@link Script}.
     */
    public static Script parse(XContentParser parser, String defaultLang) throws IOException {
        Objects.requireNonNull(defaultLang);

        Token token = parser.currentToken();

        if (token == null) {
            token = parser.nextToken();
        }

        if (token == Token.VALUE_STRING) {
            return new Script(ScriptType.INLINE, defaultLang, parser.text(), Collections.emptyMap());
        }

        return PARSER.apply(parser, null).build(defaultLang);
    }

    /**
     * Parse a {@link Script} from an {@link Object}, that can either be a {@link String} or a {@link Map}.
     * @see #parse(XContentParser, String)
     * @param config  The object to parse the script from.
     * @return        The parsed {@link Script}.
     */
    @SuppressWarnings("unchecked")
    public static Script parse(Object config) {
        Objects.requireNonNull(config, "Script must not be null");
        if (config instanceof String) {
            return new Script((String) config);
        } else if (config instanceof Map) {
            Map<String,Object> configMap = (Map<String, Object>) config;
            String script = null;
            ScriptType type = null;
            String lang = null;
            Map<String, Object> params = Collections.emptyMap();
            Map<String, String> options = Collections.emptyMap();
            for (Map.Entry<String, Object> entry : configMap.entrySet()) {
                String parameterName = entry.getKey();
                Object parameterValue = entry.getValue();
                if (Script.LANG_PARSE_FIELD.match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof String || parameterValue == null) {
                        lang = (String) parameterValue;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                    }
                } else if (Script.PARAMS_PARSE_FIELD.match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof Map || parameterValue == null) {
                        params = (Map<String, Object>) parameterValue;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type Map: [" + parameterName + "]");
                    }
                } else if (Script.OPTIONS_PARSE_FIELD.match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof Map || parameterValue == null) {
                        options = (Map<String, String>) parameterValue;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type Map: [" + parameterName + "]");
                    }
                } else if (ScriptType.INLINE.getParseField().match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof String || parameterValue == null) {
                        script = (String) parameterValue;
                        type = ScriptType.INLINE;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                    }
                } else if (ScriptType.STORED.getParseField().match(parameterName, LoggingDeprecationHandler.INSTANCE)) {
                    if (parameterValue instanceof String || parameterValue == null) {
                        script = (String) parameterValue;
                        type = ScriptType.STORED;
                    } else {
                        throw new ElasticsearchParseException("Value must be of type String: [" + parameterName + "]");
                    }
                } else {
                    throw new ElasticsearchParseException("Unsupported field [" + parameterName + "]");
                }
            }
            if (script == null) {
                throw new ElasticsearchParseException("Expected one of [{}] or [{}] fields, but found none",
                    ScriptType.INLINE.getParseField().getPreferredName(), ScriptType.STORED.getParseField().getPreferredName());
            }
            assert type != null : "if script is not null, type should definitely not be null";

            if (type == ScriptType.STORED) {
                if (lang != null) {
                    throw new IllegalArgumentException("[" + Script.LANG_PARSE_FIELD.getPreferredName() +
                        "] cannot be specified for stored scripts");
                }

                return new Script(type, null, script, null, params);
            } else {
                return new Script(type, lang == null ? DEFAULT_SCRIPT_LANG : lang, script, options, params);
            }
        } else {
            throw new IllegalArgumentException("Script value should be a String or a Map");
        }
    }

    private final ScriptType type;
    private final String lang;
    private final String idOrCode;
    private final Map<String, String> options;
    private final Map<String, Object> params;

    /**
     * Constructor for simple script using the default language and default type.
     * @param idOrCode The id or code to use dependent on the default script type.
     */
    public Script(String idOrCode) {
        this(DEFAULT_SCRIPT_TYPE, DEFAULT_SCRIPT_LANG, idOrCode, Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Constructor for a script that does not need to use compiler options.
     * @param type     The {@link ScriptType}.
     * @param lang     The language for this {@link Script} if the {@link ScriptType} is {@link ScriptType#INLINE}.
     *                 For {@link ScriptType#STORED} scripts this should be null, but can
     *                 be specified to access scripts stored as part of the stored scripts deprecated API.
     * @param idOrCode The id for this {@link Script} if the {@link ScriptType} is {@link ScriptType#STORED}.
     *                 The code for this {@link Script} if the {@link ScriptType} is {@link ScriptType#INLINE}.
     * @param params   The user-defined params to be bound for script execution.
     */
    public Script(ScriptType type, String lang, String idOrCode, Map<String, Object> params) {
        this(type, lang, idOrCode, type == ScriptType.INLINE ? Collections.emptyMap() : null, params);
    }

    /**
     * Constructor for a script that requires the use of compiler options.
     * @param type     The {@link ScriptType}.
     * @param lang     The language for this {@link Script} if the {@link ScriptType} is {@link ScriptType#INLINE}.
     *                 For {@link ScriptType#STORED} scripts this should be null, but can
     *                 be specified to access scripts stored as part of the stored scripts deprecated API.
     * @param idOrCode The id for this {@link Script} if the {@link ScriptType} is {@link ScriptType#STORED}.
     *                 The code for this {@link Script} if the {@link ScriptType} is {@link ScriptType#INLINE}.
     * @param options  The map of compiler options for this {@link Script} if the {@link ScriptType}
     *                 is {@link ScriptType#INLINE}, {@code null} otherwise.
     * @param params   The user-defined params to be bound for script execution.
     */
    public Script(ScriptType type, String lang, String idOrCode, Map<String, String> options, Map<String, Object> params) {
        this.type = Objects.requireNonNull(type);
        this.idOrCode = Objects.requireNonNull(idOrCode);
        this.params = Collections.unmodifiableMap(Objects.requireNonNull(params));

        if (type == ScriptType.INLINE) {
            this.lang = Objects.requireNonNull(lang);
            this.options = Collections.unmodifiableMap(Objects.requireNonNull(options));
        } else if (type == ScriptType.STORED) {
            if (lang != null) {
                throw new IllegalArgumentException("lang cannot be specified for stored scripts");
            }

            this.lang = null;

            if (options != null) {
                throw new IllegalStateException("options cannot be specified for stored scripts");
            }

            this.options = null;
        } else {
            throw new IllegalStateException("unknown script type [" + type.getName() + "]");
        }
    }

    /**
     * Creates a {@link Script} read from an input stream.
     */
    public Script(StreamInput in) throws IOException {
        this.type = ScriptType.readFrom(in);
        this.lang = in.readOptionalString();
        this.idOrCode = in.readString();
        @SuppressWarnings("unchecked")
        Map<String, String> options = (Map<String, String>)(Map)in.readMap();
        this.options = options;
        this.params = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        type.writeTo(out);
        out.writeOptionalString(lang);
        out.writeString(idOrCode);
        @SuppressWarnings("unchecked")
        Map<String, Object> options = (Map<String, Object>) (Map) this.options;
        out.writeMap(options);
        out.writeMap(params);
    }

    /**
     * This will build scripts into the following XContent structure:
     *
     * {@code
     * {
     *     "<(id, source)>" : "<idOrCode>",
     *     "lang" : "<lang>",
     *     "options" : {
     *         "option0" : "<option0>",
     *         "option1" : "<option1>",
     *         ...
     *     },
     *     "params" : {
     *         "param0" : "<param0>",
     *         "param1" : "<param1>",
     *         ...
     *     }
     * }
     * }
     *
     * Example:
     * {@code
     * {
     *     "source" : "return Math.log(doc.popularity) * params.multiplier;",
     *     "lang" : "painless",
     *     "params" : {
     *         "multiplier" : 100.0
     *     }
     * }
     * }
     *
     * Note that lang, options, and params will only be included if there have been any specified.
     *
     * This also handles templates in a special way.  If the {@link Script#CONTENT_TYPE_OPTION} option
     * is provided and the {@link ScriptType#INLINE} is specified then the template will be preserved as a raw field.
     *
     * {@code
     * {
     *     "source" : { "query" : ... },
     *     "lang" : "<lang>",
     *     "options" : {
     *         "option0" : "<option0>",
     *         "option1" : "<option1>",
     *         ...
     *     },
     *     "params" : {
     *         "param0" : "<param0>",
     *         "param1" : "<param1>",
     *         ...
     *     }
     * }
     * }
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject();

        String contentType = options == null ? null : options.get(CONTENT_TYPE_OPTION);

        if (type == ScriptType.INLINE) {
            if (contentType != null && builder.contentType().mediaType().equals(contentType)) {
                try (InputStream stream = new BytesArray(idOrCode).streamInput()) {
                    builder.rawField(SOURCE_PARSE_FIELD.getPreferredName(), stream);
                }
            } else {
                builder.field(SOURCE_PARSE_FIELD.getPreferredName(), idOrCode);
            }
        } else {
            builder.field("id", idOrCode);
        }

        if (lang != null) {
            builder.field(LANG_PARSE_FIELD.getPreferredName(), lang);
        }

        if (options != null && !options.isEmpty()) {
            builder.field(OPTIONS_PARSE_FIELD.getPreferredName(), options);
        }

        if (!params.isEmpty()) {
            builder.field(PARAMS_PARSE_FIELD.getPreferredName(), params);
        }

        builder.endObject();

        return builder;
    }

    /**
     * @return The {@link ScriptType} for this {@link Script}.
     */
    public ScriptType getType() {
        return type;
    }

    /**
     * @return The language for this {@link Script} if the {@link ScriptType} is {@link ScriptType#INLINE}.
     *         For {@link ScriptType#STORED} scripts this should be null, but can
     *         be specified to access scripts stored as part of the stored scripts deprecated API.
     */
    public String getLang() {
        return lang;
    }

    /**
     * @return The id for this {@link Script} if the {@link ScriptType} is {@link ScriptType#STORED}.
     *         The code for this {@link Script} if the {@link ScriptType} is {@link ScriptType#INLINE}.
     */
    public String getIdOrCode() {
        return idOrCode;
    }

    /**
     * @return The map of compiler options for this {@link Script} if the {@link ScriptType}
     *         is {@link ScriptType#INLINE}, {@code null} otherwise.
     */
    public Map<String, String> getOptions() {
        return options;
    }

    /**
     * @return The map of user-defined params for this {@link Script}.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Script script = (Script)o;

        if (type != script.type) return false;
        if (lang != null ? !lang.equals(script.lang) : script.lang != null) return false;
        if (!idOrCode.equals(script.idOrCode)) return false;
        if (options != null ? !options.equals(script.options) : script.options != null) return false;
        return params.equals(script.params);

    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (lang != null ? lang.hashCode() : 0);
        result = 31 * result + idOrCode.hashCode();
        result = 31 * result + (options != null ? options.hashCode() : 0);
        result = 31 * result + params.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Script{" +
            "type=" + type +
            ", lang='" + lang + '\'' +
            ", idOrCode='" + idOrCode + '\'' +
            ", options=" + options +
            ", params=" + params +
            '}';
    }
}
