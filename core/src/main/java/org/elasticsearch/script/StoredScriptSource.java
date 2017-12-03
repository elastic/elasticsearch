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
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link StoredScriptSource} represents user-defined parameters for a script
 * saved in the {@link ClusterState}.
 */
public class StoredScriptSource extends AbstractDiffable<StoredScriptSource> implements Writeable, ToXContentObject {

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
     * Standard {@link ParseField} for source on the inner level.
     */
    public static final ParseField SOURCE_PARSE_FIELD = new ParseField("source", "code");

    /**
     * Standard {@link ParseField} for options on the inner level.
     */
    public static final ParseField OPTIONS_PARSE_FIELD = new ParseField("options");

    /**
     * Helper class used by {@link ObjectParser} to store mutable {@link StoredScriptSource} variables and then
     * construct an immutable {@link StoredScriptSource} object based on parsed XContent.
     */
    private static final class Builder {
        private String lang;
        private String source;
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
         * to handle template parsing, hence the need for custom parsing source.  Templates can
         * consist of either an {@link String} or a JSON object.  If a JSON object is discovered
         * then the content type option must also be saved as a compiler option.
         */
        private void setSource(XContentParser parser) {
            try {
                if (parser.currentToken() == Token.START_OBJECT) {
                    //this is really for search templates, that need to be converted to json format
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    source = builder.copyCurrentStructure(parser).string();
                    options.put(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType());
                } else {
                    source = parser.text();
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

            if (source == null) {
                throw new IllegalArgumentException("must specify source for stored script");
            } else if (source.isEmpty()) {
                throw new IllegalArgumentException("source cannot be empty");
            }

            if (options.size() > 1 || options.size() == 1 && options.get(Script.CONTENT_TYPE_OPTION) == null) {
                throw new IllegalArgumentException("illegal compiler options [" + options + "] specified");
            }

            return new StoredScriptSource(lang, source, options);
        }
    }

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("stored script source", Builder::new);

    static {
        // Defines the fields necessary to parse a Script as XContent using an ObjectParser.
        PARSER.declareString(Builder::setLang, LANG_PARSE_FIELD);
        PARSER.declareField(Builder::setSource, parser -> parser, SOURCE_PARSE_FIELD, ValueType.OBJECT_OR_STRING);
        PARSER.declareField(Builder::setOptions, XContentParser::mapStrings, OPTIONS_PARSE_FIELD, ValueType.OBJECT);
    }

    /**
     * This will parse XContent into a {@link StoredScriptSource}.  The following formats can be parsed:
     *
     * The simple script format with no compiler options or user-defined params:
     *
     * Example:
     * {@code
     * {"script": "return Math.log(doc.popularity) * 100;"}
     * }
     *
     * The above format requires the lang to be specified using the deprecated stored script namespace
     * (as a url parameter during a put request).  See {@link ScriptMetaData} for more information about
     * the stored script namespaces.
     *
     * The complex script format using the new stored script namespace
     * where lang and source are required but options is optional:
     *
     * {@code
     * {
     *     "script" : {
     *         "lang" : "<lang>",
     *         "source" : "<source>",
     *         "options" : {
     *             "option0" : "<option0>",
     *             "option1" : "<option1>",
     *             ...
     *         }
     *     }
     * }
     * }
     *
     * Example:
     * {@code
     * {
     *     "script": {
     *         "lang" : "painless",
     *         "source" : "return Math.log(doc.popularity) * params.multiplier"
     *     }
     * }
     * }
     *
     * The use of "source" may also be substituted with "code" for backcompat with 5.3 to 5.5 format. For example:
     *
     * {@code
     * {
     *     "script" : {
     *         "lang" : "<lang>",
     *         "code" : "<source>",
     *         "options" : {
     *             "option0" : "<option0>",
     *             "option1" : "<option1>",
     *             ...
     *         }
     *     }
     * }
     * }
     *
     * The simple template format:
     *
     * {@code
     * {
     *     "query" : ...
     * }
     * }
     *
     * The complex template format:
     *
     * {@code
     * {
     *     "template": {
     *         "query" : ...
     *     }
     * }
     * }
     *
     * Note that templates can be handled as both strings and complex JSON objects.
     * Also templates may be part of the 'source' parameter in a script.  The Parser
     * can handle this case as well.
     *
     * @param content The content from the request to be parsed as described above.
     * @return        The parsed {@link StoredScriptSource}.
     */
    public static StoredScriptSource parse(BytesReference content, XContentType xContentType) {
        try (XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY, content)) {
            Token token = parser.nextToken();

            if (token != Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [{]");
            }

            token = parser.nextToken();

            if (token == Token.END_OBJECT) {
                return new StoredScriptSource(Script.DEFAULT_TEMPLATE_LANG, "", Collections.emptyMap());
            }

            if (token != Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + ", expected [" +
                    SCRIPT_PARSE_FIELD.getPreferredName() + ", " + TEMPLATE_PARSE_FIELD.getPreferredName());
            }

            String name = parser.currentName();

            if (SCRIPT_PARSE_FIELD.getPreferredName().equals(name)) {
                token = parser.nextToken();

                if (token == Token.START_OBJECT) {
                    return PARSER.apply(parser, null).build();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [{, <source>]");
                }
            } else {
                if (TEMPLATE_PARSE_FIELD.getPreferredName().equals(name)) {
                    token = parser.nextToken();

                    if (token == Token.VALUE_STRING) {
                        return new StoredScriptSource(Script.DEFAULT_TEMPLATE_LANG, parser.text(), Collections.emptyMap());
                    }
                }

                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    if (token != Token.START_OBJECT) {
                        builder.startObject();
                        builder.copyCurrentStructure(parser);
                        builder.endObject();
                    } else {
                        builder.copyCurrentStructure(parser);
                    }

                    return new StoredScriptSource(Script.DEFAULT_TEMPLATE_LANG, builder.string(), Collections.emptyMap());
                }
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    /**
     * This will parse XContent into a {@link StoredScriptSource}. The following format is what will be parsed:
     *
     * {@code
     * {
     *     "script" : {
     *         "lang" : "<lang>",
     *         "source" : "<source>",
     *         "options" : {
     *             "option0" : "<option0>",
     *             "option1" : "<option1>",
     *             ...
     *         }
     *     }
     * }
     * }
     *
     * Note that the "source" parameter can also handle template parsing including from
     * a complex JSON object.
     */
    public static StoredScriptSource fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null).build();
    }

    /**
     * Required for {@link ScriptMetaData.ScriptMetadataDiff}.  Uses
     * the {@link StoredScriptSource#StoredScriptSource(StreamInput)}
     * constructor.
     */
    public static Diff<StoredScriptSource> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(StoredScriptSource::new, in);
    }

    private final String lang;
    private final String source;
    private final Map<String, String> options;

    /**
     * Constructor for use with {@link GetStoredScriptResponse}
     * to support the deprecated stored script namespace.
     */
    public StoredScriptSource(String source) {
        this.lang = null;
        this.source = Objects.requireNonNull(source);
        this.options = null;
    }

    /**
     * Standard StoredScriptSource constructor.
     * @param lang    The language to compile the script with.  Must not be {@code null}.
     * @param source  The source source to compile with.  Must not be {@code null}.
     * @param options Compiler options to be compiled with.  Must not be {@code null},
     *                use an empty {@link Map} to represent no options.
     */
    public StoredScriptSource(String lang, String source, Map<String, String> options) {
        this.lang = Objects.requireNonNull(lang);
        this.source = Objects.requireNonNull(source);
        this.options = Collections.unmodifiableMap(Objects.requireNonNull(options));
    }

    /**
     * Reads a {@link StoredScriptSource} from a stream.  Version 5.3+ will read
     * all of the lang, source, and options parameters.  For versions prior to 5.3,
     * only the source parameter will be read in as a bytes reference.
     */
    public StoredScriptSource(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            this.lang = in.readString();
            this.source = in.readString();
            @SuppressWarnings("unchecked")
            Map<String, String> options = (Map<String, String>)(Map)in.readMap();
            this.options = options;
        } else {
            this.lang = null;
            this.source = in.readBytesReference().utf8ToString();
            this.options = null;
        }
    }

    /**
     * Writes a {@link StoredScriptSource} to a stream.  Version 5.3+ will write
     * all of the lang, source, and options parameters.  For versions prior to 5.3,
     * only the source parameter will be read in as a bytes reference.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            out.writeString(lang);
            out.writeString(source);
            @SuppressWarnings("unchecked")
            Map<String, Object> options = (Map<String, Object>)(Map)this.options;
            out.writeMap(options);
        } else {
            out.writeBytesReference(new BytesArray(source));
        }
    }

    /**
     * This will write XContent from a {@link StoredScriptSource}. The following format will be written:
     *
     * {@code
     * {
     *     "script" : {
     *         "lang" : "<lang>",
     *         "source" : "<source>",
     *         "options" : {
     *             "option0" : "<option0>",
     *             "option1" : "<option1>",
     *             ...
     *         }
     *     }
     * }
     * }
     *
     * Note that the 'source' parameter can also handle templates written as complex JSON.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LANG_PARSE_FIELD.getPreferredName(), lang);
        builder.field(SOURCE_PARSE_FIELD.getPreferredName(), source);
        builder.field(OPTIONS_PARSE_FIELD.getPreferredName(), options);
        builder.endObject();

        return builder;
    }

    /**
     * @return The language used for compiling this script.
     */
    public String getLang() {
        return lang;
    }

    /**
     * @return The source used for compiling this script.
     */
    public String getSource() {
        return source;
    }

    /**
     * @return The compiler options used for this script.
     */
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoredScriptSource that = (StoredScriptSource)o;

        if (lang != null ? !lang.equals(that.lang) : that.lang != null) return false;
        if (source != null ? !source.equals(that.source) : that.source != null) return false;
        return options != null ? options.equals(that.options) : that.options == null;

    }

    @Override
    public int hashCode() {
        int result = lang != null ? lang.hashCode() : 0;
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + (options != null ? options.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "StoredScriptSource{" +
            "lang='" + lang + '\'' +
            ", source='" + source + '\'' +
            ", options=" + options +
            '}';
    }
}
