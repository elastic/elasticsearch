/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link StoredScriptSource} represents user-defined parameters for a script
 * saved in the {@link ClusterState}.
 */
public class StoredScriptSource implements SimpleDiffable<StoredScriptSource>, Writeable, ToXContentObject {
    /**
     * Standard {@link ParseField} for outer level of stored script source.
     */
    public static final ParseField SCRIPT_PARSE_FIELD = new ParseField("script");

    /**
     * Standard {@link ParseField} for lang on the inner level.
     */
    public static final ParseField LANG_PARSE_FIELD = new ParseField("lang");

    /**
     * Standard {@link ParseField} for source on the inner level.
     */
    public static final ParseField SOURCE_PARSE_FIELD = new ParseField("source");

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
                    // this is really for search templates, that need to be converted to json format
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    source = Strings.toString(builder.copyCurrentStructure(parser));
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
         *
         * @param ignoreEmpty Specify as {@code true} to ignoreEmpty the empty source check.
         *                    This allow empty templates to be dropped for backwards compatibility.
         */
        private StoredScriptSource build(boolean ignoreEmpty) {
            if (lang == null) {
                throw new IllegalArgumentException("must specify lang for stored script");
            } else if (lang.isEmpty()) {
                throw new IllegalArgumentException("lang cannot be empty");
            }

            if (source == null) {
                if (ignoreEmpty) {
                    source = "";
                } else {
                    throw new IllegalArgumentException("must specify source for stored script");
                }
            } else if (source.isEmpty() && ignoreEmpty == false) {
                throw new IllegalArgumentException("source cannot be empty");
            }

            if (options.size() > 1 || options.size() == 1 && options.get(Script.CONTENT_TYPE_OPTION) == null) {
                throw new IllegalArgumentException("illegal compiler options [" + options + "] specified");
            }

            return new StoredScriptSource(lang, source, options);
        }
    }

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("stored script source", true, Builder::new);

    static {
        // Defines the fields necessary to parse a Script as XContent using an ObjectParser.
        PARSER.declareString(Builder::setLang, LANG_PARSE_FIELD);
        PARSER.declareField(Builder::setSource, parser -> parser, SOURCE_PARSE_FIELD, ValueType.OBJECT_OR_STRING);
        PARSER.declareField(Builder::setOptions, XContentParser::mapStrings, OPTIONS_PARSE_FIELD, ValueType.OBJECT);
    }

    /**
     * This will parse XContent into a {@link StoredScriptSource}.
     *
     * Examples of legal JSON:
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
     * Note that the "source" parameter can also handle template parsing including from
     * a complex JSON object.
     *
     * @param content The content from the request to be parsed as described above.
     * @return        The parsed {@link StoredScriptSource}.
     */
    public static StoredScriptSource parse(BytesReference content, XContentType xContentType) {
        try (
            InputStream stream = content.streamInput();
            XContentParser parser = xContentType.xContent()
                .createParser(XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE), stream)
        ) {
            Token token = parser.nextToken();

            if (token != Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [{]");
            }

            token = parser.nextToken();

            if (token != Token.FIELD_NAME) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "unexpected token [" + token + ", expected [" + SCRIPT_PARSE_FIELD.getPreferredName() + "]"
                );
            }

            String name = parser.currentName();

            if (SCRIPT_PARSE_FIELD.getPreferredName().equals(name)) {
                token = parser.nextToken();

                if (token == Token.START_OBJECT) {
                    return PARSER.apply(parser, null).build(false);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [{, <source>]");
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "unexpected field [" + name + "], expected [" + SCRIPT_PARSE_FIELD.getPreferredName() + "]"
                );
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
     *
     * @param ignoreEmpty Specify as {@code true} to ignoreEmpty the empty source check.
     *                    This allows empty templates to be loaded for backwards compatibility.
     */
    public static StoredScriptSource fromXContent(XContentParser parser, boolean ignoreEmpty) {
        return PARSER.apply(parser, null).build(ignoreEmpty);
    }

    /**
     * Required for {@link ScriptMetadata.ScriptMetadataDiff}.  Uses
     * the {@link StoredScriptSource#StoredScriptSource(StreamInput)}
     * constructor.
     */
    public static Diff<StoredScriptSource> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(StoredScriptSource::new, in);
    }

    private final String lang;
    private final String source;
    private final Map<String, String> options;

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
        this.lang = in.readString();
        this.source = in.readString();
        @SuppressWarnings("unchecked")
        Map<String, String> options = (Map<String, String>) (Map) in.readMap();
        this.options = options;
    }

    /**
     * Writes a {@link StoredScriptSource} to a stream. Will write
     * all of the lang, source, and options parameters.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(lang);
        out.writeString(source);
        @SuppressWarnings("unchecked")
        Map<String, Object> options = (Map<String, Object>) (Map) this.options;
        out.writeGenericMap(options);
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
        if (options.isEmpty() == false) {
            builder.field(OPTIONS_PARSE_FIELD.getPreferredName(), options);
        }
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

        StoredScriptSource that = (StoredScriptSource) o;

        return Objects.equals(lang, that.lang) && Objects.equals(source, that.source) && Objects.equals(options, that.options);
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
        return "StoredScriptSource{" + "lang='" + lang + '\'' + ", source='" + source + '\'' + ", options=" + options + '}';
    }
}
