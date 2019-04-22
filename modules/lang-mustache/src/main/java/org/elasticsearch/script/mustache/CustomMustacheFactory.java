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

package org.elasticsearch.script.mustache;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.github.mustachejava.Code;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.DefaultMustacheVisitor;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.MustacheVisitor;
import com.github.mustachejava.TemplateContext;
import com.github.mustachejava.codes.DefaultMustache;
import com.github.mustachejava.codes.IterableCode;
import com.github.mustachejava.codes.WriteCode;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomMustacheFactory extends DefaultMustacheFactory {

    static final String JSON_MIME_TYPE_WITH_CHARSET = "application/json; charset=UTF-8";
    static final String JSON_MIME_TYPE = "application/json";
    static final String PLAIN_TEXT_MIME_TYPE = "text/plain";
    static final String X_WWW_FORM_URLENCODED_MIME_TYPE = "application/x-www-form-urlencoded";

    private static final String DEFAULT_MIME_TYPE = JSON_MIME_TYPE;

    private static final Map<String, Supplier<Encoder>> ENCODERS = Map.of(
            JSON_MIME_TYPE_WITH_CHARSET, JsonEscapeEncoder::new,
            JSON_MIME_TYPE, JsonEscapeEncoder::new,
            PLAIN_TEXT_MIME_TYPE, DefaultEncoder::new,
            X_WWW_FORM_URLENCODED_MIME_TYPE, UrlEncoder::new);

    private final Encoder encoder;

    public CustomMustacheFactory(String mimeType) {
        super();
        setObjectHandler(new CustomReflectionObjectHandler());
        this.encoder = createEncoder(mimeType);
    }

    public CustomMustacheFactory() {
        this(DEFAULT_MIME_TYPE);
    }

    @Override
    public void encode(String value, Writer writer) {
        try {
            encoder.encode(value, writer);
        } catch (IOException e) {
            throw new MustacheException("Unable to encode value", e);
        }
    }

    static Encoder createEncoder(String mimeType) {
        final Supplier<Encoder> supplier = ENCODERS.get(mimeType);
        if (supplier == null) {
            throw new IllegalArgumentException("No encoder found for MIME type [" + mimeType + "]");
        }
        return supplier.get();
    }

    @Override
    public MustacheVisitor createMustacheVisitor() {
        return new CustomMustacheVisitor(this);
    }

    class CustomMustacheVisitor extends DefaultMustacheVisitor {

        CustomMustacheVisitor(DefaultMustacheFactory df) {
            super(df);
        }

        @Override
        public void iterable(TemplateContext templateContext, String variable, Mustache mustache) {
            if (ToJsonCode.match(variable)) {
                list.add(new ToJsonCode(templateContext, df, mustache, variable));
            } else if (JoinerCode.match(variable)) {
                list.add(new JoinerCode(templateContext, df, mustache));
            } else if (CustomJoinerCode.match(variable)) {
                list.add(new CustomJoinerCode(templateContext, df, mustache, variable));
            } else if (UrlEncoderCode.match(variable)) {
                list.add(new UrlEncoderCode(templateContext, df, mustache, variable));
            } else {
                list.add(new IterableCode(templateContext, df, mustache, variable));
            }
        }
    }

    /**
     * Base class for custom Mustache functions
     */
    abstract static class CustomCode extends IterableCode {

        private final String code;

        CustomCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String code) {
            super(tc, df, mustache, extractVariableName(code, mustache, tc));
            this.code = Objects.requireNonNull(code);
        }

        @Override
        public Writer execute(Writer writer, final List<Object> scopes) {
            Object resolved = get(scopes);
            writer = handle(writer, createFunction(resolved), scopes);
            appendText(writer);
            return writer;
        }

        @Override
        protected void tag(Writer writer, String tag) throws IOException {
            writer.write(tc.startChars());
            writer.write(tag);
            writer.write(code);
            writer.write(tc.endChars());
        }

        protected abstract Function<String, String> createFunction(Object resolved);

        /**
         * At compile time, this function extracts the name of the variable:
         * {{#toJson}}variable_name{{/toJson}}
         */
        protected static String extractVariableName(String fn, Mustache mustache, TemplateContext tc) {
            Code[] codes = mustache.getCodes();
            if (codes == null || codes.length != 1) {
                throw new MustacheException("Mustache function [" + fn + "] must contain one and only one identifier");
            }

            try (StringWriter capture = new StringWriter()) {
                // Variable name is in plain text and has type WriteCode
                if (codes[0] instanceof WriteCode) {
                    codes[0].execute(capture, Collections.emptyList());
                    return capture.toString();
                } else {
                    codes[0].identity(capture);
                    return capture.toString();
                }
            } catch (IOException e) {
                throw new MustacheException("Exception while parsing mustache function [" + fn + "] at line " + tc.line(), e);
            }
        }
    }

    /**
     * This function renders {@link Iterable} and {@link Map} as their JSON representation
     */
    static class ToJsonCode extends CustomCode {

        private static final String CODE = "toJson";

        ToJsonCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String variable) {
            super(tc, df, mustache, CODE);
            if (CODE.equalsIgnoreCase(variable) == false) {
                throw new MustacheException("Mismatch function code [" + CODE + "] cannot be applied to [" + variable + "]");
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Function<String, String> createFunction(Object resolved) {
            return s -> {
                if (resolved == null) {
                    return null;
                }
                try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    if (resolved instanceof Iterable) {
                        builder.startArray();
                        for (Object o : (Iterable<?>) resolved) {
                            builder.value(o);
                        }
                        builder.endArray();
                    } else if (resolved instanceof Map) {
                        builder.map((Map<String, ?>) resolved);
                    } else {
                        // Do not handle as JSON
                        return oh.stringify(resolved);
                    }
                    return Strings.toString(builder);
                } catch (IOException e) {
                    throw new MustacheException("Failed to convert object to JSON", e);
                }
            };
        }

        static boolean match(String variable) {
            return CODE.equalsIgnoreCase(variable);
        }
    }

    /**
     * This function concatenates the values of an {@link Iterable} using a given delimiter
     */
    static class JoinerCode extends CustomCode {

        protected static final String CODE = "join";
        private static final String DEFAULT_DELIMITER = ",";

        private final String delimiter;

        JoinerCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String delimiter) {
            super(tc, df, mustache, CODE);
            this.delimiter = delimiter;
        }

        JoinerCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache) {
            this(tc, df, mustache, DEFAULT_DELIMITER);
        }

        @Override
        protected Function<String, String> createFunction(Object resolved) {
            return s -> {
                if (s == null) {
                    return null;
                } else if (resolved instanceof Iterable) {
                    StringJoiner joiner = new StringJoiner(delimiter);
                    for (Object o : (Iterable<?>) resolved) {
                        joiner.add(oh.stringify(o));
                    }
                    return joiner.toString();
                }
                return s;
            };
        }

        static boolean match(String variable) {
            return CODE.equalsIgnoreCase(variable);
        }
    }

    static class CustomJoinerCode extends JoinerCode {

        private static final Pattern PATTERN = Pattern.compile("^(?:" + CODE + " delimiter='(.*)')$");

        CustomJoinerCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String variable) {
            super(tc, df, mustache, extractDelimiter(variable));
        }

        private static String extractDelimiter(String variable) {
            Matcher matcher = PATTERN.matcher(variable);
            if (matcher.find()) {
                return matcher.group(1);
            }
            throw new MustacheException("Failed to extract delimiter for join function");
        }

        static boolean match(String variable) {
            return PATTERN.matcher(variable).matches();
        }
    }

    /**
     * This function encodes a string using the {@link URLEncoder#encode(String, String)} method
     * with the UTF-8 charset.
     */
    static class UrlEncoderCode extends DefaultMustache {

        private static final String CODE = "url";
        private final Encoder encoder;

        UrlEncoderCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String variable) {
            super(tc, df, mustache.getCodes(), variable);
            this.encoder = new UrlEncoder();
        }

        @Override
        public Writer run(Writer writer, List<Object> scopes) {
            if (getCodes() != null) {
                for (Code code : getCodes()) {
                    try (StringWriter capture = new StringWriter()) {
                        code.execute(capture, scopes);

                        String s = capture.toString();
                        if (s != null) {
                            encoder.encode(s, writer);
                        }
                    } catch (IOException e) {
                        throw new MustacheException("Exception while parsing mustache function at line " + tc.line(), e);
                    }
                }
            }
            return writer;
        }

        static boolean match(String variable) {
            return CODE.equalsIgnoreCase(variable);
        }
    }

    @FunctionalInterface
    interface Encoder {
        /**
         * Encodes the {@code s} string and writes it to the {@code writer} {@link Writer}.
         *
         * @param s      The string to encode
         * @param writer The {@link Writer} to which the encoded string will be written to
         */
        void encode(String s, Writer writer) throws IOException;
    }

    /**
     * Encoder that simply writes the string to the writer without encoding.
     */
    static class DefaultEncoder implements Encoder {

        @Override
        public void encode(String s, Writer writer) throws IOException {
            writer.write(s);
        }
    }

    /**
     * Encoder that escapes JSON string values/fields.
     */
    static class JsonEscapeEncoder implements Encoder {

        @Override
        public void encode(String s, Writer writer) throws IOException {
            writer.write(JsonStringEncoder.getInstance().quoteAsString(s));
        }
    }

    /**
     * Encoder that escapes strings using HTML form encoding
     */
    static class UrlEncoder implements Encoder {

        @Override
        public void encode(String s, Writer writer) throws IOException {
            writer.write(URLEncoder.encode(s, StandardCharsets.UTF_8.name()));
        }
    }
}
