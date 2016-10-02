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
import com.github.mustachejava.codes.IterableCode;
import com.github.mustachejava.codes.WriteCode;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomMustacheFactory extends DefaultMustacheFactory {

    private final BiConsumer<String, Writer> encoder;

    public CustomMustacheFactory(boolean escaping) {
        super();
        setObjectHandler(new CustomReflectionObjectHandler());
        if (escaping) {
            this.encoder = new JsonEscapeEncoder();
        } else {
            this.encoder = new NoEscapeEncoder();
        }
    }

    @Override
    public void encode(String value, Writer writer) {
        encoder.accept(value, writer);
    }

    @Override
    public MustacheVisitor createMustacheVisitor() {
        return new CustomMustacheVisitor(this);
    }

    class CustomMustacheVisitor extends DefaultMustacheVisitor {

        public CustomMustacheVisitor(DefaultMustacheFactory df) {
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

        public CustomCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String code) {
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

        public ToJsonCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String variable) {
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
                    if (resolved == null) {
                        builder.nullValue();
                    } else if (resolved instanceof Iterable) {
                        builder.startArray();
                        for (Object o : (Iterable) resolved) {
                            builder.value(o);
                        }
                        builder.endArray();
                    } else if (resolved instanceof Map) {
                        builder.map((Map<String, ?>) resolved);
                    } else {
                        // Do not handle as JSON
                        return oh.stringify(resolved);
                    }
                    return builder.string();
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

        public JoinerCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String delimiter) {
            super(tc, df, mustache, CODE);
            this.delimiter = delimiter;
        }

        public JoinerCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache) {
            this(tc, df, mustache, DEFAULT_DELIMITER);
        }

        @Override
        protected Function<String, String> createFunction(Object resolved) {
            return s -> {
                if (s == null) {
                    return null;
                } else if (resolved instanceof Iterable) {
                    StringJoiner joiner = new StringJoiner(delimiter);
                    for (Object o : (Iterable) resolved) {
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

        public CustomJoinerCode(TemplateContext tc, DefaultMustacheFactory df, Mustache mustache, String variable) {
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

    class NoEscapeEncoder implements BiConsumer<String, Writer> {

        @Override
        public void accept(String s, Writer writer) {
            try {
                writer.write(s);
            } catch (IOException e) {
                throw new MustacheException("Failed to encode value: " + s);
            }
        }
    }

    class JsonEscapeEncoder implements BiConsumer<String, Writer> {

        @Override
        public void accept(String s, Writer writer) {
            try {
                writer.write(JsonStringEncoder.getInstance().quoteAsString(s));
            } catch (IOException e) {
                throw new MustacheException("Failed to escape and encode value: " + s);
            }
        }
    }
}
