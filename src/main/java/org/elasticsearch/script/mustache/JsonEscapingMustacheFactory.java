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

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheException;

import java.io.IOException;
import java.io.Writer;

/**
 * A MustacheFactory that does simple JSON escaping.
 */
public final class JsonEscapingMustacheFactory extends DefaultMustacheFactory {

    @Override
    public void encode(String value, Writer writer) {
        try {
            escape(value, writer);
        } catch (IOException e) {
            throw new MustacheException("Failed to encode value: " + value);
        }
    }

    public static Writer escape(String value, Writer writer) throws IOException {
        for (int i = 0; i < value.length(); i++) {
            final char character = value.charAt(i);
            if (isEscapeChar(character)) {
                writer.write('\\');
            }
            writer.write(character);
        }
        return writer;
    }

    public static boolean isEscapeChar(char c) {
        switch(c) {
            case '\b':
            case '\f':
            case '\n':
            case '\r':
            case '"':
            case '\\':
            case '\u000B': // vertical tab
            case '\t':
                return true;
        }
        return false;
    }

}
