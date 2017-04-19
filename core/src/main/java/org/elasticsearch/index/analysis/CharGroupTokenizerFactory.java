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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.util.HashSet;
import java.util.Set;

public class CharGroupTokenizerFactory extends AbstractTokenizerFactory{

    private final Set<Integer> tokenizeOnChars = new HashSet<>();
    private boolean tokenizeOnSpace = false;
    private boolean tokenizeOnLetter = false;
    private boolean tokenizeOnDigit = false;

    public CharGroupTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);

        char[] chars = parseCharsList(settings.get("tokenize_on_chars"));
        if (chars != null) {
            for (char c : chars) {
                tokenizeOnChars.add((int) c);
            }
        }
    }

    private char[] parseCharsList(final String s) {
        char[] out = new char[256];
        int readPos = 0;
        int len = s.length();
        int writePos = 0;
        while (readPos < len) {
            char c = s.charAt(readPos++);
            if (c == '\\') {
                if (readPos >= len)
                    throw new RuntimeException("Invalid escaped char in [" + s + "]");
                c = s.charAt(readPos++);
                switch (c) {
                    case '\\':
                        c = '\\';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                    case 'u':
                        if (readPos + 3 >= len)
                            throw new RuntimeException("Invalid escaped char in [" + s + "]");
                        c = (char) Integer.parseInt(s.substring(readPos, readPos + 4), 16);
                        readPos += 4;
                        break;
                    case 's':
                        tokenizeOnSpace = true;
                        writePos++;
                        continue;
                    case 'd':
                        tokenizeOnDigit = true;
                        writePos++;
                        continue;
                    case 'w':
                        tokenizeOnLetter = true;
                        writePos++;
                        continue;
                    default:
                        throw new RuntimeException("Invalid escaped char " + c + " in [" + s + "]");
                }
            }
            out[writePos++] = c;
        }
        return out;
    }

    @Override
    public Tokenizer create() {
        return new CharTokenizer() {
            @Override
            protected boolean isTokenChar(int c) {
                if (tokenizeOnSpace && Character.isWhitespace(c)) {
                    return false;
                }
                if (tokenizeOnLetter && Character.isLetter(c)) {
                    return false;
                }
                if (tokenizeOnDigit && Character.isDigit(c)) {
                    return false;
                }
                // TODO also support PUNCTUATION and SYMBOL a la CharMatcher ?
                return !tokenizeOnChars.contains(c);
            }
        };
    }
}
