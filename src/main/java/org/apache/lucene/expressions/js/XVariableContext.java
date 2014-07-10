package org.apache.lucene.expressions.js;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;

/**
 * A helper to parse the context of a variable name, which is the base variable, followed by the
 * sequence of array (integer or string indexed) and member accesses.
 */
public class XVariableContext {

    static {
        assert org.elasticsearch.Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_4_9: "Remove this code once we upgrade to Lucene 4.10 (LUCENE-5806)";
    }

    public static enum Type {
        MEMBER,         // "dot" access
        STR_INDEX,      // brackets with a string
        INT_INDEX       // brackets with a positive integer
    }

    public final Type type;
    public final String text;
    public final int integer;

    private XVariableContext(Type c, String s, int i) {
        type = c;
        text = s;
        integer = i;
    }

    /**
     * Parses a normalized javascript variable. All strings in the variable should be single quoted,
     * and no spaces (except possibly within strings).
     */
    public static final XVariableContext[] parse(String variable) {
        char[] text = variable.toCharArray();
        List<XVariableContext> contexts = new ArrayList<>();
        int i = addMember(text, 0, contexts); // base variable is a "member" of the global namespace
        while (i < text.length) {
            if (text[i] == '[') {
                if (text[++i] == '\'') {
                    i = addStringIndex(text, i, contexts);
                } else {
                    i = addIntIndex(text, i, contexts);
                }
                ++i; // move past end bracket
            } else { // text[i] == '.', ie object member
                i = addMember(text, i + 1, contexts);
            }
        }
        return contexts.toArray(new XVariableContext[contexts.size()]);
    }

    // i points to start of member name
    private static int addMember(final char[] text, int i, List<XVariableContext> contexts) {
        int j = i + 1;
        while (j < text.length && text[j] != '[' && text[j] != '.') ++j; // find first array or member access
        contexts.add(new XVariableContext(Type.MEMBER, new String(text, i, j - i), -1));
        return j;
    }

    // i points to start of single quoted index
    private static int addStringIndex(final char[] text, int i, List<XVariableContext> contexts) {
        ++i; // move past quote
        int j = i;
        while (text[j] != '\'') { // find end of single quoted string
            if (text[j] == '\\') ++j; // skip over escapes
            ++j;
        }
        StringBuffer buf = new StringBuffer(j - i); // space for string, without end quote
        while (i < j) { // copy string to buffer (without begin/end quotes)
            if (text[i] == '\\') ++i; // unescape escapes
            buf.append(text[i]);
            ++i;
        }
        contexts.add(new XVariableContext(Type.STR_INDEX, buf.toString(), -1));
        return j + 1; // move past quote, return end bracket location
    }

    // i points to start of integer index
    private static int addIntIndex(final char[] text, int i, List<XVariableContext> contexts) {
        int j = i + 1;
        while (text[j] != ']') ++j; // find end of array access
        int index = Integer.parseInt(new String(text, i, j - i));
        contexts.add(new XVariableContext(Type.INT_INDEX, null, index));
        return j ;
    }
}
