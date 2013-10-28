package org.apache.lucene.search.postingshighlight;

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

import org.elasticsearch.Version;

/**
 * Creates a formatted snippet from the top passages.
 * <p>
 * The default implementation marks the query terms as bold, and places
 * ellipses between unconnected passages.
 */
//LUCENE MONITOR - REMOVE ME WHEN LUCENE 4.6 IS OUT
//Applied LUCENE-4906 to be able to return arbitrary objects
public class XDefaultPassageFormatter extends XPassageFormatter {

    static {
        assert Version.CURRENT.luceneVersion.compareTo(org.apache.lucene.util.Version.LUCENE_45) == 0 : "Remove XDefaultPassageFormatter once 4.6 is out";
    }

    /** text that will appear before highlighted terms */
    protected final String preTag;
    /** text that will appear after highlighted terms */
    protected final String postTag;
    /** text that will appear between two unconnected passages */
    protected final String ellipsis;
    /** true if we should escape for html */
    protected final boolean escape;

    /**
     * Creates a new DefaultPassageFormatter with the default tags.
     */
    public XDefaultPassageFormatter() {
        this("<b>", "</b>", "... ", false);
    }

    /**
     * Creates a new DefaultPassageFormatter with custom tags.
     * @param preTag text which should appear before a highlighted term.
     * @param postTag text which should appear after a highlighted term.
     * @param ellipsis text which should be used to connect two unconnected passages.
     * @param escape true if text should be html-escaped
     */
    public XDefaultPassageFormatter(String preTag, String postTag, String ellipsis, boolean escape) {
        if (preTag == null || postTag == null || ellipsis == null) {
            throw new NullPointerException();
        }
        this.preTag = preTag;
        this.postTag = postTag;
        this.ellipsis = ellipsis;
        this.escape = escape;
    }

    @Override
    public String format(Passage passages[], String content) {
        StringBuilder sb = new StringBuilder();
        int pos = 0;
        for (Passage passage : passages) {
            // don't add ellipsis if its the first one, or if its connected.
            if (passage.startOffset > pos && pos > 0) {
                sb.append(ellipsis);
            }
            pos = passage.startOffset;
            for (int i = 0; i < passage.numMatches; i++) {
                int start = passage.matchStarts[i];
                int end = passage.matchEnds[i];
                // its possible to have overlapping terms
                if (start > pos) {
                    append(sb, content, pos, start);
                }
                if (end > pos) {
                    sb.append(preTag);
                    append(sb, content, Math.max(pos, start), end);
                    sb.append(postTag);
                    pos = end;
                }
            }
            // its possible a "term" from the analyzer could span a sentence boundary.
            append(sb, content, pos, Math.max(pos, passage.endOffset));
            pos = passage.endOffset;
        }
        return sb.toString();
    }

    /**
     * Appends original text to the response.
     * @param dest resulting text, possibly transformed or encoded
     * @param content original text content
     * @param start index of the first character in content
     * @param end index of the character following the last character in content
     */
    protected void append(StringBuilder dest, String content, int start, int end) {
        if (escape) {
            // note: these are the rules from owasp.org
            for (int i = start; i < end; i++) {
                char ch = content.charAt(i);
                switch(ch) {
                    case '&':
                        dest.append("&amp;");
                        break;
                    case '<':
                        dest.append("&lt;");
                        break;
                    case '>':
                        dest.append("&gt;");
                        break;
                    case '"':
                        dest.append("&quot;");
                        break;
                    case '\'':
                        dest.append("&#x27;");
                        break;
                    case '/':
                        dest.append("&#x2F;");
                        break;
                    default:
                        if (ch >= 0x30 && ch <= 0x39 || ch >= 0x41 && ch <= 0x5A || ch >= 0x61 && ch <= 0x7A) {
                            dest.append(ch);
                        } else if (ch < 0xff) {
                            dest.append("&#");
                            dest.append((int)ch);
                            dest.append(";");
                        } else {
                            dest.append(ch);
                        }
                }
            }
        } else {
            dest.append(content, start, end);
        }
    }
}
