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

package org.apache.lucene.search.uhighlight;

import org.apache.lucene.search.highlight.Encoder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightUtils;

/**
 * Custom passage formatter that allows us to:
 * 1) extract different snippets (instead of a single big string) together with their scores ({@link Snippet})
 * 2) use the {@link Encoder} implementations that are already used with the other highlighters
 */
public class CustomPassageFormatter extends PassageFormatter {

    private final String preTag;
    private final String postTag;
    private final Encoder encoder;

    public CustomPassageFormatter(String preTag, String postTag, Encoder encoder) {
        this.preTag = preTag;
        this.postTag = postTag;
        this.encoder = encoder;
    }

    @Override
    public Snippet[] format(Passage[] passages, String content) {
        Snippet[] snippets = new Snippet[passages.length];
        int pos;
        for (int j = 0; j < passages.length; j++) {
            Passage passage = passages[j];
            StringBuilder sb = new StringBuilder();
            pos = passage.getStartOffset();
            for (int i = 0; i < passage.getNumMatches(); i++) {
                int start = passage.getMatchStarts()[i];
                assert start >= pos && start < passage.getEndOffset();
                // append content before this start
                append(sb, content, pos, start);

                int end = passage.getMatchEnds()[i];
                assert end > start;
                // Look ahead to expand 'end' past all overlapping:
                while (i + 1 < passage.getNumMatches() && passage.getMatchStarts()[i + 1] < end) {
                    end = passage.getMatchEnds()[++i];
                }
                end = Math.min(end, passage.getEndOffset()); // in case match straddles past passage

                sb.append(preTag);
                append(sb, content, start, end);
                sb.append(postTag);

                pos = end;
            }
            // its possible a "term" from the analyzer could span a sentence boundary.
            append(sb, content, pos, Math.max(pos, passage.getEndOffset()));
            //we remove the paragraph separator if present at the end of the snippet (we used it as separator between values)
            if (sb.charAt(sb.length() - 1) == HighlightUtils.PARAGRAPH_SEPARATOR) {
                sb.deleteCharAt(sb.length() - 1);
            } else if (sb.charAt(sb.length() - 1) == HighlightUtils.NULL_SEPARATOR) {
                sb.deleteCharAt(sb.length() - 1);
            }
            //and we trim the snippets too
            snippets[j] = new Snippet(sb.toString().trim(), passage.getScore(), passage.getNumMatches() > 0);
        }
        return snippets;
    }

    private void append(StringBuilder dest, String content, int start, int end) {
        dest.append(encoder.encodeText(content.substring(start, end)));
    }
}
