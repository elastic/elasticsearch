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

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.Snippet;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText.AnnotationToken;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Custom passage formatter that :
 * 1) marks up search hits in markdown-like syntax for URLs ({@link Snippet})
 * 2) injects any annotations from the original text that don't conflict with search hit highlighting
 */
public class AnnotatedPassageFormatter extends PassageFormatter {


    public static final String SEARCH_HIT_TYPE = "_hit_term";
    private final Encoder encoder;
    AnnotatedText[] annotations;

    public AnnotatedPassageFormatter(AnnotatedText[] annotations, Encoder encoder) {
        this.encoder = encoder;
        this.annotations = annotations;
    }

    static class MarkupPassage {
        List<Markup> markups = new ArrayList<>();
        int lastMarkupEnd = -1;

        public void addUnlessOverlapping(Markup newMarkup) {
            
            // Fast exit.
            if(newMarkup.start > lastMarkupEnd) {
                markups.add(newMarkup);
                lastMarkupEnd = newMarkup.end;                 
                return;
            }
            
            // Check to see if this new markup overlaps with any prior
            int index=0;
            for (Markup existingMarkup: markups) {
                if(existingMarkup.samePosition(newMarkup)) {
                    existingMarkup.merge(newMarkup);
                    return;
                }
                if(existingMarkup.overlaps(newMarkup)) {
                    // existing markup wins - we throw away the new markup that would span this position
                    return;
                }
                // markup list is in start offset order so we can insert at this position then shift others right 
                if(existingMarkup.isAfter(newMarkup)) {
                    markups.add(index, newMarkup);
                    return;
                }
                index++;
            }
            markups.add(newMarkup);
            lastMarkupEnd = newMarkup.end; 
        }
        
    }
    static class Markup {
        int start;
        int end;
        String metadata;
        Markup(int start, int end, String metadata) {
            super();
            this.start = start;
            this.end = end;
            this.metadata = metadata;
        }
        boolean isAfter(Markup other) {
            return start > other.end;
        }
        void merge(Markup newMarkup) {
            // metadata is key1=value&key2=value&.... syntax used for urls 
            assert samePosition(newMarkup);
            metadata += "&" + newMarkup.metadata;
        }
        boolean samePosition(Markup other) {
            return this.start == other.start && this.end == other.end;
        }
        boolean overlaps(Markup other) {
            return (start<=other.start  && end >= other.start)
                    || (start <= other.end && end >=other.end)
                    || (start>=other.start && end<=other.end);
        }
        @Override
        public String toString() {
            return "Markup [start=" + start + ", end=" + end + ", metadata=" + metadata + "]";
        }
        
        
    }
    // Merge original annotations and search hits into a single set of markups for each passage
    static MarkupPassage mergeAnnotations(AnnotationToken [] annotations, Passage passage){
        try {
            MarkupPassage markupPassage = new MarkupPassage();

            // Add search hits first - they take precedence over any other markup
            for (int i = 0; i < passage.getNumMatches(); i++) {
                int start = passage.getMatchStarts()[i];
                int end = passage.getMatchEnds()[i];
                String searchTerm = passage.getMatchTerms()[i].utf8ToString();
                Markup markup = new Markup(start, end, SEARCH_HIT_TYPE+"="+URLEncoder.encode(searchTerm, StandardCharsets.UTF_8.name()));
                markupPassage.addUnlessOverlapping(markup);
            }
            
            // Now add original text's annotations - ignoring any that might conflict with the search hits markup.
            for (AnnotationToken token: annotations) {                
                int start = token.offset;
                int end = token.endOffset;
                if(start >= passage.getStartOffset() && end<=passage.getEndOffset()) {
                    String escapedValue = URLEncoder.encode(token.value, StandardCharsets.UTF_8.name());
                    Markup markup = new Markup(start, end, escapedValue);
                    markupPassage.addUnlessOverlapping(markup);                        
                }
            }
            return markupPassage;
            
        } catch (UnsupportedEncodingException e) {
            // We should always have UTF-8 support
            throw new IllegalStateException(e);
        }
    }
    

    @Override
    public Snippet[] format(Passage[] passages, String content) {
        Snippet[] snippets = new Snippet[passages.length];

        int pos;
        int j = 0;
        for (Passage passage : passages) {
            AnnotationToken [] annotations = getIntersectingAnnotations(passage.getStartOffset(), 
                    passage.getEndOffset());            
            MarkupPassage mergedMarkup = mergeAnnotations(annotations, passage);
            
            StringBuilder sb = new StringBuilder();
            pos = passage.getStartOffset();            
            for(Markup markup: mergedMarkup.markups) {
                int start = markup.start;
                int end = markup.end;
                // its possible to have overlapping terms
                if (start > pos) {
                    append(sb, content, pos, start);
                }
                if (end > pos) {
                    sb.append("[");
                    append(sb, content, Math.max(pos, start), end);
                    
                    sb.append("](");
                    sb.append(markup.metadata);
                    sb.append(")");
                    pos = end;
                }
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
            snippets[j++] = new Snippet(sb.toString().trim(), passage.getScore(), passage.getNumMatches() > 0);
        }                    
        return snippets;
    }
    
    public AnnotationToken[] getIntersectingAnnotations(int start, int end) {
        List<AnnotationToken> intersectingAnnotations = new ArrayList<>();
        int fieldValueOffset =0;
        for (AnnotatedText fieldValueAnnotations : this.annotations) {
            //This is called from a highlighter where all of the field values are concatenated
            // so each annotation offset will need to be adjusted so that it takes into account 
            // the previous values AND the MULTIVAL delimiter
            for (int i = 0; i < fieldValueAnnotations.numAnnotations(); i++) {
                AnnotationToken token = fieldValueAnnotations.getAnnotation(i);
                if (token.intersects(start - fieldValueOffset, end - fieldValueOffset)) {
                    intersectingAnnotations
                            .add(new AnnotationToken(token.offset + fieldValueOffset, token.endOffset + 
                                    fieldValueOffset, token.value));
                }
            }
            //add 1 for the fieldvalue separator character
            fieldValueOffset +=fieldValueAnnotations.textMinusMarkup.length() +1;
        }
        return intersectingAnnotations.toArray(new AnnotationToken[intersectingAnnotations.size()]);
    }     

    private void append(StringBuilder dest, String content, int start, int end) {
        dest.append(encoder.encodeText(content.substring(start, end)));
    }
}
