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

package org.elasticsearch.search.highlight;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <pre>
 * highlight : {
 *  tags_schema : "styled",
 *  pre_tags : ["tag1", "tag2"],
 *  post_tags : ["tag1", "tag2"],
 *  order : "score",
 *  highlight_filter : true,
 *  fields : {
 *      field1 : {  },
 *      field2 : { fragment_size : 100, number_of_fragments : 2 },
 *      field3 : { number_of_fragments : 5, order : "simple", tags_schema : "styled" },
 *      field4 : { number_of_fragments: 0, pre_tags : ["openingTagA", "openingTagB"], post_tags : ["closingTag"] }
 *  }
 * }
 * </pre>
 */
public class HighlighterParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        try {
            context.highlight(parse(parser, context.indexShard().getQueryShardContext()));
        } catch (IllegalArgumentException ex) {
            throw new SearchParseException(context, "Error while trying to parse Highlighter element in request", parser.getTokenLocation());
        }
    }

    public SearchContextHighlight parse(XContentParser parser, QueryShardContext queryShardContext) throws IOException {
        XContentParser.Token token;
        String topLevelFieldName = null;
        final List<Tuple<String, SearchContextHighlight.FieldOptions.Builder>> fieldsOptions = new ArrayList<>();

        final SearchContextHighlight.FieldOptions.Builder globalOptionsBuilder = HighlightBuilder.defaultFieldOptions();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("pre_tags".equals(topLevelFieldName) || "preTags".equals(topLevelFieldName)) {
                    List<String> preTagsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        preTagsList.add(parser.text());
                    }
                    globalOptionsBuilder.preTags(preTagsList.toArray(new String[preTagsList.size()]));
                } else if ("post_tags".equals(topLevelFieldName) || "postTags".equals(topLevelFieldName)) {
                    List<String> postTagsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        postTagsList.add(parser.text());
                    }
                    globalOptionsBuilder.postTags(postTagsList.toArray(new String[postTagsList.size()]));
                } else if ("fields".equals(topLevelFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.START_OBJECT) {
                            String highlightFieldName = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    if (highlightFieldName != null) {
                                        throw new IllegalArgumentException("If highlighter fields is an array it must contain objects containing a single field");
                                    }
                                    highlightFieldName = parser.currentName();
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    fieldsOptions.add(Tuple.tuple(highlightFieldName, parseFields(parser, queryShardContext)));
                                }
                            }
                        } else {
                            throw new IllegalArgumentException("If highlighter fields is an array it must contain objects containing a single field");
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("order".equals(topLevelFieldName)) {
                    globalOptionsBuilder.scoreOrdered("score".equals(parser.text()));
                } else if ("tags_schema".equals(topLevelFieldName) || "tagsSchema".equals(topLevelFieldName)) {
                    String schema = parser.text();
                    if ("styled".equals(schema)) {
                        globalOptionsBuilder.preTags(HighlightBuilder.DEFAULT_STYLED_PRE_TAG);
                        globalOptionsBuilder.postTags(HighlightBuilder.DEFAULT_STYLED_POST_TAGS);
                    }
                } else if ("highlight_filter".equals(topLevelFieldName) || "highlightFilter".equals(topLevelFieldName)) {
                    globalOptionsBuilder.highlightFilter(parser.booleanValue());
                } else if ("fragment_size".equals(topLevelFieldName) || "fragmentSize".equals(topLevelFieldName)) {
                    globalOptionsBuilder.fragmentCharSize(parser.intValue());
                } else if ("number_of_fragments".equals(topLevelFieldName) || "numberOfFragments".equals(topLevelFieldName)) {
                    globalOptionsBuilder.numberOfFragments(parser.intValue());
                } else if ("encoder".equals(topLevelFieldName)) {
                    globalOptionsBuilder.encoder(parser.text());
                } else if ("require_field_match".equals(topLevelFieldName) || "requireFieldMatch".equals(topLevelFieldName)) {
                    globalOptionsBuilder.requireFieldMatch(parser.booleanValue());
                } else if ("boundary_max_scan".equals(topLevelFieldName) || "boundaryMaxScan".equals(topLevelFieldName)) {
                    globalOptionsBuilder.boundaryMaxScan(parser.intValue());
                } else if ("boundary_chars".equals(topLevelFieldName) || "boundaryChars".equals(topLevelFieldName)) {
                    char[] charsArr = parser.text().toCharArray();
                    Character[] globalBoundaryChars = new Character[charsArr.length];
                    for (int i = 0; i < charsArr.length; i++) {
                        globalBoundaryChars[i] = charsArr[i];
                    }
                    globalOptionsBuilder.boundaryChars(globalBoundaryChars);
                } else if ("type".equals(topLevelFieldName)) {
                    globalOptionsBuilder.highlighterType(parser.text());
                } else if ("fragmenter".equals(topLevelFieldName)) {
                    globalOptionsBuilder.fragmenter(parser.text());
                } else if ("no_match_size".equals(topLevelFieldName) || "noMatchSize".equals(topLevelFieldName)) {
                    globalOptionsBuilder.noMatchSize(parser.intValue());
                } else if ("force_source".equals(topLevelFieldName) || "forceSource".equals(topLevelFieldName)) {
                    globalOptionsBuilder.forceSource(parser.booleanValue());
                } else if ("phrase_limit".equals(topLevelFieldName) || "phraseLimit".equals(topLevelFieldName)) {
                    globalOptionsBuilder.phraseLimit(parser.intValue());
                }
            } else if (token == XContentParser.Token.START_OBJECT && "options".equals(topLevelFieldName)) {
                globalOptionsBuilder.options(parser.map());
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("fields".equals(topLevelFieldName)) {
                    String highlightFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            highlightFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            fieldsOptions.add(Tuple.tuple(highlightFieldName, parseFields(parser, queryShardContext)));
                        }
                    }
                } else if ("highlight_query".equals(topLevelFieldName) || "highlightQuery".equals(topLevelFieldName)) {
                    globalOptionsBuilder.highlightQuery(queryShardContext.parse(parser).query());
                }
            }
        }

        final SearchContextHighlight.FieldOptions globalOptions = globalOptionsBuilder.build();
        if (globalOptions.preTags() != null && globalOptions.postTags() == null) {
            throw new IllegalArgumentException("Highlighter global preTags are set, but global postTags are not set");
        }

        final List<SearchContextHighlight.Field> fields = new ArrayList<>();
        // now, go over and fill all fieldsOptions with default values from the global state
        for (final Tuple<String, SearchContextHighlight.FieldOptions.Builder> tuple : fieldsOptions) {
            fields.add(new SearchContextHighlight.Field(tuple.v1(), tuple.v2().merge(globalOptions).build()));
        }
        return new SearchContextHighlight(fields);
    }

    private static SearchContextHighlight.FieldOptions.Builder parseFields(XContentParser parser, QueryShardContext queryShardContext) throws IOException {
        XContentParser.Token token;

        final SearchContextHighlight.FieldOptions.Builder fieldOptionsBuilder = new SearchContextHighlight.FieldOptions.Builder();
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("pre_tags".equals(fieldName) || "preTags".equals(fieldName)) {
                    List<String> preTagsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        preTagsList.add(parser.text());
                    }
                    fieldOptionsBuilder.preTags(preTagsList.toArray(new String[preTagsList.size()]));
                } else if ("post_tags".equals(fieldName) || "postTags".equals(fieldName)) {
                    List<String> postTagsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        postTagsList.add(parser.text());
                    }
                    fieldOptionsBuilder.postTags(postTagsList.toArray(new String[postTagsList.size()]));
                } else if ("matched_fields".equals(fieldName) || "matchedFields".equals(fieldName)) {
                    Set<String> matchedFields = new HashSet<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        matchedFields.add(parser.text());
                    }
                    fieldOptionsBuilder.matchedFields(matchedFields);
                }
            } else if (token.isValue()) {
                if ("fragment_size".equals(fieldName) || "fragmentSize".equals(fieldName)) {
                    fieldOptionsBuilder.fragmentCharSize(parser.intValue());
                } else if ("number_of_fragments".equals(fieldName) || "numberOfFragments".equals(fieldName)) {
                    fieldOptionsBuilder.numberOfFragments(parser.intValue());
                } else if ("fragment_offset".equals(fieldName) || "fragmentOffset".equals(fieldName)) {
                    fieldOptionsBuilder.fragmentOffset(parser.intValue());
                } else if ("highlight_filter".equals(fieldName) || "highlightFilter".equals(fieldName)) {
                    fieldOptionsBuilder.highlightFilter(parser.booleanValue());
                } else if ("order".equals(fieldName)) {
                    fieldOptionsBuilder.scoreOrdered("score".equals(parser.text()));
                } else if ("require_field_match".equals(fieldName) || "requireFieldMatch".equals(fieldName)) {
                    fieldOptionsBuilder.requireFieldMatch(parser.booleanValue());
                } else if ("boundary_max_scan".equals(fieldName) || "boundaryMaxScan".equals(fieldName)) {
                    fieldOptionsBuilder.boundaryMaxScan(parser.intValue());
                } else if ("boundary_chars".equals(fieldName) || "boundaryChars".equals(fieldName)) {
                    char[] charsArr = parser.text().toCharArray();
                    Character[] boundaryChars = new Character[charsArr.length];
                    for (int i = 0; i < charsArr.length; i++) {
                        boundaryChars[i] = charsArr[i];
                    }
                    fieldOptionsBuilder.boundaryChars(boundaryChars);
                } else if ("type".equals(fieldName)) {
                    fieldOptionsBuilder.highlighterType(parser.text());
                } else if ("fragmenter".equals(fieldName)) {
                    fieldOptionsBuilder.fragmenter(parser.text());
                } else if ("no_match_size".equals(fieldName) || "noMatchSize".equals(fieldName)) {
                    fieldOptionsBuilder.noMatchSize(parser.intValue());
                } else if ("force_source".equals(fieldName) || "forceSource".equals(fieldName)) {
                    fieldOptionsBuilder.forceSource(parser.booleanValue());
                } else if ("phrase_limit".equals(fieldName) || "phraseLimit".equals(fieldName)) {
                    fieldOptionsBuilder.phraseLimit(parser.intValue());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("highlight_query".equals(fieldName) || "highlightQuery".equals(fieldName)) {
                    fieldOptionsBuilder.highlightQuery(queryShardContext.parse(parser).query());
                } else if ("options".equals(fieldName)) {
                    fieldOptionsBuilder.options(parser.map());
                }
            }
        }
        return fieldOptionsBuilder;
    }
}
