/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.vectorhighlight.SimpleBoundaryScanner;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;

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

    private static final String[] DEFAULT_PRE_TAGS = new String[]{"<em>"};
    private static final String[] DEFAULT_POST_TAGS = new String[]{"</em>"};

    private static final String[] STYLED_PRE_TAG = {
            "<em class=\"hlt1\">", "<em class=\"hlt2\">", "<em class=\"hlt3\">",
            "<em class=\"hlt4\">", "<em class=\"hlt5\">", "<em class=\"hlt6\">",
            "<em class=\"hlt7\">", "<em class=\"hlt8\">", "<em class=\"hlt9\">",
            "<em class=\"hlt10\">"
    };
    private static final String[] STYLED_POST_TAGS = {"</em>"};

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;
        String topLevelFieldName = null;
        List<SearchContextHighlight.Field> fields = newArrayList();

        String[] globalPreTags = DEFAULT_PRE_TAGS;
        String[] globalPostTags = DEFAULT_POST_TAGS;
        boolean globalScoreOrdered = false;
        boolean globalHighlightFilter = false;
        boolean globalRequireFieldMatch = false;
        int globalFragmentSize = 100;
        int globalNumOfFragments = 5;
        String globalEncoder = "default";
        int globalBoundaryMaxScan = SimpleBoundaryScanner.DEFAULT_MAX_SCAN;
        Character[] globalBoundaryChars = SimpleBoundaryScanner.DEFAULT_BOUNDARY_CHARS;
        String globalHighlighterType = null;
        String globalFragmenter = null;
        Map<String, Object> globalOptions = null;
        Query globalHighlightQuery = null;
        int globalNoMatchSize = 0;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("pre_tags".equals(topLevelFieldName) || "preTags".equals(topLevelFieldName)) {
                    List<String> preTagsList = Lists.newArrayList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        preTagsList.add(parser.text());
                    }
                    globalPreTags = preTagsList.toArray(new String[preTagsList.size()]);
                } else if ("post_tags".equals(topLevelFieldName) || "postTags".equals(topLevelFieldName)) {
                    List<String> postTagsList = Lists.newArrayList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        postTagsList.add(parser.text());
                    }
                    globalPostTags = postTagsList.toArray(new String[postTagsList.size()]);
                }
            } else if (token.isValue()) {
                if ("order".equals(topLevelFieldName)) {
                    globalScoreOrdered = "score".equals(parser.text());
                } else if ("tags_schema".equals(topLevelFieldName) || "tagsSchema".equals(topLevelFieldName)) {
                    String schema = parser.text();
                    if ("styled".equals(schema)) {
                        globalPreTags = STYLED_PRE_TAG;
                        globalPostTags = STYLED_POST_TAGS;
                    }
                } else if ("highlight_filter".equals(topLevelFieldName) || "highlightFilter".equals(topLevelFieldName)) {
                    globalHighlightFilter = parser.booleanValue();
                } else if ("fragment_size".equals(topLevelFieldName) || "fragmentSize".equals(topLevelFieldName)) {
                    globalFragmentSize = parser.intValue();
                } else if ("number_of_fragments".equals(topLevelFieldName) || "numberOfFragments".equals(topLevelFieldName)) {
                    globalNumOfFragments = parser.intValue();
                } else if ("encoder".equals(topLevelFieldName)) {
                    globalEncoder = parser.text();
                } else if ("require_field_match".equals(topLevelFieldName) || "requireFieldMatch".equals(topLevelFieldName)) {
                    globalRequireFieldMatch = parser.booleanValue();
                } else if ("boundary_max_scan".equals(topLevelFieldName) || "boundaryMaxScan".equals(topLevelFieldName)) {
                    globalBoundaryMaxScan = parser.intValue();
                } else if ("boundary_chars".equals(topLevelFieldName) || "boundaryChars".equals(topLevelFieldName)) {
                    char[] charsArr = parser.text().toCharArray();
                    globalBoundaryChars = new Character[charsArr.length];
                    for (int i = 0; i < charsArr.length; i++) {
                        globalBoundaryChars[i] = charsArr[i];
                    }
                } else if ("type".equals(topLevelFieldName)) {
                    globalHighlighterType = parser.text();
                } else if ("fragmenter".equals(topLevelFieldName)) {
                    globalFragmenter = parser.text();
                } else if ("no_match_size".equals(topLevelFieldName) || "noMatchSize".equals(topLevelFieldName)) {
                    globalNoMatchSize = parser.intValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT && "options".equals(topLevelFieldName)) {
                globalOptions = parser.map();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("fields".equals(topLevelFieldName)) {
                    String highlightFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            highlightFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            SearchContextHighlight.Field field = new SearchContextHighlight.Field(highlightFieldName);
                            String fieldName = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    fieldName = parser.currentName();
                                } else if (token == XContentParser.Token.START_ARRAY) {
                                    if ("pre_tags".equals(fieldName) || "preTags".equals(fieldName)) {
                                        List<String> preTagsList = Lists.newArrayList();
                                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            preTagsList.add(parser.text());
                                        }
                                        field.preTags(preTagsList.toArray(new String[preTagsList.size()]));
                                    } else if ("post_tags".equals(fieldName) || "postTags".equals(fieldName)) {
                                        List<String> postTagsList = Lists.newArrayList();
                                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            postTagsList.add(parser.text());
                                        }
                                        field.postTags(postTagsList.toArray(new String[postTagsList.size()]));
                                    } else if ("matched_fields".equals(fieldName) || "matchedFields".equals(fieldName)) {
                                        Set<String> matchedFields = Sets.newHashSet();
                                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            matchedFields.add(parser.text());
                                        }
                                        field.matchedFields(matchedFields);
                                    }
                                } else if (token.isValue()) {
                                    if ("fragment_size".equals(fieldName) || "fragmentSize".equals(fieldName)) {
                                        field.fragmentCharSize(parser.intValue());
                                    } else if ("number_of_fragments".equals(fieldName) || "numberOfFragments".equals(fieldName)) {
                                        field.numberOfFragments(parser.intValue());
                                    } else if ("fragment_offset".equals(fieldName) || "fragmentOffset".equals(fieldName)) {
                                        field.fragmentOffset(parser.intValue());
                                    } else if ("highlight_filter".equals(fieldName) || "highlightFilter".equals(fieldName)) {
                                        field.highlightFilter(parser.booleanValue());
                                    } else if ("order".equals(fieldName)) {
                                        field.scoreOrdered("score".equals(parser.text()));
                                    } else if ("require_field_match".equals(fieldName) || "requireFieldMatch".equals(fieldName)) {
                                        field.requireFieldMatch(parser.booleanValue());
                                    } else if ("boundary_max_scan".equals(topLevelFieldName) || "boundaryMaxScan".equals(topLevelFieldName)) {
                                        field.boundaryMaxScan(parser.intValue());
                                    } else if ("boundary_chars".equals(topLevelFieldName) || "boundaryChars".equals(topLevelFieldName)) {
                                        char[] charsArr = parser.text().toCharArray();
                                        Character[] boundaryChars = new Character[charsArr.length];
                                        for (int i = 0; i < charsArr.length; i++) {
                                            boundaryChars[i] = charsArr[i];
                                        }
                                        field.boundaryChars(boundaryChars);
                                    } else if ("type".equals(fieldName)) {
                                        field.highlighterType(parser.text());
                                    } else if ("fragmenter".equals(fieldName)) {
                                        field.fragmenter(parser.text());
                                    } else if ("no_match_size".equals(fieldName) || "noMatchSize".equals(fieldName)) {
                                        field.noMatchSize(parser.intValue());
                                    }
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    if ("highlight_query".equals(fieldName) || "highlightQuery".equals(fieldName)) {
                                        field.highlightQuery(context.queryParserService().parse(parser).query());
                                    } else if (fieldName.equals("options")) {
                                        field.options(parser.map());
                                    }
                                }
                            }
                            fields.add(field);
                        }
                    }
                } else if ("highlight_query".equals(topLevelFieldName) || "highlightQuery".equals(topLevelFieldName)) {
                    globalHighlightQuery = context.queryParserService().parse(parser).query();
                }
            }
        }
        if (globalPreTags != null && globalPostTags == null) {
            throw new SearchParseException(context, "Highlighter global preTags are set, but global postTags are not set");
        }

        // now, go over and fill all fields with default values from the global state
        for (SearchContextHighlight.Field field : fields) {
            if (field.preTags() == null) {
                field.preTags(globalPreTags);
            }
            if (field.postTags() == null) {
                field.postTags(globalPostTags);
            }
            if (field.highlightFilter() == null) {
                field.highlightFilter(globalHighlightFilter);
            }
            if (field.scoreOrdered() == null) {
                field.scoreOrdered(globalScoreOrdered);
            }
            if (field.fragmentCharSize() == -1) {
                field.fragmentCharSize(globalFragmentSize);
            }
            if (field.numberOfFragments() == -1) {
                field.numberOfFragments(globalNumOfFragments);
            }
            if (field.encoder() == null) {
                field.encoder(globalEncoder);
            }
            if (field.requireFieldMatch() == null) {
                field.requireFieldMatch(globalRequireFieldMatch);
            }
            if (field.boundaryMaxScan() == -1) {
                field.boundaryMaxScan(globalBoundaryMaxScan);
            }
            if (field.boundaryChars() == null) {
                field.boundaryChars(globalBoundaryChars);
            }
            if (field.highlighterType() == null) {
                field.highlighterType(globalHighlighterType);
            }
            if (field.fragmenter() == null) {
                field.fragmenter(globalFragmenter);
            }
            if (field.options() == null || field.options().size() == 0) {
                field.options(globalOptions);
            }
            if (field.highlightQuery() == null && globalHighlightQuery != null) {
                field.highlightQuery(globalHighlightQuery);
            }
            if (field.noMatchSize() == -1) {
                field.noMatchSize(globalNoMatchSize);
            }
        }

        context.highlight(new SearchContextHighlight(fields));
    }
}
