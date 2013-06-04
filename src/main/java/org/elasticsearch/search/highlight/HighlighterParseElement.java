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
import org.elasticsearch.common.lucene.search.vectorhighlight.SimpleBoundaryScanner2;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
        int globalBoundaryMaxScan = SimpleBoundaryScanner2.DEFAULT_MAX_SCAN;
        char[] globalBoundaryChars = SimpleBoundaryScanner2.DEFAULT_BOUNDARY_CHARS;
        String globalHighlighterType = null;
        String globalFragmenter = null;
        Map<String, Object> globalOptions = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("pre_tags".equals(topLevelFieldName) || "preTags".equals(topLevelFieldName)) {
                  globalPreTags = parseStartArrayTags(parser);
                } else if ("post_tags".equals(topLevelFieldName) || "postTags".equals(topLevelFieldName)) {
                  globalPostTags = parseStartArrayTags(parser);
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
                    globalBoundaryChars = parser.text().toCharArray();
                } else if ("type".equals(topLevelFieldName)) {
                    globalHighlighterType = parser.text();
                } else if ("fragmenter".equals(topLevelFieldName)) {
                    globalFragmenter = parser.text();
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
                          parseStartObject(parser, topLevelFieldName, fields, highlightFieldName);
                        }
                    }
                }
            }
        }
        if (globalPreTags != null && globalPostTags == null) {
            throw new SearchParseException(context, "Highlighter global preTags are set, but global postTags are not set");
        }
      fillFieldsWithDefaultGlobalValues(fields, globalPreTags, globalPostTags, globalScoreOrdered, globalHighlightFilter, globalRequireFieldMatch, globalFragmentSize, globalNumOfFragments, globalEncoder, globalBoundaryMaxScan, globalBoundaryChars, globalHighlighterType, globalFragmenter, globalOptions);


      context.highlight(new SearchContextHighlight(fields));
    }

  private void parseStartObject(XContentParser parser, String topLevelFieldName, List<SearchContextHighlight.Field> fields, String highlightFieldName) throws IOException {
    XContentParser.Token token;
    SearchContextHighlight.Field field = new SearchContextHighlight.Field(highlightFieldName);
    String fieldName = null;
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      fieldName = parseStartObjectTokens(parser, token, topLevelFieldName, field, fieldName);
    }
    fields.add(field);
  }

  private String parseStartObjectTokens(XContentParser parser, XContentParser.Token token, String topLevelFieldName, SearchContextHighlight.Field field, String fieldName) throws IOException {
    if (token == XContentParser.Token.FIELD_NAME) {
        fieldName = parser.currentName();
    } else if (token == XContentParser.Token.START_ARRAY) {
        if ("pre_tags".equals(fieldName) || "preTags".equals(fieldName)) {
          parseStartArrayPreTags(parser, field);
        } else if ("post_tags".equals(fieldName) || "postTags".equals(fieldName)) {
          parseStartArrayPostTags(parser, field);
        }
    } else if (token.isValue()) {
        parseTokenValue(parser, topLevelFieldName, field, fieldName);
    } else if (fieldName.equals("options")) {
        field.options(parser.map());
    }
    return fieldName;
  }

  private void parseStartArrayPostTags(XContentParser parser, SearchContextHighlight.Field field) throws IOException {
    XContentParser.Token token;List<String> postTagsList = Lists.newArrayList();
    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
        postTagsList.add(parser.text());
    }
    field.postTags(postTagsList.toArray(new String[postTagsList.size()]));
  }

  private void parseStartArrayPreTags(XContentParser parser, SearchContextHighlight.Field field) throws IOException {
    XContentParser.Token token;List<String> preTagsList = Lists.newArrayList();
    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
        preTagsList.add(parser.text());
    }
    field.preTags(preTagsList.toArray(new String[preTagsList.size()]));
  }

  private void parseTokenValue(XContentParser parser, String topLevelFieldName, SearchContextHighlight.Field field, String fieldName) throws IOException {
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
        field.boundaryChars(parser.text().toCharArray());
    } else if ("type".equals(fieldName)) {
        field.highlighterType(parser.text());
    } else if ("fragmenter".equals(fieldName)) {
        field.fragmenter(parser.text());
    }
  }

  private void fillFieldsWithDefaultGlobalValues(List<SearchContextHighlight.Field> fields, String[] globalPreTags, String[] globalPostTags, boolean globalScoreOrdered, boolean globalHighlightFilter, boolean globalRequireFieldMatch, int globalFragmentSize, int globalNumOfFragments, String globalEncoder, int globalBoundaryMaxScan, char[] globalBoundaryChars, String globalHighlighterType, String globalFragmenter, Map<String, Object> globalOptions) {
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
    }
  }

  private String[] parseStartArrayTags(XContentParser parser) throws IOException {
    XContentParser.Token token;
    String[] globalPreTags;List<String> preTagsList = Lists.newArrayList();
    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
        preTagsList.add(parser.text());
    }
    globalPreTags = preTagsList.toArray(new String[preTagsList.size()]);
    return globalPreTags;
  }
}
