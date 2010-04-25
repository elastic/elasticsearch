/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.util.Booleans;

import java.util.List;

import static com.google.common.collect.Lists.*;

/**
 * <pre>
 * highlight : {
 *  tags_schema : "styled",
 *  pre_Tags : ["tag1", "tag2"],
 *  post_tags : ["tag1", "tag2"],
 *  order : "score",
 *  fields : {
 *      field1 : {  }
 *      field2 : { fragment_size : 100, num_of_fragments : 2 }
 *  }
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class HighlighterParseElement implements SearchParseElement {

    private static final String[] DEFAULT_PRE_TAGS = new String[]{"<em>"};
    private static final String[] DEFAULT_POST_TAGS = new String[]{"</em>"};

    private static final String[] STYLED_PRE_TAG = {
            "<em class=\"hlt1\">", "<em class=\"hlt2\">", "<em class=\"hlt2\">",
            "<em class=\"hlt3\">", "<em class=\"hlt4\">", "<em class=\"hlt5\">",
            "<em class=\"hlt6\">", "<em class=\"hlt7\">", "<em class=\"hlt8\">",
            "<em class=\"hlt9\">"
    };
    public static final String[] STYLED_POST_TAGS = {"</em>"};


    @Override public void parse(JsonParser jp, SearchContext context) throws Exception {
        JsonToken token;
        String topLevelFieldName = null;
        List<SearchContextHighlight.ParsedHighlightField> fields = newArrayList();
        String[] preTags = DEFAULT_PRE_TAGS;
        String[] postTags = DEFAULT_POST_TAGS;
        boolean scoreOrdered = false;
        boolean highlightFilter = true;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                topLevelFieldName = jp.getCurrentName();
            } else if (token == JsonToken.START_ARRAY) {
                if ("pre_tags".equals(topLevelFieldName) || "preTags".equals(topLevelFieldName)) {
                    List<String> preTagsList = Lists.newArrayList();
                    while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
                        preTagsList.add(jp.getText());
                    }
                    preTags = preTagsList.toArray(new String[preTagsList.size()]);
                } else if ("post_tags".equals(topLevelFieldName) || "postTags".equals(topLevelFieldName)) {
                    List<String> postTagsList = Lists.newArrayList();
                    while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
                        postTagsList.add(jp.getText());
                    }
                    postTags = postTagsList.toArray(new String[postTagsList.size()]);
                }
            } else if (token == JsonToken.VALUE_STRING) {
                if ("order".equals(topLevelFieldName)) {
                    if ("score".equals(jp.getText())) {
                        scoreOrdered = true;
                    } else {
                        scoreOrdered = false;
                    }
                } else if ("tags_schema".equals(topLevelFieldName) || "tagsSchema".equals(topLevelFieldName)) {
                    String schema = jp.getText();
                    if ("styled".equals(schema)) {
                        preTags = STYLED_PRE_TAG;
                        postTags = STYLED_POST_TAGS;
                    }
                } else if ("highlight_filter".equals(topLevelFieldName) || "highlightFilter".equals(topLevelFieldName)) {
                    highlightFilter = Booleans.parseBoolean(jp.getText(), true);
                }
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                if ("highlight_filter".equals(topLevelFieldName) || "highlightFilter".equals(topLevelFieldName)) {
                    highlightFilter = jp.getIntValue() != 0;
                }
            } else if (token == JsonToken.VALUE_FALSE) {
                if ("highlight_filter".equals(topLevelFieldName) || "highlightFilter".equals(topLevelFieldName)) {
                    highlightFilter = false;
                }
            } else if (token == JsonToken.START_OBJECT) {
                if ("fields".equals(topLevelFieldName)) {
                    String highlightFieldName = null;
                    while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
                        if (token == JsonToken.FIELD_NAME) {
                            highlightFieldName = jp.getCurrentName();
                        } else if (token == JsonToken.START_OBJECT) {
                            String fieldName = null;
                            int fragmentSize = 100;
                            int numOfFragments = 5;
                            while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
                                if (token == JsonToken.FIELD_NAME) {
                                    fieldName = jp.getCurrentName();
                                } else if (token == JsonToken.VALUE_STRING) {
                                    if ("fragment_size".equals(fieldName) || "fragmentSize".equals(fieldName)) {
                                        fragmentSize = Integer.parseInt(jp.getText());
                                    } else if ("number_of_fragments".equals(fieldName) || "numberOfFragments".equals(fieldName)) {
                                        numOfFragments = Integer.parseInt(jp.getText());
                                    }
                                } else if (token == JsonToken.VALUE_NUMBER_INT) {
                                    if ("fragment_size".equals(fieldName) || "fragmentSize".equals(fieldName)) {
                                        fragmentSize = jp.getIntValue();
                                    } else if ("number_of_fragments".equals(fieldName) || "numberOfFragments".equals(fieldName)) {
                                        numOfFragments = jp.getIntValue();
                                    }
                                }
                            }
                            fields.add(new SearchContextHighlight.ParsedHighlightField(highlightFieldName, fragmentSize, numOfFragments));
                        }
                    }
                }
            }
        }
        if (preTags != null && postTags == null) {
            throw new SearchParseException(context, "Highlighter preTags are set, but postTags are not set");
        }
        context.highlight(new SearchContextHighlight(fields, preTags, postTags, scoreOrdered, highlightFilter));
    }
}
