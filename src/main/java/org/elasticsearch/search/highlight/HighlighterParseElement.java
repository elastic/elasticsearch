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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.search.vectorhighlight.SimpleBoundaryScanner;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.highlight.SearchContextHighlight.Field;
import org.elasticsearch.search.highlight.SearchContextHighlight.FieldOptions.Builder;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
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
        List<ParsedField> parsedFields = newArrayList();
        List<SearchContextHighlight.Field> fields = newArrayList();

        SearchContextHighlight.FieldOptions.Builder globalOptionsBuilder = new SearchContextHighlight.FieldOptions.Builder()
                .preTags(DEFAULT_PRE_TAGS).postTags(DEFAULT_POST_TAGS).scoreOrdered(false).highlightFilter(false)
                .requireFieldMatch(false).forceSource(false).fragmentCharSize(100).numberOfFragments(5)
                .encoder("default").boundaryMaxScan(SimpleBoundaryScanner.DEFAULT_MAX_SCAN)
                .boundaryChars(SimpleBoundaryScanner.DEFAULT_BOUNDARY_CHARS)
                .noMatchSize(0).phraseLimit(256);

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("pre_tags".equals(topLevelFieldName) || "preTags".equals(topLevelFieldName)) {
                    List<String> preTagsList = Lists.newArrayList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        preTagsList.add(parser.text());
                    }
                    globalOptionsBuilder.preTags(preTagsList.toArray(new String[preTagsList.size()]));
                } else if ("post_tags".equals(topLevelFieldName) || "postTags".equals(topLevelFieldName)) {
                    List<String> postTagsList = Lists.newArrayList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        postTagsList.add(parser.text());
                    }
                    globalOptionsBuilder.postTags(postTagsList.toArray(new String[postTagsList.size()]));
                }
            } else if (token.isValue()) {
                if ("order".equals(topLevelFieldName)) {
                    globalOptionsBuilder.scoreOrdered("score".equals(parser.text()));
                } else if ("tags_schema".equals(topLevelFieldName) || "tagsSchema".equals(topLevelFieldName)) {
                    String schema = parser.text();
                    if ("styled".equals(schema)) {
                        globalOptionsBuilder.preTags(STYLED_PRE_TAG);
                        globalOptionsBuilder.postTags(STYLED_POST_TAGS);
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
                    parseFields(parser, context, parsedFields, fields);
                } else if ("highlight_query".equals(topLevelFieldName) || "highlightQuery".equals(topLevelFieldName)) {
                    globalOptionsBuilder.highlightQuery(context.queryParserService().parse(parser).query());
                }
            }
        }

        SearchContextHighlight.FieldOptions globalOptions = globalOptionsBuilder.build();
        if (globalOptions.preTags() != null && globalOptions.postTags() == null) {
            throw new SearchParseException(context, "Highlighter global preTags are set, but global postTags are not set");
        }

        // now, go over and fill all fieldsOptions with default values from the global state
        for (ParsedField parsedField: parsedFields) {
            parsedField.finish(globalOptions);
        }

        context.highlight(new SearchContextHighlight(fields));
    }

    private void parseFields(XContentParser parser, SearchContext context, List<ParsedField> parsedFields,
            List<SearchContextHighlight.Field> destination) throws IOException {
        String highlightFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                highlightFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                SearchContextHighlight.FieldOptions.Builder fieldOptionsBuilder = new SearchContextHighlight.FieldOptions.Builder();
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
                            fieldOptionsBuilder.preTags(preTagsList.toArray(new String[preTagsList.size()]));
                        } else if ("post_tags".equals(fieldName) || "postTags".equals(fieldName)) {
                            List<String> postTagsList = Lists.newArrayList();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                postTagsList.add(parser.text());
                            }
                            fieldOptionsBuilder.postTags(postTagsList.toArray(new String[postTagsList.size()]));
                        } else if ("matched_fields".equals(fieldName) || "matchedFields".equals(fieldName)) {
                            Set<String> matchedFields = Sets.newHashSet();
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
                            fieldOptionsBuilder.highlightQuery(context.queryParserService().parse(parser).query());
                        } else if ("options".equals(fieldName)) {
                            fieldOptionsBuilder.options(parser.map());
                        } else if ("conditional".equals(fieldName)) {
                            String conditionalFieldType = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    conditionalFieldType = parser.currentName();
                                } else if (token == XContentParser.Token.START_OBJECT) {
                                    if ("match".equals(conditionalFieldType)) {
                                        List<Field> matchConditional = newArrayList();
                                        fieldOptionsBuilder.matchConditionalFields(matchConditional);
                                        parseFields(parser, context, parsedFields, matchConditional);
                                    } else if ("no_match".equals(conditionalFieldType) || "noMatch".equals(conditionalFieldType)) {
                                        List<Field> noMatchConditional = newArrayList();
                                        fieldOptionsBuilder.noMatchConditionalFields(noMatchConditional);
                                        parseFields(parser, context, parsedFields, noMatchConditional);
                                    } else {
                                        throw new ElasticsearchIllegalArgumentException("[conditional highlight] does not support ["
                                                + fieldName + "]");
                                    }
                                }
                            }
                        }
                    }
                }
                parsedFields.add(new ParsedField(destination, highlightFieldName, fieldOptionsBuilder));
            }
        }
    }

    /**
     * Everything required to complete building a SearchContextHighlight.Field
     * and drop it in the right spot once the global options are done parsing.
     */
    private static class ParsedField {
        private final List<SearchContextHighlight.Field> destination;
        private final String name;
        private final SearchContextHighlight.FieldOptions.Builder local;

        public ParsedField(List<Field> destination, String name, Builder local) {
            this.destination = destination;
            this.name = name;
            this.local = local;
        }

        /**
         * Finish building the field by combining it with global and adding it to destination.
         */
        public void finish(SearchContextHighlight.FieldOptions global) {
            destination.add(new SearchContextHighlight.Field(name, local.merge(global).build()));
        }
    }
}
