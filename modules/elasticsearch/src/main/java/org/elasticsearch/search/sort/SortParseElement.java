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

package org.elasticsearch.search.sort;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class SortParseElement implements SearchParseElement {

    private static final SortField SORT_SCORE = new SortField(null, SortField.SCORE);
    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.SCORE, true);
    private static final SortField SORT_DOC = new SortField(null, SortField.DOC);
    private static final SortField SORT_DOC_REVERSE = new SortField(null, SortField.DOC, true);

    public static final String SCORE_FIELD_NAME = "_score";
    public static final String DOC_FIELD_NAME = "_doc";

    private final ImmutableMap<String, SortParser> parsers;

    public SortParseElement() {
        ImmutableMap.Builder<String, SortParser> builder = ImmutableMap.builder();
        addParser(builder, new ScriptSortParser());
        addParser(builder, new GeoDistanceSortParser());
        this.parsers = builder.build();
    }

    private void addParser(ImmutableMap.Builder<String, SortParser> parsers, SortParser parser) {
        for (String name : parser.names()) {
            parsers.put(name, parser);
        }
    }

    @Override public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        List<SortField> sortFields = Lists.newArrayListWithCapacity(2);
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    addCompoundSortField(parser, context, sortFields);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    addSortField(context, sortFields, parser.text(), false, null);
                }
            }
        } else {
            addCompoundSortField(parser, context, sortFields);
        }
        if (!sortFields.isEmpty()) {
            context.sort(new Sort(sortFields.toArray(new SortField[sortFields.size()])));
        }
    }

    private void addCompoundSortField(XContentParser parser, SearchContext context, List<SortField> sortFields) throws Exception {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                boolean reverse = false;
                String missing = null;
                String innerJsonName = null;
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_STRING) {
                    String direction = parser.text();
                    if (direction.equals("asc")) {
                        reverse = SCORE_FIELD_NAME.equals(fieldName);
                    } else if (direction.equals("desc")) {
                        reverse = !SCORE_FIELD_NAME.equals(fieldName);
                    }
                    addSortField(context, sortFields, fieldName, reverse, missing);
                } else {
                    if (parsers.containsKey(fieldName)) {
                        sortFields.add(parsers.get(fieldName).parse(parser, context));
                    } else {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                innerJsonName = parser.currentName();
                            } else if (token.isValue()) {
                                if ("reverse".equals(innerJsonName)) {
                                    reverse = parser.booleanValue();
                                } else if ("order".equals(innerJsonName)) {
                                    if ("asc".equals(parser.text())) {
                                        reverse = SCORE_FIELD_NAME.equals(fieldName);
                                    } else if ("desc".equals(parser.text())) {
                                        reverse = !SCORE_FIELD_NAME.equals(fieldName);
                                    }
                                } else if ("missing".equals(innerJsonName)) {
                                    missing = parser.textOrNull();
                                }
                            }
                        }
                        addSortField(context, sortFields, fieldName, reverse, missing);
                    }
                }
            }
        }
    }

    private void addSortField(SearchContext context, List<SortField> sortFields, String fieldName, boolean reverse, @Nullable final String missing) {
        if (SCORE_FIELD_NAME.equals(fieldName)) {
            if (reverse) {
                sortFields.add(SORT_SCORE_REVERSE);
            } else {
                sortFields.add(SORT_SCORE);
            }
        } else if (DOC_FIELD_NAME.equals(fieldName)) {
            if (reverse) {
                sortFields.add(SORT_DOC_REVERSE);
            } else {
                sortFields.add(SORT_DOC);
            }
        } else {
            FieldMapper fieldMapper = context.mapperService().smartNameFieldMapper(fieldName);
            if (fieldMapper == null) {
                throw new SearchParseException(context, "No mapping found for [" + fieldName + "]");
            }
            sortFields.add(new SortField(fieldName, fieldMapper.fieldDataType().newFieldComparatorSource(context.fieldDataCache(), missing), reverse));
        }
    }
}
