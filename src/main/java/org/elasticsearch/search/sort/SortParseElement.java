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

package org.elasticsearch.search.sort;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ObjectMappers;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.search.nested.NestedFieldComparatorSource;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;

/**
 *
 */
public class SortParseElement implements SearchParseElement {

    private static final SortField SORT_SCORE = new SortField(null, SortField.Type.SCORE);
    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField SORT_DOC = new SortField(null, SortField.Type.DOC);
    private static final SortField SORT_DOC_REVERSE = new SortField(null, SortField.Type.DOC, true);

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

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        List<SortField> sortFields = Lists.newArrayListWithCapacity(2);
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    addCompoundSortField(parser, context, sortFields);
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    addSortField(context, sortFields, parser.text(), false, false, null, null, null, null);
                } else {
                    throw new ElasticSearchIllegalArgumentException("malformed sort format, within the sort array, an object, or an actual string are allowed");
                }
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            addSortField(context, sortFields, parser.text(), false, false, null, null, null, null);
        } else if (token == XContentParser.Token.START_OBJECT) {
            addCompoundSortField(parser, context, sortFields);
        } else {
            throw new ElasticSearchIllegalArgumentException("malformed sort format, either start with array, object, or an actual string");
        }
        if (!sortFields.isEmpty()) {
            // optimize if we just sort on score non reversed, we don't really need sorting
            boolean sort;
            if (sortFields.size() > 1) {
                sort = true;
            } else {
                SortField sortField = sortFields.get(0);
                if (sortField.getType() == SortField.Type.SCORE && !sortField.getReverse()) {
                    sort = false;
                } else {
                    sort = true;
                }
            }
            if (sort) {
                context.sort(new Sort(sortFields.toArray(new SortField[sortFields.size()])));
            }
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
                boolean ignoreUnmapped = false;
                SortMode sortMode = null;
                Filter nestedFilter = null;
                String nestedPath = null;
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_STRING) {
                    String direction = parser.text();
                    if (direction.equals("asc")) {
                        reverse = SCORE_FIELD_NAME.equals(fieldName);
                    } else if (direction.equals("desc")) {
                        reverse = !SCORE_FIELD_NAME.equals(fieldName);
                    } else {
                        throw new ElasticSearchIllegalArgumentException("sort direction [" + fieldName + "] not supported");
                    }
                    addSortField(context, sortFields, fieldName, reverse, ignoreUnmapped, missing, sortMode, nestedPath, nestedFilter);
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
                                } else if ("ignore_unmapped".equals(innerJsonName) || "ignoreUnmapped".equals(innerJsonName)) {
                                    ignoreUnmapped = parser.booleanValue();
                                } else if ("mode".equals(innerJsonName)) {
                                    sortMode = SortMode.fromString(parser.text());
                                } else if ("nested_path".equals(innerJsonName) || "nestedPath".equals(innerJsonName)) {
                                    nestedPath = parser.text();
                                } else {
                                    throw new ElasticSearchIllegalArgumentException("sort option [" + innerJsonName + "] not supported");
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                if ("nested_filter".equals(innerJsonName) || "nestedFilter".equals(innerJsonName)) {
                                    ParsedFilter parsedFilter = context.queryParserService().parseInnerFilter(parser);
                                    nestedFilter = parsedFilter == null ? null : parsedFilter.filter();
                                } else {
                                    throw new ElasticSearchIllegalArgumentException("sort option [" + innerJsonName + "] not supported");
                                }
                            }
                        }
                        addSortField(context, sortFields, fieldName, reverse, ignoreUnmapped, missing, sortMode, nestedPath, nestedFilter);
                    }
                }
            }
        }
    }

    private void addSortField(SearchContext context, List<SortField> sortFields, String fieldName, boolean reverse, boolean ignoreUnmapped, @Nullable final String missing, SortMode sortMode, String nestedPath, Filter nestedFilter) {
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
            FieldMapper fieldMapper = context.smartNameFieldMapper(fieldName);
            if (fieldMapper == null) {
                if (ignoreUnmapped) {
                    return;
                }
                throw new SearchParseException(context, "No mapping found for [" + fieldName + "] in order to sort on");
            }

            if (!fieldMapper.isSortable()) {
                throw new SearchParseException(context, "Sorting not supported for field[" + fieldName + "]");
            }

            // Enable when we also know how to detect fields that do tokenize, but only emit one token
            /*if (fieldMapper instanceof StringFieldMapper) {
                StringFieldMapper stringFieldMapper = (StringFieldMapper) fieldMapper;
                if (stringFieldMapper.fieldType().tokenized()) {
                    // Fail early
                    throw new SearchParseException(context, "Can't sort on tokenized string field[" + fieldName + "]");
                }
            }*/

            // We only support AVG and SUM on number based fields
            if (!(fieldMapper instanceof NumberFieldMapper) && (sortMode == SortMode.SUM || sortMode == SortMode.AVG)) {
                sortMode = null;
            }
            if (sortMode == null) {
                sortMode = resolveDefaultSortMode(reverse);
            }

            IndexFieldData.XFieldComparatorSource fieldComparatorSource = context.fieldData().getForField(fieldMapper)
                    .comparatorSource(missing, sortMode);
            ObjectMapper objectMapper;
            if (nestedPath != null) {
                ObjectMappers objectMappers = context.mapperService().objectMapper(nestedPath);
                if (objectMappers == null) {
                    throw new ElasticSearchIllegalArgumentException("failed to find nested object mapping for explicit nested path [" + nestedPath + "]");
                }
                objectMapper = objectMappers.mapper();
                if (!objectMapper.nested().isNested()) {
                    throw new ElasticSearchIllegalArgumentException("mapping for explicit nested path is not mapped as nested: [" + nestedPath + "]");
                }
            } else {
                objectMapper = context.mapperService().resolveClosestNestedObjectMapper(fieldName);
            }
            if (objectMapper != null && objectMapper.nested().isNested()) {
                Filter rootDocumentsFilter = context.filterCache().cache(NonNestedDocsFilter.INSTANCE);
                Filter innerDocumentsFilter;
                if (nestedFilter != null) {
                    innerDocumentsFilter = context.filterCache().cache(nestedFilter);
                } else {
                    innerDocumentsFilter = context.filterCache().cache(objectMapper.nestedTypeFilter());
                }
                fieldComparatorSource = new NestedFieldComparatorSource(sortMode, fieldComparatorSource, rootDocumentsFilter, innerDocumentsFilter);
            }
            sortFields.add(new SortField(fieldMapper.names().indexName(), fieldComparatorSource, reverse));
        }
    }

    private static SortMode resolveDefaultSortMode(boolean reverse) {
        return reverse ? SortMode.MAX : SortMode.MIN;
    }

}
