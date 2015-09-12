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

package org.elasticsearch.search.sort;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.query.support.NestedInnerQueryParseSupport;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ScriptSortParser implements SortParser {

    private static final String STRING_SORT_TYPE = "string";
    private static final String NUMBER_SORT_TYPE = "number";

    @Override
    public String[] names() {
        return new String[]{"_script"};
    }

    @Override
    public SortField parse(XContentParser parser, SearchContext context) throws Exception {
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        Script script = null;
        String type = null;
        Map<String, Object> params = null;
        boolean reverse = false;
        MultiValueMode sortMode = null;
        NestedInnerQueryParseSupport nestedHelper = null;

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentName, ScriptField.SCRIPT)) {
                    script = Script.parse(parser, context.parseFieldMatcher());
                } else if ("params".equals(currentName)) {
                    params = parser.map();
                } else if ("nested_filter".equals(currentName) || "nestedFilter".equals(currentName)) {
                    if (nestedHelper == null) {
                        nestedHelper = new NestedInnerQueryParseSupport(parser, context);
                    }
                    nestedHelper.filter();
                }
            } else if (token.isValue()) {
                if ("reverse".equals(currentName)) {
                    reverse = parser.booleanValue();
                } else if ("order".equals(currentName)) {
                    reverse = "desc".equals(parser.text());
                } else if (scriptParameterParser.token(currentName, token, parser, context.parseFieldMatcher())) {
                    // Do Nothing (handled by ScriptParameterParser
                } else if ("type".equals(currentName)) {
                    type = parser.text();
                } else if ("mode".equals(currentName)) {
                    sortMode = MultiValueMode.fromString(parser.text());
                } else if ("nested_path".equals(currentName) || "nestedPath".equals(currentName)) {
                    if (nestedHelper == null) {
                        nestedHelper = new NestedInnerQueryParseSupport(parser, context);
                    }
                    nestedHelper.setPath(parser.text());
                }
            }
        }

        if (script == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
            if (scriptValue != null) {
                if (params == null) {
                    params = new HashMap<>();
                }
                script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), params);
            }
        } else if (params != null) {
            throw new SearchParseException(context, "script params must be specified inside script object", parser.getTokenLocation());
        }

        if (script == null) {
            throw new SearchParseException(context, "_script sorting requires setting the script to sort by", parser.getTokenLocation());
        }
        if (type == null) {
            throw new SearchParseException(context, "_script sorting requires setting the type of the script", parser.getTokenLocation());
        }
        final SearchScript searchScript = context.scriptService().search(context.lookup(), script, ScriptContext.Standard.SEARCH);

        if (STRING_SORT_TYPE.equals(type) && (sortMode == MultiValueMode.SUM || sortMode == MultiValueMode.AVG)) {
            throw new SearchParseException(context, "type [string] doesn't support mode [" + sortMode + "]", parser.getTokenLocation());
        }

        if (sortMode == null) {
            sortMode = reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
        }

        // If nested_path is specified, then wrap the `fieldComparatorSource` in a `NestedFieldComparatorSource`
        final Nested nested;
        if (nestedHelper != null && nestedHelper.getPath() != null) {
            BitSetProducer rootDocumentsFilter = context.bitsetFilterCache().getBitSetProducer(Queries.newNonNestedFilter());
            Filter innerDocumentsFilter;
            if (nestedHelper.filterFound()) {
                // TODO: use queries instead
                innerDocumentsFilter = new QueryWrapperFilter(nestedHelper.getInnerFilter());
            } else {
                innerDocumentsFilter = nestedHelper.getNestedObjectMapper().nestedTypeFilter();
            }
            nested = new Nested(rootDocumentsFilter, innerDocumentsFilter);
        } else {
            nested = null;
        }

        final IndexFieldData.XFieldComparatorSource fieldComparatorSource;
        switch (type) {
            case STRING_SORT_TYPE:
                fieldComparatorSource = new BytesRefFieldComparatorSource(null, null, sortMode, nested) {
                    LeafSearchScript leafScript;
                    @Override
                    protected SortedBinaryDocValues getValues(LeafReaderContext context) throws IOException {
                        leafScript = searchScript.getLeafSearchScript(context);
                        final BinaryDocValues values = new BinaryDocValues() {
                            final BytesRefBuilder spare = new BytesRefBuilder();
                            @Override
                            public BytesRef get(int docID) {
                                leafScript.setDocument(docID);
                                spare.copyChars(leafScript.run().toString());
                                return spare.get();
                            }
                        };
                        return FieldData.singleton(values, null);
                    }
                    @Override
                    protected void setScorer(Scorer scorer) {
                        leafScript.setScorer(scorer);
                    }
                };
                break;
            case NUMBER_SORT_TYPE:
                // TODO: should we rather sort missing values last?
                fieldComparatorSource = new DoubleValuesComparatorSource(null, Double.MAX_VALUE, sortMode, nested) {
                    LeafSearchScript leafScript;
                    @Override
                    protected SortedNumericDoubleValues getValues(LeafReaderContext context) throws IOException {
                        leafScript = searchScript.getLeafSearchScript(context);
                        final NumericDoubleValues values = new NumericDoubleValues() {
                            @Override
                            public double get(int docID) {
                                leafScript.setDocument(docID);
                                return leafScript.runAsDouble();
                            }
                        };
                        return FieldData.singleton(values, null);
                    }
                    @Override
                    protected void setScorer(Scorer scorer) {
                        leafScript.setScorer(scorer);
                    }
                };
                break;
            default:
            throw new SearchParseException(context, "custom script sort type [" + type + "] not supported", parser.getTokenLocation());
        }

        return new SortField("_script", fieldComparatorSource, reverse);
    }
}
