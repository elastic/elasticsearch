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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.join.BitDocIdSetFilter;
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
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
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
        String script = null;
        String scriptLang = null;
        String type = null;
        Map<String, Object> params = null;
        boolean reverse = false;
        MultiValueMode sortMode = null;
        NestedInnerQueryParseSupport nestedHelper = null;

        XContentParser.Token token;
        String currentName = parser.currentName();
        ScriptService.ScriptType scriptType = ScriptService.ScriptType.INLINE;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentName)) {
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
                } else if (ScriptService.SCRIPT_INLINE.match(currentName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.INLINE;
                } else if (ScriptService.SCRIPT_ID.match(currentName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.INDEXED;
                } else if (ScriptService.SCRIPT_FILE.match(currentName)) {
                    script = parser.text();
                    scriptType = ScriptService.ScriptType.FILE;
                } else if (ScriptService.SCRIPT_LANG.match(currentName)) {
                    scriptLang = parser.text();
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

        if (script == null) {
            throw new SearchParseException(context, "_script sorting requires setting the script to sort by");
        }
        if (type == null) {
            throw new SearchParseException(context, "_script sorting requires setting the type of the script");
        }
        final SearchScript searchScript = context.scriptService().search(context.lookup(), new Script(scriptLang, script, scriptType, params), ScriptContext.Standard.SEARCH);

        if (STRING_SORT_TYPE.equals(type) && (sortMode == MultiValueMode.SUM || sortMode == MultiValueMode.AVG)) {
            throw new SearchParseException(context, "type [string] doesn't support mode [" + sortMode + "]");
        }

        if (sortMode == null) {
            sortMode = reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
        }

        // If nested_path is specified, then wrap the `fieldComparatorSource` in a `NestedFieldComparatorSource`
        final Nested nested;
        if (nestedHelper != null && nestedHelper.getPath() != null) {
            BitDocIdSetFilter rootDocumentsFilter = context.bitsetFilterCache().getBitDocIdSetFilter(Queries.newNonNestedFilter());
            Filter innerDocumentsFilter;
            if (nestedHelper.filterFound()) {
                innerDocumentsFilter = context.filterCache().cache(nestedHelper.getInnerFilter(), null, context.queryParserService().autoFilterCachePolicy());
            } else {
                innerDocumentsFilter = context.filterCache().cache(nestedHelper.getNestedObjectMapper().nestedTypeFilter(), null, context.queryParserService().autoFilterCachePolicy());
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
                throw new SearchParseException(context, "custom script sort type [" + type + "] not supported");
        }

        return new SortField("_script", fieldComparatorSource, reverse);
    }
}
