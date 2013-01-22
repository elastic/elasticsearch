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

import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleScriptDataComparator;
import org.elasticsearch.index.fielddata.fieldcomparator.StringScriptDataComparator;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 *
 */
public class ScriptSortParser implements SortParser {

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

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
                if ("reverse".equals(currentName)) {
                    reverse = parser.booleanValue();
                } else if ("order".equals(currentName)) {
                    reverse = "desc".equals(parser.text());
                } else if ("script".equals(currentName)) {
                    script = parser.text();
                } else if ("type".equals(currentName)) {
                    type = parser.text();
                } else if ("lang".equals(currentName)) {
                    scriptLang = parser.text();
                }
            }
        }

        if (script == null) {
            throw new SearchParseException(context, "_script sorting requires setting the script to sort by");
        }
        if (type == null) {
            throw new SearchParseException(context, "_script sorting requires setting the type of the script");
        }
        SearchScript searchScript = context.scriptService().search(context.lookup(), scriptLang, script, params);
        FieldComparatorSource fieldComparatorSource;
        if ("string".equals(type)) {
            fieldComparatorSource = StringScriptDataComparator.comparatorSource(searchScript);
        } else if ("number".equals(type)) {
            fieldComparatorSource = DoubleScriptDataComparator.comparatorSource(searchScript);
        } else {
            throw new SearchParseException(context, "custom script sort type [" + type + "] not supported");
        }
        return new SortField("_script", fieldComparatorSource, reverse);
    }
}
