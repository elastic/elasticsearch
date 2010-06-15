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

package org.elasticsearch.search.fetch.script;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.field.function.script.ScriptFieldsFunction;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 * "script_fields" : {
 *  "test1" : {
 *      "script" : "doc['field_name'].value"
 *  },
 *  "test2" : {
 *      "script" : "..."
 *  }
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class ScriptFieldsParseElement implements SearchParseElement {

    @Override public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;
        String currentFieldName = null;
        List<ScriptFieldsContext.ScriptField> scriptFields = Lists.newArrayList();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String fieldName = currentFieldName;
                String script = null;
                Map<String, Object> params = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        params = parser.map();
                    } else if (token.isValue()) {
                        script = parser.text();
                    }
                }
                scriptFields.add(new ScriptFieldsContext.ScriptField(fieldName,
                        new ScriptFieldsFunction(script, context.scriptService(), context.mapperService(), context.fieldDataCache()),
                        params == null ? new HashMap<String, Object>() : params));
            }
        }
        context.scriptFields(new ScriptFieldsContext(scriptFields));
    }
}