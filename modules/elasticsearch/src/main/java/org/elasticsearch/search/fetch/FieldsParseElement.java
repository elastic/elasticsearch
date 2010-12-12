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

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.search.SearchScript;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.internal.SearchContext;

/**
 * @author kimchy (shay.banon)
 */
public class FieldsParseElement implements SearchParseElement {

    @Override public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            boolean added = false;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                added = true;
                String name = parser.text();
                if (name.contains("_source.") || name.contains("doc[")) {
                    // script field to load from source
                    SearchScript searchScript = new SearchScript(context.lookup(), null, name, null, context.scriptService());
                    context.scriptFields().add(new ScriptFieldsContext.ScriptField(name, searchScript));
                } else {
                    FieldMapper fieldMapper = context.mapperService().smartNameFieldMapper(name);
                    if (!"*".equals(name) && (fieldMapper == null || !fieldMapper.stored())) {
                        // script field to load from source
                        SearchScript searchScript = new SearchScript(context.lookup(), null, "_source." + name, null, context.scriptService());
                        context.scriptFields().add(new ScriptFieldsContext.ScriptField(name, searchScript));
                    } else {
                        context.fieldNames().add(name);
                    }
                }
            }
            if (!added) {
                context.emptyFieldNames();
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            String name = parser.text();
            if (name.contains("_source.") || name.contains("doc[")) {
                // script field to load from source
                SearchScript searchScript = new SearchScript(context.lookup(), null, name, null, context.scriptService());
                context.scriptFields().add(new ScriptFieldsContext.ScriptField(name, searchScript));
            } else {
                FieldMapper fieldMapper = context.mapperService().smartNameFieldMapper(name);
                if (!"*".equals(name) && (fieldMapper == null || !fieldMapper.stored())) {
                    // script field to load from source
                    SearchScript searchScript = new SearchScript(context.lookup(), null, "_source." + name, null, context.scriptService());
                    context.scriptFields().add(new ScriptFieldsContext.ScriptField(name, searchScript));
                } else {
                    context.fieldNames().add(name);
                }
            }
        }
    }
}
