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

package org.elasticsearch.index.query;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TemplateParser {

    public static BytesReference parse (XContentParser parser, ScriptService scriptService, String templateKey, String paramsKey) throws IOException {

        String template = "";
        Map<String, Object> vars = new HashMap<String, Object>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (templateKey.equals(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT && ! parser.hasTextCharacters()) {
                    // when called with un-escaped json string
                    XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);
                    builder.copyCurrentStructure(parser);
                    template = builder.string();
                } else {
                    // when called with excaped json string or when called with filename
                    template = parser.text();
                }
            } else if (paramsKey.equals(currentFieldName)) {
                XContentParser.Token innerToken;
                String key = null;
                while ((innerToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    // parsing template parameter map
                    if (innerToken == XContentParser.Token.FIELD_NAME) {
                        key = parser.currentName();
                    } else {
                        if (key != null) {
                            vars.put(key, parser.text());
                        } else {
                            throw new IllegalStateException("Template parameter key must not be null.");
                        }
                        key = null;
                    }
                }
            }
        }

        ExecutableScript executable = scriptService.executable("mustache", template, vars);
        return (BytesReference) executable.run();
    }
}
