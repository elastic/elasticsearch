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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * In the simplest case, parse template string and variables from the request,
 * compile the template and execute the template against the given variables.
 * */
public class TemplateQueryParser implements QueryParser<TemplateQueryBuilder> {

    private final static Map<String, ScriptService.ScriptType> parametersToTypes = new HashMap<>();
    static {
        parametersToTypes.put("query", ScriptService.ScriptType.INLINE);
        parametersToTypes.put("file", ScriptService.ScriptType.FILE);
        parametersToTypes.put("id", ScriptService.ScriptType.INDEXED);
    }

    @Override
    public String[] names() {
        return new String[] {TemplateQueryBuilder.NAME};
    }

    /**
     * Parses the template query replacing template parameters with provided
     * values. Handles both submitting the template as part of the request as
     * well as referencing only the template name.
     *
     * @param parseContext parse context containing the templated query.
     */
    @Override
    @Nullable
    public TemplateQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        Template template = parse(parser, parseContext.parseFieldMatcher());
        return new TemplateQueryBuilder(template);
    }

    public static Template parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher, String... parameters) throws IOException {
        Map<String, ScriptService.ScriptType> parameterMap = new HashMap<>(parametersToTypes);
        for (String parameter : parameters) {
            parameterMap.put(parameter, ScriptService.ScriptType.INLINE);
        }
        return parse(parser, parameterMap, parseFieldMatcher);
    }

    public static Template parse(String defaultLang, XContentParser parser, ParseFieldMatcher parseFieldMatcher, String... parameters) throws IOException {

        Map<String, ScriptService.ScriptType> parameterMap = new HashMap<>(parametersToTypes);
        for (String parameter : parameters) {
            parameterMap.put(parameter, ScriptService.ScriptType.INLINE);
        }
        return Template.parse(parser, parameterMap, defaultLang, parseFieldMatcher);
    }

    public static Template parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return parse(parser, parametersToTypes, parseFieldMatcher);
    }

    public static Template parse(XContentParser parser, Map<String, ScriptService.ScriptType> parameterMap, ParseFieldMatcher parseFieldMatcher) throws IOException {
        return Template.parse(parser, parameterMap, parseFieldMatcher);
    }

    @Override
    public TemplateQueryBuilder getBuilderPrototype() {
        return TemplateQueryBuilder.PROTOTYPE;
    }


}
