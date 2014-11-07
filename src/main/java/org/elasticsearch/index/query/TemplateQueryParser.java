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

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * In the simplest case, parse template string and variables from the request,
 * compile the template and execute the template against the given variables.
 * */
public class TemplateQueryParser implements QueryParser {

    /** Name to reference this type of query. */
    public static final String NAME = "template";
    /** Name of query parameter containing the template string. */
    public static final String QUERY = "query";
    /** Name of query parameter containing the template parameters. */
    public static final String PARAMS = "params";

    private final ScriptService scriptService;

    private final static Map<String, ScriptService.ScriptType> parametersToTypes = new HashMap<>();
    static {
        parametersToTypes.put("query", ScriptService.ScriptType.INLINE);
        parametersToTypes.put("file", ScriptService.ScriptType.FILE);
        parametersToTypes.put("id", ScriptService.ScriptType.INDEXED);
    }

    @Inject
    public TemplateQueryParser(ScriptService scriptService) {
        this.scriptService = scriptService;
    }

    @Override
    public String[] names() {
        return new String[] { NAME };
    }

    /**
     * Parses the template query replacing template parameters with provided
     * values. Handles both submitting the template as part of the request as
     * well as referencing only the template name.
     * 
     * @param parseContext
     *            parse context containing the templated query.
     */
    @Override
    @Nullable
    public Query parse(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        TemplateContext templateContext = parse(parser, PARAMS, parametersToTypes);
        ExecutableScript executable = this.scriptService.executable("mustache", templateContext.template(), templateContext.scriptType(),
                templateContext.params());

        BytesReference querySource = (BytesReference) executable.run();

        try (XContentParser qSourceParser = XContentFactory.xContent(querySource).createParser(querySource)) {
            final QueryParseContext context = new QueryParseContext(parseContext.index(), parseContext.indexQueryParserService());
            context.reset(qSourceParser);
            Query result = context.parseInnerQuery();
            return result;
        }
    }

    public static TemplateContext parse(XContentParser parser, String paramsFieldname, String... parameters) throws IOException {

        Map<String, ScriptService.ScriptType> parameterMap = new HashMap<>(parametersToTypes);
        for (String parameter : parameters) {
            parameterMap.put(parameter, ScriptService.ScriptType.INLINE);
        }
        return parse(parser, paramsFieldname, parameterMap);
    }

    public static TemplateContext parse(XContentParser parser, String paramsFieldname) throws IOException {
        return parse(parser, paramsFieldname, parametersToTypes);
    }

    public static TemplateContext parse(XContentParser parser, String paramsFieldname, Map<String, ScriptService.ScriptType> parameterMap)
            throws IOException {
        Map<String, Object> params = null;
        String templateNameOrTemplateContent = null;

        String currentFieldName = null;
        XContentParser.Token token;
        ScriptService.ScriptType type = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parameterMap.containsKey(currentFieldName)) {
                type = parameterMap.get(currentFieldName);
                if (token == XContentParser.Token.START_OBJECT) {
                    XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent());
                    builder.copyCurrentStructure(parser);
                    templateNameOrTemplateContent = builder.string();
                } else {
                    templateNameOrTemplateContent = parser.text();
                }
            } else if (paramsFieldname.equals(currentFieldName)) {
                params = parser.map();
            }
        }

        return new TemplateContext(type, templateNameOrTemplateContent, params);
    }

    public static class TemplateContext {
        private Map<String, Object> params;
        private String template;
        private ScriptService.ScriptType type;

        public TemplateContext(ScriptService.ScriptType type, String template, Map<String, Object> params) {
            this.params = params;
            this.template = template;
            this.type = type;
        }

        public Map<String, Object> params() {
            return params;
        }

        public String template() {
            return template;
        }

        public ScriptService.ScriptType scriptType() {
            return type;
        }

        @Override
        public String toString() {
            return type + " " + template;
        }

        /*
         * Search Template - conditional clauses not rendering correctly #8308
         */
        public void reduceConditionalClauses() throws IOException {
            if (params != null && params.size() > 0) {
                StringBuffer templateSb = new StringBuffer(template);
                reduceConditionalClauses(templateSb, params);
                template = templateSb.toString();
            }
        }

        @SuppressWarnings("unchecked")
        private void reduceConditionalClauses(StringBuffer templateSb, Map<String, Object> params) throws IOException {
            Iterator<?> it = params.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> pairs = (Map.Entry<String, Object>) it.next();
                String openTag = "{{#" + pairs.getKey() + "}}";
                String closeTag = "{{/" + pairs.getKey() + "}}";
                // System.out.println(openClause + ", " + closeClause);
                Object v = pairs.getValue();
                if (v instanceof Boolean && !((Boolean) v)) {
                    removeTwoTagsFromStringContents(templateSb, openTag, closeTag, true);
                } else {
                    removeTwoTagsFromStringContents(templateSb, openTag, closeTag, false);
                    if (v instanceof Map<?, ?>) {
                        reduceConditionalClauses(templateSb, (Map<String, Object>) v);      
                    }
                }

            }
        }

        private static void removeTwoTagsFromStringContents(StringBuffer sb, String openTag, String closeTag,
                boolean shouldRemoveContentsInBewteen) throws IOException {
            while (true) {
                int posOpen = sb.indexOf(openTag);
                if (posOpen > -1) {
                    if (!shouldRemoveContentsInBewteen)
                        sb = sb.replace(posOpen, posOpen + openTag.length(), "");
                } else {
                    break;
                }
                int posClose = sb.indexOf(closeTag);
                if (posClose > -1) {
                    if (posOpen < 0) {
                        throw new IOException("Search template is missing the opening tag: " + openTag
                                + ", which was calculated from the template params.");
                    } else if (posOpen > posClose) {
                        throw new IOException("Search template is having opening tag: " + openTag + " placed after the closing tag: "
                                + closeTag + "!");
                    }
                    if (!shouldRemoveContentsInBewteen)
                        sb = sb.replace(posClose, posClose + closeTag.length(), "");
                } else {
                    if (posOpen > -1)
                        throw new IOException("Search template is missing the closing tag: " + closeTag
                                + ", which was calculated from the template params.");
                }
                if (shouldRemoveContentsInBewteen) {
                    sb = sb.replace(posOpen, posClose+closeTag.length(), "");
                }
            }
        }
    }
}
