/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SearchApplicationTemplateService {

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;

    private final Logger logger = LogManager.getLogger(SearchApplicationTemplateService.class);

    public SearchApplicationTemplateService(ScriptService scriptService, NamedXContentRegistry xContentRegistry) {
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
    }

    public SearchSourceBuilder renderQuery(SearchApplication searchApplication, Map<String, Object> templateParams) throws IOException,
        ValidationException {
        final SearchApplicationTemplate template = searchApplication.searchApplicationTemplate();
        template.validateTemplateParams(templateParams);
        final Map<String, Object> renderedTemplateParams = renderTemplate(searchApplication, templateParams);
        final Script script = template.script();

        TemplateScript compiledTemplate = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(renderedTemplateParams);
        final String requestSource = SearchTemplateHelper.stripTrailingComma(compiledTemplate.execute());
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, requestSource)) {
            SearchSourceBuilder builder = SearchSourceBuilder.searchSource();
            builder.parseXContent(parser, false);
            return builder;
        }
    }

    /**
     * Renders the search application's associated template with the provided request parameters.
     *
     * @param searchApplication The SearchApplication we're accessing, to access the associated template
     * @param queryParams Specified parameters, which is expected to contain a subset of the parameters included in the template.
     * @return Map of all template parameters including template defaults for non-specified parameters
     * @throws ValidationException on invalid template parameters
     */
    public Map<String, Object> renderTemplate(SearchApplication searchApplication, Map<String, Object> queryParams)
        throws ValidationException {

        final SearchApplicationTemplate template = searchApplication.searchApplicationTemplate();
        final Script script = template.script();

        Map<String, Object> mergedTemplateParams = new HashMap<>(script.getParams());
        mergedTemplateParams.putAll(queryParams);

        return mergedTemplateParams;
    }
}
