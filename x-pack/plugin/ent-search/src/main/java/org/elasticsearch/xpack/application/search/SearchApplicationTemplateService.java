/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class SearchApplicationTemplateService {

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    public SearchApplicationTemplateService(
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    public SearchSourceBuilder renderQuery(SearchApplication searchApplication, Map<String, Object> templateParams) throws IOException,
        ValidationException, XContentParseException {
        final SearchApplicationTemplate template = searchApplication.searchApplicationTemplateOrDefault();
        template.validateTemplateParams(templateParams);
        final Map<String, Object> renderedTemplateParams = renderTemplateParams(template, templateParams);
        final Script script = template.script();
        TemplateScript compiledTemplate = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(renderedTemplateParams);
        String requestSource;
        try {
            requestSource = SearchTemplateHelper.stripTrailingComma(compiledTemplate.execute());
        } catch (JsonProcessingException e) {
            JsonLocation loc = e.getLocation();
            throw new XContentParseException(new XContentLocation(loc.getLineNr(), loc.getColumnNr()), e.getMessage(), e);
        }

        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, requestSource)) {
            SearchSourceBuilder builder = SearchSourceBuilder.searchSource();
            builder.parseXContent(parser, false, clusterSupportsFeature);
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
        final SearchApplicationTemplate template = searchApplication.searchApplicationTemplateOrDefault();
        return renderTemplateParams(template, queryParams);
    }

    private Map<String, Object> renderTemplateParams(SearchApplicationTemplate template, Map<String, Object> queryParams) {
        final Script script = template.script();
        Map<String, Object> mergedTemplateParams = new HashMap<>(script.getParams());
        mergedTemplateParams.putAll(queryParams);
        return mergedTemplateParams;
    }
}
