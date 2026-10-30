/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack.qa;

import org.elasticsearch.xpack.stack.QueryLoggingTemplateRegistry;

import java.util.Map;

public class QueryLoggingTemplateMappingsBwcTests extends ComponentTemplateMappingsBwcTestCase {
    @Override
    protected String mappingsResourcePath() {
        return QueryLoggingTemplateRegistry.QUERY_LOGGING_MAPPINGS_RESOURCE;
    }

    @Override
    protected Map<String, String> templateVariables() {
        return Map.of(
            QueryLoggingTemplateRegistry.QUERY_LOGGING_TEMPLATE_VERSION_VARIABLE,
            Integer.toString(QueryLoggingTemplateRegistry.INDEX_TEMPLATE_VERSION)
        );
    }
}
