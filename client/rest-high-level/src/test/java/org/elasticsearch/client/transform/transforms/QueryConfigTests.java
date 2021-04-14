/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static java.util.Collections.emptyList;

public class QueryConfigTests extends AbstractXContentTestCase<QueryConfig> {

    public static QueryConfig randomQueryConfig() {
        QueryBuilder queryBuilder = randomBoolean() ? new MatchAllQueryBuilder() : new MatchNoneQueryBuilder();
        return new QueryConfig(queryBuilder);
    }

    @Override
    protected QueryConfig createTestInstance() {
        return randomQueryConfig();
    }

    @Override
    protected QueryConfig doParseInstance(XContentParser parser) throws IOException {
        return QueryConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }
}
