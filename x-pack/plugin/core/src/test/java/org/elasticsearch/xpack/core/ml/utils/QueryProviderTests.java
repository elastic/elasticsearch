/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;


public class QueryProviderTests extends AbstractSerializingTestCase<QueryProvider> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return writableRegistry();
    }

    @Override
    protected QueryProvider createTestInstance() {
        return createRandomValidQueryProvider();
    }

    @Override
    protected Writeable.Reader<QueryProvider> instanceReader() {
        return QueryProvider::fromStream;
    }

    @Override
    protected QueryProvider doParseInstance(XContentParser parser) throws IOException {
        return QueryProvider.fromXContent(parser, false, Messages.DATAFEED_CONFIG_QUERY_BAD_FORMAT);
    }

    public static QueryProvider createRandomValidQueryProvider() {
        return createRandomValidQueryProvider(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
    }

    public static QueryProvider createRandomValidQueryProvider(String field, String value) {
        Map<String, Object> terms = Collections.singletonMap(BoolQueryBuilder.NAME,
            Collections.singletonMap("filter",
                Collections.singletonList(
                    Collections.singletonMap(TermQueryBuilder.NAME,
                        Collections.singletonMap(field, value)))));
        return new QueryProvider(
            terms,
            QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(field, value)),
            null);
    }

    public void testEmptyQueryMap() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, "{}");
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> QueryProvider.fromXContent(parser, false, Messages.DATAFEED_CONFIG_QUERY_BAD_FORMAT));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Datafeed query is not parsable"));
    }

    @Override
    protected QueryProvider mutateInstance(QueryProvider instance) throws IOException {
        Exception parsingException = instance.getParsingException();
        QueryBuilder parsedQuery = instance.getParsedQuery();
        switch (between(0, 1)) {
            case 0:
                parsingException = parsingException == null ? new IOException("failed parsing") : null;
                break;
            case 1:
                parsedQuery = parsedQuery == null ?
                    XContentObjectTransformer.queryBuilderTransformer(xContentRegistry()).fromMap(instance.getQuery()) :
                    null;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new QueryProvider(instance.getQuery(), parsedQuery, parsingException);
    }
}
