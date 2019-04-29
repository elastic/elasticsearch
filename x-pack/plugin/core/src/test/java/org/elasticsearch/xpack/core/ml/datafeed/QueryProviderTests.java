/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
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
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;


public class QueryProviderTests extends AbstractSerializingTestCase<QueryProvider> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
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
        return QueryProvider.fromXContent(parser, false);
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
            () -> QueryProvider.fromXContent(parser, false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Datafeed query is not parsable"));
    }

    public void testSerializationBetweenBugVersion() throws IOException {
        QueryProvider tempQueryProvider = createRandomValidQueryProvider();
        QueryProvider queryProviderWithEx = new QueryProvider(tempQueryProvider.getQuery(),
            tempQueryProvider.getParsedQuery(),
            new IOException("ex"));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_6_6_2);
            queryProviderWithEx.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry())) {
                in.setVersion(Version.V_6_6_2);
                QueryProvider streamedQueryProvider = QueryProvider.fromStream(in);
                assertThat(streamedQueryProvider.getQuery(), equalTo(queryProviderWithEx.getQuery()));
                assertThat(streamedQueryProvider.getParsingException(), is(nullValue()));

                QueryBuilder streamedParsedQuery = XContentObjectTransformer.queryBuilderTransformer(xContentRegistry())
                    .fromMap(streamedQueryProvider.getQuery());
                assertThat(streamedParsedQuery, equalTo(queryProviderWithEx.getParsedQuery()));
                assertThat(streamedQueryProvider.getParsedQuery(), is(nullValue()));
            }
        }
    }

    public void testSerializationBetweenEagerVersion() throws IOException {
        QueryProvider validQueryProvider = createRandomValidQueryProvider();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_6_0_0);
            validQueryProvider.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry())) {
                in.setVersion(Version.V_6_0_0);

                QueryProvider streamedQueryProvider = QueryProvider.fromStream(in);
                XContentObjectTransformer<QueryBuilder> transformer = XContentObjectTransformer.queryBuilderTransformer(xContentRegistry());
                Map<String, Object> sourceQueryMapWithDefaults = transformer.toMap(transformer.fromMap(validQueryProvider.getQuery()));

                assertThat(streamedQueryProvider.getQuery(), equalTo(sourceQueryMapWithDefaults));
                assertThat(streamedQueryProvider.getParsingException(), is(nullValue()));
                assertThat(streamedQueryProvider.getParsedQuery(), equalTo(validQueryProvider.getParsedQuery()));
            }
        }

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            QueryProvider queryProviderWithEx = new QueryProvider(validQueryProvider.getQuery(),
                validQueryProvider.getParsedQuery(),
                new IOException("bad parsing"));
            output.setVersion(Version.V_6_0_0);
            IOException ex = expectThrows(IOException.class, () -> queryProviderWithEx.writeTo(output));
            assertThat(ex.getMessage(), equalTo("bad parsing"));
        }

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            QueryProvider queryProviderWithEx = new QueryProvider(validQueryProvider.getQuery(),
                validQueryProvider.getParsedQuery(),
                new ElasticsearchException("bad parsing"));
            output.setVersion(Version.V_6_0_0);
            ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> queryProviderWithEx.writeTo(output));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause().getMessage(), equalTo("bad parsing"));
        }

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            QueryProvider queryProviderWithOutParsed = new QueryProvider(validQueryProvider.getQuery(), null, null);
            output.setVersion(Version.V_6_0_0);
            ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> queryProviderWithOutParsed.writeTo(output));
            assertThat(ex.getMessage(), equalTo("Unsupported operation: parsed query is null"));
        }
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
