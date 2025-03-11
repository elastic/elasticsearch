/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.eql.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.xpack.ql.TestUtils.randomRuntimeMappings;

public class EqlSearchRequestTests extends AbstractBWCSerializationTestCase<EqlSearchRequest> {

    // TODO: possibly add mutations
    static String defaultTestFilter = """
        {
           "match" : {
               "foo": "bar"
           }}""";

    static String defaultTestIndex = "endgame-*";
    boolean ccsMinimizeRoundtrips;

    @Before
    public void setup() {}

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected EqlSearchRequest createTestInstance() {
        try {
            List<FieldAndFormat> randomFetchFields = new ArrayList<>();
            int fetchFieldsCount = randomIntBetween(0, 5);
            for (int j = 0; j < fetchFieldsCount; j++) {
                randomFetchFields.add(new FieldAndFormat(randomAlphaOfLength(10), randomAlphaOfLength(10)));
            }
            if (randomFetchFields.isEmpty()) {
                randomFetchFields = null;
            }
            QueryBuilder filter = parseFilter(defaultTestFilter);
            ccsMinimizeRoundtrips = randomBoolean();
            return new EqlSearchRequest().indices(defaultTestIndex)
                .filter(filter)
                .timestampField(randomAlphaOfLength(10))
                .eventCategoryField(randomAlphaOfLength(10))
                .fetchSize(randomIntBetween(1, 50))
                .size(randomInt(50))
                .query(randomAlphaOfLength(10))
                .ccsMinimizeRoundtrips(ccsMinimizeRoundtrips)
                .waitForCompletionTimeout(randomTimeValue())
                .keepAlive(randomTimeValue())
                .keepOnCompletion(randomBoolean())
                .allowPartialSearchResults(randomBoolean())
                .allowPartialSequenceResults(randomBoolean())
                .fetchFields(randomFetchFields)
                .runtimeMappings(randomRuntimeMappings())
                .resultPosition(randomFrom("tail", "head"))
                .maxSamplesPerKey(randomIntBetween(1, 1000));
        } catch (IOException ex) {
            assertNotNull("unexpected IOException " + ex.getCause().getMessage(), ex);
        }
        return null;
    }

    @Override
    protected EqlSearchRequest mutateInstance(EqlSearchRequest instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    protected QueryBuilder parseFilter(String filter) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, filter);
        return parseFilter(parser);
    }

    protected QueryBuilder parseFilter(XContentParser parser) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseTopLevelQuery(parser);
        assertNull(parser.nextToken());
        return parseInnerQueryBuilder;
    }

    @Override
    protected Writeable.Reader<EqlSearchRequest> instanceReader() {
        return EqlSearchRequest::new;
    }

    @Override
    protected EqlSearchRequest doParseInstance(XContentParser parser) {
        return EqlSearchRequest.fromXContent(parser).indices(defaultTestIndex).ccsMinimizeRoundtrips(ccsMinimizeRoundtrips);
    }

    @Override
    protected EqlSearchRequest mutateInstanceForVersion(EqlSearchRequest instance, TransportVersion version) {
        EqlSearchRequest mutatedInstance = new EqlSearchRequest();
        mutatedInstance.indices(instance.indices());
        mutatedInstance.indicesOptions(instance.indicesOptions());
        mutatedInstance.filter(instance.filter());
        mutatedInstance.timestampField(instance.timestampField());
        mutatedInstance.tiebreakerField(instance.tiebreakerField());
        mutatedInstance.eventCategoryField(instance.eventCategoryField());
        mutatedInstance.size(instance.size());
        mutatedInstance.fetchSize(instance.fetchSize());
        mutatedInstance.query(instance.query());
        mutatedInstance.ccsMinimizeRoundtrips(instance.ccsMinimizeRoundtrips());
        mutatedInstance.waitForCompletionTimeout(instance.waitForCompletionTimeout());
        mutatedInstance.keepAlive(instance.keepAlive());
        mutatedInstance.keepOnCompletion(instance.keepOnCompletion());
        mutatedInstance.fetchFields(instance.fetchFields());
        mutatedInstance.runtimeMappings(instance.runtimeMappings());
        mutatedInstance.resultPosition(instance.resultPosition());
        mutatedInstance.maxSamplesPerKey(version.onOrAfter(TransportVersions.V_8_7_0) ? instance.maxSamplesPerKey() : 1);
        mutatedInstance.allowPartialSearchResults(
            version.onOrAfter(TransportVersions.EQL_ALLOW_PARTIAL_SEARCH_RESULTS) ? instance.allowPartialSearchResults() : false
        );
        mutatedInstance.allowPartialSequenceResults(
            version.onOrAfter(TransportVersions.EQL_ALLOW_PARTIAL_SEARCH_RESULTS) ? instance.allowPartialSequenceResults() : false
        );

        return mutatedInstance;
    }
}
