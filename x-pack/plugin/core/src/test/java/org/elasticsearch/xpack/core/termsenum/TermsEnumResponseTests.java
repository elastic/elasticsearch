/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.AbstractBroadcastResponseTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TermsEnumResponseTests extends AbstractBroadcastResponseTestCase<TermsEnumResponse> {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TermsEnumResponse, Void> PARSER = new ConstructingObjectParser<>(
        "term_enum_results",
        true,
        arg -> {
            BaseBroadcastResponse response = (BaseBroadcastResponse) arg[0];
            return new TermsEnumResponse(
                (List<String>) arg[1],
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getFailedShards(),
                Arrays.asList(response.getShardFailures()),
                (Boolean) arg[2]
            );
        }
    );

    static {
        AbstractBroadcastResponseTestCase.declareBroadcastFields(PARSER);
        PARSER.declareStringArray(optionalConstructorArg(), new ParseField(TermsEnumResponse.TERMS_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(TermsEnumResponse.COMPLETE_FIELD));
    }

    protected static List<String> getRandomTerms() {
        int termCount = randomIntBetween(0, 100);
        Set<String> uniqueTerms = Sets.newHashSetWithExpectedSize(termCount);
        while (uniqueTerms.size() < termCount) {
            String s = randomAlphaOfLengthBetween(1, 10);
            uniqueTerms.add(s);
        }
        List<String> terms = new ArrayList<>(uniqueTerms);
        return terms;
    }

    private static TermsEnumResponse createRandomTermEnumResponse() {
        int totalShards = randomIntBetween(1, 10);
        int successfulShards = randomIntBetween(0, totalShards);
        int failedShards = totalShards - successfulShards;
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>(failedShards);
        for (int i = 0; i < failedShards; i++) {
            ElasticsearchException exc = new ElasticsearchException("some_error_" + randomInt());
            String index = "index_" + randomInt(1000);
            int shard = randomInt(100);
            shardFailures.add(new DefaultShardOperationFailedException(index, shard, exc));
        }
        return new TermsEnumResponse(getRandomTerms(), totalShards, successfulShards, failedShards, shardFailures, randomBoolean());
    }

    @Override
    protected TermsEnumResponse doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected TermsEnumResponse createTestInstance() {
        return createRandomTermEnumResponse();
    }

    @Override
    protected void assertEqualInstances(TermsEnumResponse response, TermsEnumResponse parsedResponse) {
        super.assertEqualInstances(response, parsedResponse);
        assertEquals(response.getTerms().size(), parsedResponse.getTerms().size());
        assertTrue(response.getTerms().containsAll(parsedResponse.getTerms()));
    }

    @Override
    protected TermsEnumResponse createTestInstance(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> failures
    ) {
        return new TermsEnumResponse(getRandomTerms(), totalShards, successfulShards, failedShards, failures, randomBoolean());

    }

    @Override
    public void testToXContent() {
        String s = randomAlphaOfLengthBetween(1, 10);
        List<String> terms = new ArrayList<>();
        terms.add(s);
        TermsEnumResponse response = new TermsEnumResponse(terms, 10, 10, 0, new ArrayList<>(), true);

        String output = Strings.toString(response);
        assertEquals(Strings.format("""
            {"_shards":{"total":10,"successful":10,"failed":0},"terms":["%s"],"complete":true}""", s), output);
    }
}
