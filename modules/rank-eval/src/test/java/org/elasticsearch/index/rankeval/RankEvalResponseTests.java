/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.instanceOf;

public class RankEvalResponseTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RankEvalResponse, Void> PARSER = new ConstructingObjectParser<>(
        "rank_eval_response",
        true,
        a -> new RankEvalResponse(
            (Double) a[0],
            ((List<EvalQueryQuality>) a[1]).stream().collect(Collectors.toMap(EvalQueryQuality::getId, Function.identity())),
            ((List<Tuple<String, Exception>>) a[2]).stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2))
        )
    );
    static {
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), EvalQueryQuality.METRIC_SCORE_FIELD);
        PARSER.declareNamedObjects(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c, n) -> EvalQueryQualityTests.parseInstance(p, n),
            new ParseField("details")
        );
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(), (p, c, n) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.nextToken(), p);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.nextToken(), p);
            Tuple<String, ElasticsearchException> tuple = new Tuple<>(n, ElasticsearchException.failureFromXContent(p));
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, p.nextToken(), p);
            return tuple;
        }, new ParseField("failures"));
    }

    private static final Exception[] RANDOM_EXCEPTIONS = new Exception[] {
        new ClusterBlockException(singleton(NoMasterBlockService.NO_MASTER_BLOCK_WRITES)),
        new CircuitBreakingException("Data too large", 123, 456, CircuitBreaker.Durability.PERMANENT),
        new IllegalArgumentException("Closed resource", new RuntimeException("Resource")),
        new SearchPhaseExecutionException(
            "search",
            "all shards failed",
            new ShardSearchFailure[] {
                new ShardSearchFailure(
                    new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null)
                ) }
        ),
        new ElasticsearchException(
            "Parsing failed",
            new ParsingException(9, 42, "Wrong state", new NullPointerException("Unexpected null value"))
        ) };

    private static RankEvalResponse createRandomResponse() {
        int numberOfRequests = randomIntBetween(0, 5);
        Map<String, EvalQueryQuality> partials = Maps.newMapWithExpectedSize(numberOfRequests);
        for (int i = 0; i < numberOfRequests; i++) {
            String id = randomAlphaOfLengthBetween(3, 10);
            EvalQueryQuality evalQuality = new EvalQueryQuality(id, randomDoubleBetween(0.0, 1.0, true));
            int numberOfDocs = randomIntBetween(0, 5);
            List<RatedSearchHit> ratedHits = new ArrayList<>(numberOfDocs);
            for (int d = 0; d < numberOfDocs; d++) {
                ratedHits.add(searchHit(randomAlphaOfLength(10), randomIntBetween(0, 1000), randomIntBetween(0, 10)));
            }
            evalQuality.addHitsAndRatings(ratedHits);
            partials.put(id, evalQuality);
        }
        int numberOfErrors = randomIntBetween(0, 2);
        Map<String, Exception> errors = Maps.newMapWithExpectedSize(numberOfRequests);
        for (int i = 0; i < numberOfErrors; i++) {
            errors.put(randomAlphaOfLengthBetween(3, 10), randomFrom(RANDOM_EXCEPTIONS));
        }
        return new RankEvalResponse(randomDouble(), partials, errors);
    }

    public void testSerialization() throws IOException {
        RankEvalResponse randomResponse = createRandomResponse();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            randomResponse.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                RankEvalResponse deserializedResponse = new RankEvalResponse(in);
                assertEquals(randomResponse.getMetricScore(), deserializedResponse.getMetricScore(), 0.0000000001);
                assertEquals(randomResponse.getPartialResults(), deserializedResponse.getPartialResults());
                assertEquals(randomResponse.getFailures().keySet(), deserializedResponse.getFailures().keySet());
                assertNotSame(randomResponse, deserializedResponse);
                assertEquals(-1, in.read());
            }
        }
    }

    public void testXContentParsing() throws IOException {
        RankEvalResponse testItem = createRandomResponse();
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        // skip inserting random fields for:
        // - the `details` section, which can contain arbitrary queryIds
        // - everything under `failures` (exceptions parsing is quiet lenient)
        // - everything under `hits` (we test lenient SearchHit parsing elsewhere)
        Predicate<String> pathsToExclude = path -> (path.endsWith("details") || path.contains("failures") || path.contains("hits"));
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, pathsToExclude, random());
        RankEvalResponse parsedItem;
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parsedItem = PARSER.apply(parser, null);
            assertNull(parser.nextToken());
        }
        assertNotSame(testItem, parsedItem);
        // We cannot check equality of object here because some information (e.g.
        // SearchHit#shard) cannot fully be parsed back.
        assertEquals(testItem.getMetricScore(), parsedItem.getMetricScore(), 0.0);
        assertEquals(testItem.getPartialResults().keySet(), parsedItem.getPartialResults().keySet());
        for (EvalQueryQuality metricDetail : testItem.getPartialResults().values()) {
            EvalQueryQuality parsedEvalQueryQuality = parsedItem.getPartialResults().get(metricDetail.getId());
            assertToXContentEquivalent(
                toXContent(metricDetail, xContentType, humanReadable),
                toXContent(parsedEvalQueryQuality, xContentType, humanReadable),
                xContentType
            );
        }
        // Also exceptions that are parsed back will be different since they are re-wrapped during parsing.
        // However, we can check that there is the expected number
        assertEquals(testItem.getFailures().keySet(), parsedItem.getFailures().keySet());
        for (String queryId : testItem.getFailures().keySet()) {
            Exception ex = parsedItem.getFailures().get(queryId);
            assertThat(ex, instanceOf(ElasticsearchException.class));
        }
    }

    public void testToXContent() throws IOException {
        EvalQueryQuality coffeeQueryQuality = new EvalQueryQuality("coffee_query", 0.1);
        coffeeQueryQuality.addHitsAndRatings(Arrays.asList(searchHit("index", 123, 5), searchHit("index", 456, null)));
        RankEvalResponse response = new RankEvalResponse(
            0.123,
            Collections.singletonMap("coffee_query", coffeeQueryQuality),
            Collections.singletonMap("beer_query", new ParsingException(new XContentLocation(0, 0), "someMsg"))
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        String xContent = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString();
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "metric_score": 0.123,
              "details": {
                "coffee_query": {
                  "metric_score": 0.1,
                  "unrated_docs": [ { "_index": "index", "_id": "456" } ],
                  "hits": [
                    {
                      "hit": {
                        "_index": "index",
                        "_id": "123",
                        "_score": 1.0
                      },
                      "rating": 5
                    },
                    {
                      "hit": {
                        "_index": "index",
                        "_id": "456",
                        "_score": 1.0
                      },
                      "rating": null
                    }
                  ]
                }
              },
              "failures": {
                "beer_query": {
                  "error": {
                    "root_cause": [ { "type": "parsing_exception", "reason": "someMsg", "line": 0, "col": 0 } ],
                    "type": "parsing_exception",
                    "reason": "someMsg",
                    "line": 0,
                    "col": 0
                  }
                }
              }
            }"""), xContent);
    }

    private static RatedSearchHit searchHit(String index, int docId, Integer rating) {
        SearchHit hit = SearchHit.unpooled(docId, docId + "");
        hit.shard(new SearchShardTarget("testnode", new ShardId(index, "uuid", 0), null));
        hit.score(1.0f);
        return new RatedSearchHit(hit, rating != null ? OptionalInt.of(rating) : OptionalInt.empty());
    }
}
