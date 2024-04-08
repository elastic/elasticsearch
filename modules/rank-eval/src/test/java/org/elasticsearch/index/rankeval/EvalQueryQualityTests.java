/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class EvalQueryQualityTests extends ESTestCase {

    private static final NamedWriteableRegistry namedWritableRegistry = new NamedWriteableRegistry(
        new RankEvalPlugin().getNamedWriteables()
    );

    private static final ObjectParser<ParsedEvalQueryQuality, Void> PARSER = new ObjectParser<>(
        "eval_query_quality",
        true,
        ParsedEvalQueryQuality::new
    );

    private static final class ParsedEvalQueryQuality {
        double evaluationResult;
        MetricDetail optionalMetricDetails;
        List<RatedSearchHit> ratedHits = new ArrayList<>();
    }

    static {
        PARSER.declareDouble((obj, value) -> obj.evaluationResult = value, EvalQueryQuality.METRIC_SCORE_FIELD);
        PARSER.declareObject(
            (obj, value) -> obj.optionalMetricDetails = value,
            (p, c) -> parseMetricDetail(p),
            EvalQueryQuality.METRIC_DETAILS_FIELD
        );
        PARSER.declareObjectArray(
            (obj, list) -> obj.ratedHits = list,
            (p, c) -> RatedSearchHitTests.parseInstance(p),
            EvalQueryQuality.HITS_FIELD
        );
    }

    public static EvalQueryQuality parseInstance(XContentParser parser, String queryId) {
        var evalQuality = PARSER.apply(parser, null);
        return new EvalQueryQuality(queryId, evalQuality.evaluationResult, evalQuality.ratedHits, evalQuality.optionalMetricDetails);
    }

    private static MetricDetail parseMetricDetail(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        MetricDetail metricDetail = parser.namedObject(MetricDetail.class, parser.currentName(), null);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return metricDetail;
    }

    @SuppressWarnings("resource")
    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new RankEvalPlugin().getNamedXContent());
    }

    public static EvalQueryQuality randomEvalQueryQuality() {
        int numberOfSearchHits = randomInt(5);
        List<RatedSearchHit> ratedHits = new ArrayList<>();
        for (int i = 0; i < numberOfSearchHits; i++) {
            RatedSearchHit ratedSearchHit = RatedSearchHitTests.randomRatedSearchHit();
            // we need to associate each hit with an index name otherwise rendering will not work
            ratedSearchHit.getSearchHit().shard(new SearchShardTarget("_na_", new ShardId("index", "_na_", 0), null));
            ratedHits.add(ratedSearchHit);
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(randomAlphaOfLength(10), randomDoubleBetween(0.0, 1.0, true));
        if (randomBoolean()) {
            int metricDetail = randomIntBetween(0, 2);
            switch (metricDetail) {
                case 0 -> evalQueryQuality.setMetricDetails(new PrecisionAtK.Detail(randomIntBetween(0, 1000), randomIntBetween(0, 1000)));
                case 1 -> evalQueryQuality.setMetricDetails(new MeanReciprocalRank.Detail(randomIntBetween(0, 1000)));
                case 2 -> evalQueryQuality.setMetricDetails(
                    new DiscountedCumulativeGain.Detail(
                        randomDoubleBetween(0, 1, true),
                        randomBoolean() ? randomDoubleBetween(0, 1, true) : 0,
                        randomInt()
                    )
                );
                default -> throw new IllegalArgumentException("illegal randomized value in test");
            }
        }
        evalQueryQuality.addHitsAndRatings(ratedHits);
        return evalQueryQuality;
    }

    public void testSerialization() throws IOException {
        EvalQueryQuality original = randomEvalQueryQuality();
        EvalQueryQuality deserialized = copy(original);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testXContentParsing() throws IOException {
        EvalQueryQuality testItem = randomEvalQueryQuality();
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        // skip inserting random fields for:
        // - the root object, since we expect a particular queryId there in this test
        // - the `metric_details` section, which can potentially contain different namedXContent names
        // - everything under `hits` (we test lenient SearchHit parsing elsewhere)
        Predicate<String> pathsToExclude = path -> path.isEmpty() || path.endsWith("metric_details") || path.contains("hits");
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, pathsToExclude, random());
        EvalQueryQuality parsedItem;
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            String queryId = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsedItem = parseInstance(parser, queryId);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            assertNull(parser.nextToken());
        }
        assertNotSame(testItem, parsedItem);
        // we cannot check equality of object here because some information (e.g. SearchHit#shard) cannot fully be
        // parsed back after going through the rest layer. That's why we only check that the original and the parsed item
        // have the same xContent representation
        assertToXContentEquivalent(originalBytes, toXContent(parsedItem, xContentType, humanReadable), xContentType);
    }

    private static EvalQueryQuality copy(EvalQueryQuality original) throws IOException {
        return ESTestCase.copyWriteable(original, namedWritableRegistry, EvalQueryQuality::new);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(randomEvalQueryQuality(), EvalQueryQualityTests::copy, EvalQueryQualityTests::mutateTestItem);
    }

    private static EvalQueryQuality mutateTestItem(EvalQueryQuality original) {
        String id = original.getId();
        double metricScore = original.metricScore();
        List<RatedSearchHit> ratedHits = new ArrayList<>(original.getHitsAndRatings());
        MetricDetail metricDetails = original.getMetricDetails();
        switch (randomIntBetween(0, 3)) {
            case 0:
                id = id + "_";
                break;
            case 1:
                metricScore = metricScore + 0.1;
                break;
            case 2:
                if (metricDetails == null) {
                    metricDetails = new PrecisionAtK.Detail(1, 5);
                } else {
                    metricDetails = null;
                }
                break;
            case 3:
                ratedHits.add(RatedSearchHitTests.randomRatedSearchHit());
                break;
            default:
                throw new IllegalStateException("The test should only allow four parameters mutated");
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(id, metricScore);
        evalQueryQuality.setMetricDetails(metricDetails);
        evalQueryQuality.addHitsAndRatings(ratedHits);
        return evalQueryQuality;
    }
}
