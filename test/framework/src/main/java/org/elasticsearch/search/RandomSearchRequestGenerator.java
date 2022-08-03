/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.generateRandomStringArray;
import static org.elasticsearch.test.ESTestCase.mockScript;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomByte;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFloat;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomPositiveTimeValue;
import static org.elasticsearch.test.ESTestCase.randomShort;
import static org.elasticsearch.test.ESTestCase.randomTimeValue;

/**
 * Builds random search requests.
 */
public class RandomSearchRequestGenerator {
    private RandomSearchRequestGenerator() {}

    /**
     * Build a random search request.
     *
     * @param randomSearchSourceBuilder builds a random {@link SearchSourceBuilder}. You can use
     *        {@link #randomSearchSourceBuilder}.
     */
    public static SearchRequest randomSearchRequest(Supplier<SearchSourceBuilder> randomSearchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);
        if (randomBoolean()) {
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        }
        if (randomBoolean()) {
            searchRequest.indices(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            searchRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            searchRequest.preference(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.requestCache(randomBoolean());
        }
        if (randomBoolean()) {
            searchRequest.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.scroll(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            searchRequest.searchType(randomFrom(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        }
        if (randomBoolean()) {
            searchRequest.source(randomSearchSourceBuilder.get());
        }
        if (randomBoolean()) {
            searchRequest.setForceSyntheticSource(randomBoolean());
        }
        return searchRequest;
    }

    public static SearchSourceBuilder randomSearchSourceBuilder(
        Supplier<HighlightBuilder> randomHighlightBuilder,
        Supplier<SuggestBuilder> randomSuggestBuilder,
        Supplier<RescorerBuilder<?>> randomRescoreBuilder,
        Supplier<List<SearchExtBuilder>> randomExtBuilders,
        Supplier<CollapseBuilder> randomCollapseBuilder,
        Supplier<Map<String, Object>> randomRuntimeMappings
    ) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        if (randomBoolean()) {
            builder.from(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.size(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.explain(randomBoolean());
        }
        if (randomBoolean()) {
            builder.version(randomBoolean());
        }
        if (randomBoolean()) {
            builder.seqNoAndPrimaryTerm(randomBoolean());
        }
        if (randomBoolean()) {
            builder.trackScores(randomBoolean());
        }
        if (randomBoolean()) {
            builder.minScore(randomFloat() * 1000);
        }
        if (randomBoolean()) {
            builder.timeout(TimeValue.parseTimeValue(randomTimeValue(), null, "timeout"));
        }
        if (randomBoolean()) {
            builder.terminateAfter(randomIntBetween(1, 100000));
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                builder.trackTotalHits(randomBoolean());
            } else {
                builder.trackTotalHitsUpTo(
                    randomIntBetween(SearchContext.TRACK_TOTAL_HITS_DISABLED, SearchContext.TRACK_TOTAL_HITS_ACCURATE)
                );
            }
        }

        switch (randomInt(2)) {
            case 0 -> builder.storedFields();
            case 1 -> builder.storedField("_none_");
            case 2 -> {
                int fieldsSize = randomInt(25);
                List<String> fields = new ArrayList<>(fieldsSize);
                for (int i = 0; i < fieldsSize; i++) {
                    fields.add(randomAlphaOfLengthBetween(5, 50));
                }
                builder.storedFields(fields);
            }
            default -> throw new IllegalStateException();
        }

        if (randomBoolean()) {
            int numFields = randomInt(5);
            for (int i = 0; i < numFields; i++) {
                String field = randomAlphaOfLengthBetween(5, 10);
                String format = randomBoolean() ? randomAlphaOfLengthBetween(5, 10) : null;
                builder.fetchField(new FieldAndFormat(field, format));
            }
        }

        if (randomBoolean()) {
            int numFields = randomInt(5);
            for (int i = 0; i < numFields; i++) {
                String field = randomAlphaOfLengthBetween(5, 10);
                String format = randomBoolean() ? randomAlphaOfLengthBetween(5, 10) : null;
                builder.docValueField(field, format);
            }
        }

        if (randomBoolean()) {
            int scriptFieldsSize = randomInt(25);
            for (int i = 0; i < scriptFieldsSize; i++) {
                if (randomBoolean()) {
                    builder.scriptField(randomAlphaOfLengthBetween(5, 50), mockScript("foo"), randomBoolean());
                } else {
                    builder.scriptField(randomAlphaOfLengthBetween(5, 50), mockScript("foo"));
                }
            }
        }
        if (randomBoolean()) {
            FetchSourceContext fetchSourceContext;
            int branch = randomInt(5);
            String[] includes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < includes.length; i++) {
                includes[i] = randomAlphaOfLengthBetween(5, 20);
            }
            String[] excludes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < excludes.length; i++) {
                excludes[i] = randomAlphaOfLengthBetween(5, 20);
            }
            fetchSourceContext = switch (branch) {
                case 0 -> FetchSourceContext.of(randomBoolean());
                case 1 -> FetchSourceContext.of(true, includes, excludes);
                case 2 -> FetchSourceContext.of(
                    true,
                    new String[] { randomAlphaOfLengthBetween(5, 20) },
                    new String[] { randomAlphaOfLengthBetween(5, 20) }
                );
                case 3 -> FetchSourceContext.of(true, includes, excludes);
                case 4 -> FetchSourceContext.of(true, includes, null);
                case 5 -> FetchSourceContext.of(true, new String[] { randomAlphaOfLengthBetween(5, 20) }, null);
                default -> throw new IllegalStateException();
            };
            builder.fetchSource(fetchSourceContext);
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 20);
            List<String> statsGroups = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                statsGroups.add(randomAlphaOfLengthBetween(5, 20));
            }
            builder.stats(statsGroups);
        }
        if (randomBoolean()) {
            int indexBoostSize = randomIntBetween(1, 10);
            for (int i = 0; i < indexBoostSize; i++) {
                builder.indexBoost(randomAlphaOfLengthBetween(5, 20), randomFloat() * 10);
            }
        }
        if (randomBoolean()) {
            builder.query(QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            builder.postFilter(QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20)));
        }

        if (randomBoolean()) {
            String field = randomAlphaOfLength(6);
            int dim = randomIntBetween(2, 30);
            float[] vector = new float[dim];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = randomFloat();
            }
            int k = randomIntBetween(1, 100);
            int numCands = randomIntBetween(k, 1000);
            builder.knnSearch(new KnnSearchBuilder(field, vector, k, numCands));
        }

        if (randomBoolean()) {
            int numSorts = randomIntBetween(1, 5);
            for (int i = 0; i < numSorts; i++) {
                int branch = randomInt(5);
                switch (branch) {
                    case 0 -> builder.sort(SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                    case 1 -> builder.sort(
                        SortBuilders.geoDistanceSort(randomAlphaOfLengthBetween(5, 20), AbstractQueryTestCase.randomGeohash(1, 12))
                            .order(randomFrom(SortOrder.values()))
                    );
                    case 2 -> builder.sort(SortBuilders.scoreSort().order(randomFrom(SortOrder.values())));
                    case 3 -> builder.sort(
                        SortBuilders.scriptSort(
                            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "foo", emptyMap()),
                            ScriptSortBuilder.ScriptSortType.NUMBER
                        ).order(randomFrom(SortOrder.values()))
                    );
                    case 4 -> builder.sort(randomAlphaOfLengthBetween(5, 20));
                    case 5 -> builder.sort(randomAlphaOfLengthBetween(5, 20), randomFrom(SortOrder.values()));
                }
            }
        }

        if (randomBoolean()) {
            int numSearchFrom = randomIntBetween(1, 5);
            try {
                // We build a json version of the search_from first in order to
                // ensure that every number type remain the same before/after xcontent (de)serialization.
                // This is not a problem because the final type of each field value is extracted from associated sort field.
                // This little trick ensure that equals and hashcode are the same when using the xcontent serialization.
                XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
                jsonBuilder.startObject();
                jsonBuilder.startArray("search_from");
                for (int i = 0; i < numSearchFrom; i++) {
                    int branch = randomInt(8);
                    switch (branch) {
                        case 0 -> jsonBuilder.value(randomInt());
                        case 1 -> jsonBuilder.value(randomFloat());
                        case 2 -> jsonBuilder.value(randomLong());
                        case 3 -> jsonBuilder.value(randomDouble());
                        case 4 -> jsonBuilder.value(randomAlphaOfLengthBetween(5, 20));
                        case 5 -> jsonBuilder.value(randomBoolean());
                        case 6 -> jsonBuilder.value(randomByte());
                        case 7 -> jsonBuilder.value(randomShort());
                        case 8 -> jsonBuilder.value(new Text(randomAlphaOfLengthBetween(5, 20)));
                    }
                }
                jsonBuilder.endArray();
                jsonBuilder.endObject();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(jsonBuilder).streamInput());
                parser.nextToken();
                parser.nextToken();
                parser.nextToken();
                builder.searchAfter(SearchAfterBuilder.fromXContent(parser).getSortValues());
            } catch (IOException e) {
                throw new RuntimeException("Error building search_from", e);
            }
        }
        if (randomBoolean()) {
            builder.highlighter(randomHighlightBuilder.get());
        }
        if (randomBoolean()) {
            builder.suggest(randomSuggestBuilder.get());
        }
        if (randomBoolean()) {
            int numRescores = randomIntBetween(1, 5);
            for (int i = 0; i < numRescores; i++) {
                builder.addRescorer(randomRescoreBuilder.get());
            }
        }
        if (randomBoolean()) {
            builder.aggregation(AggregationBuilders.avg(randomAlphaOfLengthBetween(5, 20)).field("foo"));
        }
        if (randomBoolean()) {
            builder.ext(randomExtBuilders.get());
        }
        if (randomBoolean()) {
            String field = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 20);
            int max = between(2, 1000);
            int id = randomInt(max - 1);
            if (field == null) {
                builder.slice(new SliceBuilder(id, max));
            } else {
                builder.slice(new SliceBuilder(field, id, max));
            }
        }
        if (randomBoolean()) {
            builder.collapse(randomCollapseBuilder.get());
        }
        if (randomBoolean()) {
            PointInTimeBuilder pit = new PointInTimeBuilder(randomAlphaOfLengthBetween(3, 10));
            if (randomBoolean()) {
                pit.setKeepAlive(TimeValue.timeValueMinutes(randomIntBetween(1, 60)));
            }
            builder.pointInTimeBuilder(pit);
        }
        if (randomBoolean()) {
            builder.runtimeMappings(randomRuntimeMappings.get());
        }
        return builder;
    }
}
