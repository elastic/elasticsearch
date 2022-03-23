/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
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
            searchRequest.types(generateRandomStringArray(10, 10, false, false));
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
            SearchSourceBuilder source = randomSearchSourceBuilder.get();
            searchRequest.source(source);
            if (source.pointInTimeBuilder() != null) {
                searchRequest.indices(source.pointInTimeBuilder().getActualIndices());
            }
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
            case 0:
                builder.storedFields();
                break;
            case 1:
                builder.storedField("_none_");
                break;
            case 2:
                int fieldsSize = randomInt(25);
                List<String> fields = new ArrayList<>(fieldsSize);
                for (int i = 0; i < fieldsSize; i++) {
                    fields.add(randomAlphaOfLengthBetween(5, 50));
                }
                builder.storedFields(fields);
                break;
            default:
                throw new IllegalStateException();
        }

        if (randomBoolean()) {
            int numFields = randomInt(5);
            for (int i = 0; i < numFields; i++) {
                builder.fetchField(randomAlphaOfLengthBetween(5, 10));
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
            switch (branch) {
                case 0:
                    fetchSourceContext = new FetchSourceContext(randomBoolean());
                    break;
                case 1:
                    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                    break;
                case 2:
                    fetchSourceContext = new FetchSourceContext(
                        true,
                        new String[] { randomAlphaOfLengthBetween(5, 20) },
                        new String[] { randomAlphaOfLengthBetween(5, 20) }
                    );
                    break;
                case 3:
                    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                    break;
                case 4:
                    fetchSourceContext = new FetchSourceContext(true, includes, null);
                    break;
                case 5:
                    fetchSourceContext = new FetchSourceContext(true, new String[] { randomAlphaOfLengthBetween(5, 20) }, null);
                    break;
                default:
                    throw new IllegalStateException();
            }
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
            int numSorts = randomIntBetween(1, 5);
            for (int i = 0; i < numSorts; i++) {
                int branch = randomInt(5);
                switch (branch) {
                    case 0:
                        builder.sort(SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                        break;
                    case 1:
                        builder.sort(
                            SortBuilders.geoDistanceSort(randomAlphaOfLengthBetween(5, 20), AbstractQueryTestCase.randomGeohash(1, 12))
                                .order(randomFrom(SortOrder.values()))
                        );
                        break;
                    case 2:
                        builder.sort(SortBuilders.scoreSort().order(randomFrom(SortOrder.values())));
                        break;
                    case 3:
                        builder.sort(
                            SortBuilders.scriptSort(
                                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "foo", emptyMap()),
                                ScriptSortBuilder.ScriptSortType.NUMBER
                            ).order(randomFrom(SortOrder.values()))
                        );
                        break;
                    case 4:
                        builder.sort(randomAlphaOfLengthBetween(5, 20));
                        break;
                    case 5:
                        builder.sort(randomAlphaOfLengthBetween(5, 20), randomFrom(SortOrder.values()));
                        break;
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
                        case 0:
                            jsonBuilder.value(randomInt());
                            break;
                        case 1:
                            jsonBuilder.value(randomFloat());
                            break;
                        case 2:
                            jsonBuilder.value(randomLong());
                            break;
                        case 3:
                            jsonBuilder.value(randomDouble());
                            break;
                        case 4:
                            jsonBuilder.value(randomAlphaOfLengthBetween(5, 20));
                            break;
                        case 5:
                            jsonBuilder.value(randomBoolean());
                            break;
                        case 6:
                            jsonBuilder.value(randomByte());
                            break;
                        case 7:
                            jsonBuilder.value(randomShort());
                            break;
                        case 8:
                            jsonBuilder.value(new Text(randomAlphaOfLengthBetween(5, 20)));
                            break;
                    }
                }
                jsonBuilder.endArray();
                jsonBuilder.endObject();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        BytesReference.bytes(jsonBuilder).streamInput()
                    );
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
            builder.pointInTimeBuilder(randomPointInTimeBuilder());
        }
        if (randomBoolean()) {
            builder.runtimeMappings(randomRuntimeMappings.get());
        }
        return builder;
    }

    public static PointInTimeBuilder randomPointInTimeBuilder() {
        final int numResults = randomIntBetween(1, 10);
        final List<SearchPhaseResult> queryResults = new ArrayList<>(numResults);
        for (int i = 0; i < numResults; i++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node_" + randomIntBetween(1, 5),
                new ShardId("index_" + randomIntBetween(1, 5), "n/a", randomIntBetween(0, 5)),
                randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null
            );
            ShardSearchContextId shardSearchContextId = new ShardSearchContextId(
                ESTestCase.randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomBoolean() ? randomAlphaOfLength(10) : null
            );
            queryResults.add(new SearchPhaseResult() {
                @Override
                public SearchShardTarget getSearchShardTarget() {
                    return searchShardTarget;
                }

                @Override
                public ShardSearchContextId getContextId() {
                    return shardSearchContextId;
                }
            });
        }
        final Version version = VersionUtils.randomVersion(ESTestCase.random());
        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (SearchPhaseResult result : queryResults) {
            if (randomBoolean()) {
                final QueryBuilder queryBuilder;
                if (randomBoolean()) {
                    queryBuilder = new TermQueryBuilder(ESTestCase.randomAlphaOfLength(10), ESTestCase.randomAlphaOfLength(10));
                } else if (randomBoolean()) {
                    queryBuilder = new MatchAllQueryBuilder();
                } else {
                    queryBuilder = new IdsQueryBuilder().addIds(ESTestCase.randomAlphaOfLength(10));
                }
                final AliasFilter aliasFilter;
                if (randomBoolean()) {
                    aliasFilter = new AliasFilter(queryBuilder);
                } else if (randomBoolean()) {
                    aliasFilter = new AliasFilter(queryBuilder, "alias-" + between(1, 10));
                } else {
                    aliasFilter = AliasFilter.EMPTY;
                }
                aliasFilters.put(result.getSearchShardTarget().getShardId().getIndex().getUUID(), aliasFilter);
            }
        }
        final String pitId = SearchContextId.encode(queryResults, aliasFilters, version);
        PointInTimeBuilder pit = new PointInTimeBuilder(pitId);
        if (randomBoolean()) {
            pit.setKeepAlive(TimeValue.timeValueMinutes(randomIntBetween(1, 60)));
        }
        return pit;
    }
}
