/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileSelection;
import org.elasticsearch.search.profile.ProfileSelection.Names;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import static org.elasticsearch.search.profile.CodecReadProfilingTestUtils.assumeCodecReadProfilingEnabled;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@LuceneTestCase.SuppressCodecs("*")
public class ColumnarSearchProfileIT extends ESIntegTestCase {

    private static final String INDEX = "columnar_search_profile";
    private static final int DOCS = 2000;
    private static final int VALUE_LENGTH = 100;
    private static final String KEYWORD_LENGTH_SCRIPT = "doc['kw'].value.length()";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(KeywordLengthScriptPlugin.class);
    }

    public static class KeywordLengthScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(KEYWORD_LENGTH_SCRIPT, vars -> {
                final Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
                return ((ScriptDocValues.Strings) doc.get("kw")).getValue().length();
            });
        }
    }

    public void testAScriptThatFirstReadsAColumnarKeywordWhileScoringReportsTheReadOnTheScriptScoreNode() {
        createIndexWithUniqueKeywords();
        final Script script = new Script(ScriptType.INLINE, MockScriptPlugin.NAME, KEYWORD_LENGTH_SCRIPT, Map.of());
        final SearchRequestBuilder search = prepareSearch(INDEX).setQuery(
            QueryBuilders.scriptScoreQuery(QueryBuilders.matchAllQuery(), script)
        ).setSize(10);
        final List<String> unprofiledHits = new ArrayList<>();
        assertResponse(search, response -> {
            assertNoFailures(response);
            for (SearchHit hit : response.getHits().getHits()) {
                unprofiledHits.add(hit.getId() + "=" + hit.getScore());
            }
        });
        assertThat(unprofiledHits.size(), equalTo(10));

        assertResponse(search.setProfile(true), response -> {
            assertNoFailures(response);
            final List<String> hits = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                hits.add(hit.getId() + "=" + hit.getScore());
            }
            assertThat("profiling does not change the hits or their scores", hits, equalTo(unprofiledHits));
            final List<ProfileResult> codecNodes = codecNodes(response.getSearchProfileShardResults());
            if (ProfileSelection.CODEC_READ_PROFILING_FEATURE_FLAG.isEnabled() == false) {
                assertThat(codecNodes, equalTo(List.of()));
                return;
            }
            final ProfileResult scriptScore = theOnlyQueryNode(response.getSearchProfileShardResults());
            assertThat(scriptScore.getQueryName(), equalTo("ScriptScoreQuery"));
            // NOTE: The match_all sub-query opens no codec reader, so every count on the node comes from the script, which
            // opens its reader on its first score call, after the scorer was built.
            assertThat(
                "the script's reader is counted on the script_score node",
                flatten(scriptScore.getTotals()).get("ColumNAR.readers.opened"),
                greaterThan(0L)
            );
        });
    }

    public void testAProfiledQueryOverAColumnarKeywordReportsTheCodecWorkOnlyWithCodecReadProfiling() {
        createIndexWithUniqueKeywords();
        assertThat("the values were written with the ColumNAR codec", keywordDocValuesFormat(), equalTo(ColumnarFormat.NAME));

        assertResponse(prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("kw", value(7))).setProfile(true).setSize(10), response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            if (ProfileSelection.CODEC_READ_PROFILING_FEATURE_FLAG.isEnabled() == false) {
                assertFalse("the query is profiled as before", response.getSearchProfileShardResults().isEmpty());
                assertThat("and no node reports codec reads", codecNodes(response.getSearchProfileShardResults()), equalTo(List.of()));
                return;
            }
            // NOTE: Which metrics a read records belongs to the codec and is pinned by its own tests; this checks the wiring.
            final Map<String, Long> columnar = columnarOfTheOnlyQueryNode(response.getSearchProfileShardResults());
            assertTrue("the codec counted work on the node: " + columnar, columnar.values().stream().anyMatch(count -> count > 0));
            assertTrue(
                "the codec timed work on the node: " + columnar,
                columnar.keySet().stream().anyMatch(key -> key.endsWith("_time_nanos"))
            );
        });
    }

    public void testAQueryOverAnotherFieldReportsNoColumnarCounts() {
        createIndexWithUniqueKeywords();
        assertResponse(prepareSearch(INDEX).setQuery(QueryBuilders.matchAllQuery()).setProfile(true).setSize(1), response -> {
            for (SearchProfileShardResult shard : response.getSearchProfileShardResults().values()) {
                for (QueryProfileShardResult search : shard.getQueryProfileResults()) {
                    for (ProfileResult node : search.getQueryResults()) {
                        assertThat(node.getTotals(), equalTo(Map.of()));
                    }
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void testEachSelectorExpandsOnlyItsOwnDimension() {
        assumeCodecReadProfilingEnabled();
        final String index = "columnar_selection_profile";
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.merge.policy.max_merge_at_once", 100)
                    .put("index.merge.policy.segments_per_tier", 100)
            ).setMapping("a", "type=keyword", "b", "type=keyword")
        );
        for (int round = 0; round < between(2, 3); round++) {
            final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < 50; doc++) {
                bulk.add(client().prepareIndex(index).setSource("a", value(doc), "b", value(doc + 1)));
            }
            assertFalse(bulk.get().hasFailures());
            assertNoFailures(indicesAdmin().prepareFlush(index).get());
        }
        final Set<String> segmentKeys = new TreeSet<>();
        segmentNames(index).forEach(segment -> segmentKeys.add(index + ":0/" + segment));
        final String segment = randomFrom(segmentNames(index));
        final String segmentKey = index + ":0/" + segment;
        final String segmentName = randomFrom(segment, segmentKey);
        // NOTE: Each clause reads its own field, so each query node holds the work of one field.
        final BoolQueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("a", value(7)))
            .should(QueryBuilders.termQuery("b", value(7)));

        assertResponse(search(index, query, ProfileSelection.NONE), response -> {
            for (ProfileResult node : fieldNodes(response)) {
                assertTrue("the totals are always reported", counters(node.getTotals()).values().stream().anyMatch(count -> count > 0));
                assertThat("no segment is asked for", node.getSegments(), equalTo(Map.of()));
                assertThat("no field is asked for", node.getFields(), equalTo(Map.of()));
            }
        });

        assertResponse(search(index, query, ProfileSelection.of(Names.of(List.of(segmentName)), null)), response -> {
            for (ProfileResult node : fieldNodes(response)) {
                assertThat(node.getSegments().keySet(), equalTo(Set.of(segmentKey)));
                final Map<String, Object> entry = (Map<String, Object>) node.getSegments().get(segmentKey);
                assertThat(
                    "a named segment reports its identity and totals only",
                    entry.keySet(),
                    equalTo(Set.of("index", "shard", "metrics"))
                );
                assertThat(entry.get("index"), equalTo(index));
                assertThat("a segment does not expand the fields", node.getFields(), equalTo(Map.of()));
            }
        });

        assertResponse(search(index, query, ProfileSelection.of(Names.EVERY, null)), response -> {
            for (ProfileResult node : fieldNodes(response)) {
                assertThat(node.getSegments().keySet(), equalTo(segmentKeys));
                final Map<String, Long> segmentsSum = new TreeMap<>();
                for (Object value : node.getSegments().values()) {
                    final Map<String, Object> entry = (Map<String, Object>) value;
                    assertThat("every segment describes the segments", entry, hasKey("max_doc"));
                    assertThat("every segment does not expand the fields", entry, not(hasKey("fields")));
                    addCounts(segmentsSum, ((Map<String, Object>) entry.get("metrics")).get("ColumNAR"));
                }
                assertThat("the segments partition the work of the node", segmentsSum, equalTo(counters(node.getTotals())));
                assertThat(node.getFields(), equalTo(Map.of()));
            }
        });

        assertResponse(search(index, query, ProfileSelection.of(null, Names.of(List.of("a")))), response -> {
            for (ProfileResult node : fieldNodes(response)) {
                assertThat("a field does not expand the segments", node.getSegments(), equalTo(Map.of()));
                if (fieldOf(node).equals("a")) {
                    final Map<String, Object> entry = (Map<String, Object>) node.getFields().get("a");
                    assertThat("a named field reports its totals only", entry.keySet(), equalTo(Set.of("metrics")));
                    assertThat(
                        "the node reads field a only, so the field's totals are the node's",
                        counters(entry.get("metrics")),
                        equalTo(counters(node.getTotals()))
                    );
                } else {
                    assertThat("an unselected field is not reported", node.getFields(), equalTo(Map.of()));
                }
            }
        });

        assertResponse(search(index, query, ProfileSelection.of(null, Names.EVERY)), response -> {
            for (ProfileResult node : fieldNodes(response)) {
                assertThat("every field does not expand the segments", node.getSegments(), equalTo(Map.of()));
                assertThat(node.getFields().keySet(), equalTo(Set.of(fieldOf(node))));
                final Map<String, Object> entry = (Map<String, Object>) node.getFields().get(fieldOf(node));
                assertThat("every field describes the fields", entry, hasKey("field_info"));
                assertThat(counters(entry.get("metrics")), equalTo(counters(node.getTotals())));
            }
        });

        assertResponse(search(index, query, ProfileSelection.of(Names.of(List.of(segmentName)), Names.of(List.of("a")))), response -> {
            for (ProfileResult node : fieldNodes(response)) {
                final Map<String, Object> entry = (Map<String, Object>) node.getSegments().get(segmentKey);
                assertThat("no description is asked for", entry, not(hasKey("max_doc")));
                if (fieldOf(node).equals("b")) {
                    assertThat("a field that is not selected is not broken down", entry, not(hasKey("fields")));
                    assertThat(node.getFields(), equalTo(Map.of()));
                    continue;
                }
                final Map<String, Object> pair = (Map<String, Object>) ((Map<String, Object>) entry.get("fields")).get("a");
                assertThat(pair.keySet(), equalTo(Set.of("metrics")));
                final Map<String, Long> pairCounts = counters(pair.get("metrics"));
                final Map<String, Long> segmentCounts = counters(entry.get("metrics"));
                final Map<String, Long> fieldCounts = counters(((Map<String, Object>) node.getFields().get("a")).get("metrics"));
                pairCounts.forEach((name, count) -> {
                    assertThat(
                        "the field within the segment is part of the segment: " + name,
                        count,
                        lessThanOrEqualTo(segmentCounts.get(name))
                    );
                    assertThat("and part of the field: " + name, count, lessThanOrEqualTo(fieldCounts.get(name)));
                });
            }
        });

        assertResponse(search(index, query, ProfileSelection.EVERYTHING), response -> {
            for (ProfileResult node : fieldNodes(response)) {
                assertThat(node.getSegments().keySet(), equalTo(segmentKeys));
                for (Object value : node.getSegments().values()) {
                    final Map<String, Object> entry = (Map<String, Object>) value;
                    assertThat(entry, hasKey("max_doc"));
                    final Map<String, Object> pairs = (Map<String, Object>) entry.get("fields");
                    assertThat(pairs.keySet(), equalTo(Set.of(fieldOf(node))));
                    final Map<String, Object> pair = (Map<String, Object>) pairs.get(fieldOf(node));
                    assertThat(pair, hasKey("field_info"));
                    assertThat(
                        "the node's only field is the whole segment",
                        counters(pair.get("metrics")),
                        equalTo(counters(entry.get("metrics")))
                    );
                }
                assertThat(node.getFields().keySet(), equalTo(Set.of(fieldOf(node))));
            }
        });
    }

    private static List<ProfileResult> fieldNodes(org.elasticsearch.action.search.SearchResponse response) {
        final List<ProfileResult> nodes = codecNodes(response.getSearchProfileShardResults());
        assertThat("each clause reads its own field", nodes.size(), equalTo(2));
        return nodes;
    }

    private static String fieldOf(ProfileResult node) {
        return node.getLuceneDescription().substring(0, node.getLuceneDescription().indexOf(':'));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Long> counters(Object byCodec) {
        final Map<String, Long> counts = new TreeMap<>();
        ((Map<String, Object>) byCodec).values().forEach(components -> addCounts(counts, components));
        return counts;
    }

    public void testASearcherAcquiredBeforeAMergeKeepsTheSegmentNamesItWasOpenedWith() {
        assumeCodecReadProfilingEnabled();
        final String index = "columnar_merge_profile";
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.merge.policy.max_merge_at_once", 100)
                    .put("index.merge.policy.segments_per_tier", 100)
            ).setMapping("kw", "type=keyword")
        );
        for (int round = 0; round < between(2, 4); round++) {
            final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < 50; doc++) {
                bulk.add(client().prepareIndex(index).setSource("kw", value(doc)));
            }
            assertFalse(bulk.get().hasFailures());
            assertNoFailures(indicesAdmin().prepareFlush(index).get());
        }
        final Set<String> before = segmentNames(index);
        assertThat("the index has several segments", before.size(), greaterThan(1));
        final ProfileSelection oldSegments = ProfileSelection.of(Names.of(before), null);

        final BytesReference pit = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(index).keepAlive(TimeValue.timeValueMinutes(1))
        ).actionGet().getPointInTimeId();
        try {
            assertNoFailures(indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get());
            indicesAdmin().prepareRefresh(index).get();
            final Set<String> after = segmentNames(index);
            assertThat(after.size(), equalTo(1));
            assertFalse("the merged segment has a new name", before.containsAll(after));

            final SearchSourceBuilder onTheSnapshot = new SearchSourceBuilder().query(QueryBuilders.termQuery("kw", value(7)))
                .profile(true)
                .profileSelection(oldSegments);
            assertResponse(prepareSearch().setSource(onTheSnapshot).setPointInTime(new PointInTimeBuilder(pit)), response -> {
                final Set<String> reported = new TreeSet<>();
                codecNodes(response.getSearchProfileShardResults()).forEach(node -> reported.addAll(node.getSegments().keySet()));
                assertThat(
                    "the acquired searcher still reads the segments it was opened with",
                    reported,
                    equalTo(before.stream().map(segment -> index + ":0/" + segment).collect(java.util.stream.Collectors.toSet()))
                );
            });
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet();
        }

        assertResponse(prepareSearch(index).setSource(termSearch(oldSegments)), response -> {
            for (ProfileResult node : codecNodes(response.getSearchProfileShardResults())) {
                assertThat("a merged-away segment selects nothing", node.getSegments(), equalTo(Map.of()));
                assertFalse("the totals are reported all the same", node.getTotals().isEmpty());
            }
        });
    }

    private static SearchSourceBuilder termSearch(ProfileSelection selection) {
        return new SearchSourceBuilder().query(QueryBuilders.termQuery("kw", value(7))).profile(true).profileSelection(selection);
    }

    private Set<String> segmentNames(String index) {
        final Set<String> names = new TreeSet<>();
        for (Segment segment : indicesAdmin().prepareSegments(index).get().getIndices().get(index).getShards().get(0).shards()[0]
            .getSegments()) {
            if (segment.isSearch()) {
                names.add(segment.getName());
            }
        }
        return names;
    }

    private SearchRequestBuilder search(String index, BoolQueryBuilder query, ProfileSelection selection) {
        return prepareSearch(index).setSource(new SearchSourceBuilder().query(query).size(10).profile(true).profileSelection(selection));
    }

    private static List<ProfileResult> codecNodes(Map<String, SearchProfileShardResult> profiles) {
        final List<ProfileResult> nodes = new ArrayList<>();
        for (SearchProfileShardResult shard : profiles.values()) {
            for (QueryProfileShardResult search : shard.getQueryProfileResults()) {
                for (ProfileResult root : search.getQueryResults()) {
                    collectCodecNodes(root, nodes);
                }
            }
        }
        return nodes;
    }

    private static void collectCodecNodes(ProfileResult node, List<ProfileResult> nodes) {
        assertThat("codec counts are not debug output", node.getDebugInfo(), not(hasKey("read")));
        if (node.getTotals().isEmpty() == false) {
            nodes.add(node);
        }
        for (ProfileResult child : node.getProfiledChildren()) {
            collectCodecNodes(child, nodes);
        }
    }

    @SuppressWarnings("unchecked")
    private static void addCounts(Map<String, Long> into, Object components) {
        ((Map<String, Object>) components).forEach((component, metrics) -> {
            if (metrics instanceof Map<?, ?> byName) {
                byName.forEach((name, value) -> {
                    if (value instanceof Number count) {
                        into.merge(component + "." + name, count.longValue(), Long::sum);
                    }
                });
            }
        });
    }

    private static ProfileResult theOnlyQueryNode(Map<String, SearchProfileShardResult> profiles) {
        final List<ProfileResult> roots = new ArrayList<>();
        for (SearchProfileShardResult shard : profiles.values()) {
            for (QueryProfileShardResult search : shard.getQueryProfileResults()) {
                roots.addAll(search.getQueryResults());
            }
        }
        assertThat(roots.size(), equalTo(1));
        return roots.get(0);
    }

    private Map<String, Long> columnarOfTheOnlyQueryNode(Map<String, SearchProfileShardResult> profiles) {
        Map<String, Long> columnar = null;
        for (SearchProfileShardResult shard : profiles.values()) {
            for (QueryProfileShardResult search : shard.getQueryProfileResults()) {
                for (ProfileResult node : search.getQueryResults()) {
                    if (node.getTotals().isEmpty() == false) {
                        assertNull("the counts appear on one node only", columnar);
                        columnar = flatten(node.getTotals());
                    }
                }
            }
        }
        assertNotNull("no node reported columnar counts", columnar);
        return columnar;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Long> flatten(Map<String, Object> codecs) {
        final Map<String, Long> flat = new TreeMap<>();
        for (Map.Entry<String, Object> codec : codecs.entrySet()) {
            ((Map<String, Map<String, Object>>) codec.getValue()).forEach(
                (component, metrics) -> metrics.forEach(
                    (name, value) -> flat.put(
                        codec.getKey() + "." + component + "." + name,
                        ((Number) (value instanceof Map<?, ?> timer ? timer.get("value") : value)).longValue()
                    )
                )
            );
        }
        return flat;
    }

    private void createIndexWithUniqueKeywords() {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("kw", "type=keyword")
        );
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int doc = 0; doc < DOCS; doc++) {
            bulk.add(client().prepareIndex(INDEX).setSource("kw", value(doc)));
        }
        assertFalse(bulk.get().hasFailures());
        assertNoFailures(indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get());
    }

    private static String value(int doc) {
        return "unique-value-".repeat(VALUE_LENGTH / 13 + 1).substring(0, VALUE_LENGTH - 6) + String.format(Locale.ROOT, "%06d", doc);
    }

    private String keywordDocValuesFormat() {
        IndexShard found = null;
        for (IndicesService node : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (node.hasIndex(resolveIndex(INDEX))) {
                found = node.indexServiceSafe(resolveIndex(INDEX)).getShard(0);
                break;
            }
        }
        assertNotNull("no data node hosts [" + INDEX + "]", found);
        final IndexShard shard = found;
        try (Engine.Searcher searcher = shard.acquireSearcher("columnar-search-profile-it")) {
            String format = null;
            for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                final FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo("kw");
                if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                    format = fieldInfo.getAttribute("PerFieldDocValuesFormat.format");
                }
            }
            assertNotNull("expected a keyword doc-values field in [" + INDEX + "]", format);
            return format;
        }
    }
}
