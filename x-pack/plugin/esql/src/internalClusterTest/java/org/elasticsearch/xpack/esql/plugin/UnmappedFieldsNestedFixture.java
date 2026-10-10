/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;

/**
 * Indices that map {@code nested_punk.subfield} differently, and what LOAD and LOAD_ALL must return over any mix of them: a named field
 * resolves by each row's own index, whatever it shares a node or batch with, and LOAD_ALL discovers nothing below a nested path.
 */
final class UnmappedFieldsNestedFixture {

    static final String FIELD = "nested_punk.subfield";

    private static final String ID_COLUMN = "id:keyword";
    private static final String FIELD_COLUMN = FIELD + ":keyword";
    private static final String COUNT_COLUMN = "c:long";

    /**
     * How an index maps {@code nested_punk}. Only batches of {@link #OBJECT} shards plan the field as mapped and push filters on it to
     * Lucene, so batches of different kinds run different plans that must still agree.
     */
    enum Kind {
        NESTED("n_", """
            {
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "nested_punk": {
                  "type": "nested",%s
                  "properties": {
                    "subfield": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }"""),
        MISSING("m_", """
            {
              "dynamic": false,
              "properties": {
                "id": {
                  "type": "keyword"
                }
              }
            }"""),
        OBJECT("o_", """
            {
              "dynamic": false,
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "nested_punk": {
                  "properties": {
                    "subfield": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""");

        private final String idPrefix;
        private final String mapping;

        Kind(String idPrefix, String mapping) {
            this.idPrefix = idPrefix;
            this.mapping = mapping;
        }
    }

    private UnmappedFieldsNestedFixture() {}

    /**
     * @param values the {@code subfield} values in the document's {@code _source}
     */
    record Doc(String id, Kind kind, List<String> values) {
        List<String> loaded() {
            return kind == Kind.NESTED ? List.of() : values;
        }

        Object expected() {
            List<String> loaded = loaded();
            if (loaded.isEmpty()) {
                return null;
            }
            return loaded.size() == 1 ? loaded.getFirst() : loaded;
        }

        private String source() {
            if (values.isEmpty()) {
                return Strings.format("{\"id\":\"%s\"}", id);
            }
            String objects = values.stream().map(v -> Strings.format("{\"subfield\":\"%s\"}", v)).collect(Collectors.joining(","));
            boolean array = kind == Kind.NESTED || values.size() > 1;
            return Strings.format("{\"id\":\"%s\",\"nested_punk\":%s}", id, array ? "[" + objects + "]" : objects);
        }
    }

    record Expectation(String query, List<String> columns, List<List<Object>> rows, boolean ordered) {
        void assertMatches(String schedule, EsqlQueryResponse response) {
            String reason = schedule + " " + query;
            assertFalse(reason + " is partial: " + response.getExecutionInfo(), response.isPartial());
            assertThat(reason, response.columns().stream().map(c -> c.name() + ":" + c.outputType()).toList(), equalTo(columns));
            List<List<Object>> actual = getValuesList(response);
            if (ordered) {
                assertThat(reason, actual, equalTo(rows));
            } else {
                assertThat(reason, actual, containsInAnyOrder(rows.toArray()));
            }
        }
    }

    static void createIndex(Client client, Kind kind, String index, String node) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, ESTestCase.between(1, 3))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.require._name", node);
        if (randomBoolean()) {
            settings.put("index.mapping.source.mode", "synthetic");
        }
        String copy = randomFrom("", "\n\"include_in_root\": true,", "\n\"include_in_parent\": true,");
        String mapping = kind == Kind.NESTED ? Strings.format(kind.mapping, copy) : kind.mapping;
        assertAcked(client.admin().indices().prepareCreate(index).setSettings(settings).setMapping(mapping));
    }

    static List<Doc> indexDocs(Client client, Kind kind, String index, int count) {
        List<Doc> docs = new ArrayList<>(count + 1);
        String idPrefix = kind.idPrefix + index + "_";
        for (int i = 0; i < count; i++) {
            String id = idPrefix + i;
            docs.add(new Doc(id, kind, i == 1 ? List.of(id + "a", "z_" + id) : List.of(id)));
        }
        if (kind != Kind.NESTED) {
            docs.add(new Doc(idPrefix + "none", kind, List.of()));
        }
        BulkRequestBuilder bulk = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (Doc doc : docs) {
            bulk.add(client.prepareIndex(index).setId(doc.id()).setSource(doc.source(), XContentType.JSON));
        }
        assertNoFailures(bulk.get());
        return docs;
    }

    static List<Expectation> loadExpectations(String from, List<Doc> docs) {
        String prefix = "SET unmapped_fields=\"load\"; " + from;
        List<Doc> sorted = docs.stream().sorted(Comparator.comparing(Doc::id)).toList();
        List<String> idAndField = List.of(ID_COLUMN, FIELD_COLUMN);
        return List.of(
            new Expectation(prefix + " | KEEP id, " + FIELD + " | SORT id", idAndField, idAndValue(sorted), true),
            new Expectation(prefix + " | KEEP id, " + FIELD, idAndField, idAndValue(sorted), false),
            sortedByField(prefix + " | KEEP id, " + FIELD + " | SORT " + FIELD + ", id", sorted),
            notNull(prefix, sorted, false),
            isNull(prefix, sorted, false),
            probed(prefix, sorted, false),
            countAll(prefix, sorted),
            count(prefix, sorted),
            countByValue(prefix, sorted),
            new Expectation(
                prefix + " | KEEP id, nested_punk | SORT id",
                List.of(ID_COLUMN, "nested_punk:keyword"),
                sorted.stream().map(d -> Arrays.<Object>asList(d.id(), null)).toList(),
                true
            )
        );
    }

    static List<Expectation> loadAllExpectations(String from, List<Doc> docs) {
        String prefix = "SET unmapped_fields=\"load_all\"; " + from;
        List<Doc> sorted = docs.stream().sorted(Comparator.comparing(Doc::id)).toList();
        Set<Kind> kinds = docs.stream().map(Doc::kind).collect(Collectors.toSet());
        // Nothing below a path that an index maps as nested is discovered, so only a mapping elsewhere still makes the field a column
        boolean fieldIsColumn = kinds.contains(Kind.OBJECT) || kinds.contains(Kind.NESTED) == false;
        List<String> columns = fieldIsColumn ? List.of(ID_COLUMN, FIELD_COLUMN) : List.of(ID_COLUMN);
        List<List<Object>> rows = fieldIsColumn ? idAndValue(sorted) : ids(sorted);
        return List.of(
            new Expectation(prefix + " | SORT id", columns, rows, true),
            new Expectation(prefix, columns, rows, false),
            new Expectation(prefix + " | KEEP id, nested_punk.* | SORT id", columns, rows, true),
            sortedByField(prefix + " | SORT " + FIELD + ", id", sorted),
            notNull(prefix, sorted, true),
            isNull(prefix, sorted, true),
            probed(prefix, sorted, true)
        );
    }

    static void assumeSupported(boolean loadAll) {
        ESTestCase.assumeTrue("requires SET unmapped_fields", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
        if (loadAll) {
            ESTestCase.assumeTrue("requires unmapped_fields=\"load_all\"", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());
        }
    }

    private static Expectation sortedByField(String query, List<Doc> docs) {
        Comparator<Doc> byField = Comparator.comparing(
            (Doc d) -> d.loaded().isEmpty() ? null : d.loaded().getFirst(),
            Comparator.nullsLast(Comparator.naturalOrder())
        );
        List<Doc> sorted = docs.stream().sorted(byField.thenComparing(Doc::id)).toList();
        return new Expectation(query, List.of(ID_COLUMN, FIELD_COLUMN), idAndValue(sorted), true);
    }

    private static Expectation notNull(String prefix, List<Doc> sorted, boolean keepField) {
        return filtered(prefix, FIELD + " IS NOT NULL", sorted.stream().filter(d -> d.loaded().isEmpty() == false).toList(), keepField);
    }

    private static Expectation isNull(String prefix, List<Doc> sorted, boolean keepField) {
        return filtered(prefix, FIELD + " IS NULL", sorted.stream().filter(d -> d.loaded().isEmpty()).toList(), keepField);
    }

    private static Expectation probed(String prefix, List<Doc> sorted, boolean keepField) {
        List<String> probes = new ArrayList<>();
        for (Kind kind : Kind.values()) {
            sorted.stream().filter(d -> d.kind() == kind).findFirst().ifPresent(d -> probes.add(d.values().getFirst()));
        }
        String probeList = probes.stream().map(p -> "\"" + p + "\"").collect(Collectors.joining(", "));
        List<Doc> matching = sorted.stream().filter(d -> d.loaded().size() == 1 && probes.contains(d.loaded().getFirst())).toList();
        return filtered(prefix, FIELD + " IN (" + probeList + ")", matching, keepField);
    }

    private static Expectation filtered(String prefix, String condition, List<Doc> matching, boolean keepField) {
        String keep = keepField ? "id, nested_punk.*" : "id";
        return new Expectation(
            prefix + " | WHERE " + condition + " | KEEP " + keep + " | SORT id",
            keepField ? List.of(ID_COLUMN, FIELD_COLUMN) : List.of(ID_COLUMN),
            keepField ? idAndValue(matching) : ids(matching),
            true
        );
    }

    /** Counts root documents only, whatever nested documents an index holds. */
    private static Expectation countAll(String prefix, List<Doc> docs) {
        return new Expectation(prefix + " | STATS c = COUNT(*)", List.of(COUNT_COLUMN), List.of(List.<Object>of((long) docs.size())), true);
    }

    private static Expectation count(String prefix, List<Doc> docs) {
        long values = docs.stream().mapToLong(d -> d.loaded().size()).sum();
        return new Expectation(prefix + " | STATS c = COUNT(" + FIELD + ")", List.of(COUNT_COLUMN), List.of(List.<Object>of(values)), true);
    }

    private static Expectation countByValue(String prefix, List<Doc> docs) {
        Map<String, Long> groups = new TreeMap<>();
        long withoutValue = 0;
        for (Doc doc : docs) {
            if (doc.loaded().isEmpty()) {
                withoutValue++;
            }
            for (String value : doc.loaded()) {
                groups.merge(value, 1L, Long::sum);
            }
        }
        List<List<Object>> rows = new ArrayList<>(groups.size() + 1);
        groups.forEach((value, count) -> rows.add(List.<Object>of(count, value)));
        if (withoutValue > 0) {
            rows.add(Arrays.<Object>asList(withoutValue, null));
        }
        return new Expectation(
            prefix + " | STATS c = COUNT(*) BY " + FIELD + " | SORT " + FIELD,
            List.of(COUNT_COLUMN, FIELD_COLUMN),
            rows,
            true
        );
    }

    private static List<List<Object>> idAndValue(List<Doc> sorted) {
        return sorted.stream().map(d -> Arrays.asList(d.id(), d.expected())).toList();
    }

    private static List<List<Object>> ids(List<Doc> docs) {
        return docs.stream().<List<Object>>map(d -> List.of(d.id())).toList();
    }

    /**
     * Handles {@code action} only once {@code after} completes, if given, and completes {@code done} once the response is sent. ES|QL
     * answers a data-node or cluster request only once its exchange sink drained, so chained nodes evaluate one after the other.
     */
    static <R extends TransportRequest> void intercept(
        MockTransportService transportService,
        String action,
        @Nullable SubscribableListener<Void> after,
        @Nullable SubscribableListener<Void> done,
        UnaryOperator<R> rewrite
    ) {
        ThreadPool threadPool = transportService.getThreadPool();
        transportService.<R>addRequestHandlingBehavior(action, (handler, request, channel, task) -> {
            TransportChannel answering = done == null ? channel : new SignallingChannel(channel, done);
            CheckedRunnable<Exception> handle = () -> handler.messageReceived(rewrite.apply(request), answering, task);
            if (after == null) {
                handleOrAnswer(handle, answering);
                return;
            }
            after.addTimeout(TimeValue.timeValueSeconds(20), threadPool, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            // The compute handlers assert they run on the search pool they are registered with.
            after.addListener(
                ActionListener.wrap(
                    ignored -> handleOrAnswer(handle, answering),
                    e -> answering.sendResponse(new IllegalStateException("not all nodes scheduled before this one answered " + action, e))
                ),
                threadPool.executor(ThreadPool.Names.SEARCH),
                threadPool.getThreadContext()
            );
        });
    }

    private static void handleOrAnswer(CheckedRunnable<Exception> handle, TransportChannel channel) {
        try {
            handle.run();
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }

    private record SignallingChannel(TransportChannel delegate, SubscribableListener<Void> done) implements TransportChannel {
        @Override
        public String getProfileName() {
            return delegate.getProfileName();
        }

        @Override
        public TransportVersion getVersion() {
            return delegate.getVersion();
        }

        @Override
        public void sendResponse(TransportResponse response) {
            try {
                delegate.sendResponse(response);
            } finally {
                done.onResponse(null);
            }
        }

        @Override
        public void sendResponse(Exception exception) {
            try {
                delegate.sendResponse(exception);
            } finally {
                done.onResponse(null);
            }
        }
    }
}
