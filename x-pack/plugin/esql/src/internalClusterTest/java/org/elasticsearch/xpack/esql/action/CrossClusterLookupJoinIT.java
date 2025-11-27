/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

// @TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public class CrossClusterLookupJoinIT extends AbstractCrossClusterTestCase {

    public void testLookupJoinAcrossClusters() throws IOException {
        setupClustersAndLookups();

        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        ) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));
            int vIndex = columns.indexOf("v");
            int lookupNameIndex = columns.indexOf("lookup_name");
            int tagIndex = columns.indexOf("tag");
            int lookupTagIndex = columns.indexOf("lookup_tag");

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(20));
            for (var row : values) {
                assertThat(row, hasSize(9));
                Long v = (Long) row.get(vIndex);
                assertThat(v, greaterThanOrEqualTo(0L));
                if (v < 25) {
                    assertThat((String) row.get(lookupNameIndex), equalTo("lookup_" + v));
                    String tag = (String) row.get(tagIndex);
                    if (tag.equals("local")) {
                        assertThat(row.get(lookupTagIndex), equalTo("local"));
                    } else {
                        assertThat(row.get(lookupTagIndex), equalTo(REMOTE_CLUSTER_1));
                    }
                } else {
                    assertNull(row.get(lookupNameIndex));
                    assertNull(row.get(lookupTagIndex));
                }
            }

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }

        populateLookupIndex(LOCAL_CLUSTER, "values_lookup2", 5);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup2", 5);
        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key "
                    + "| LOOKUP JOIN values_lookup2 ON lookup_key",
                randomBoolean()
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(20));
        }

        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key "
                    + "| STATS c = count(*) BY lookup_name | SORT c",
                randomBoolean()
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            // 0-9 + null + 16
            assertThat(values, hasSize(12));
            for (var row : values) {
                if (row.get(1) == null) {
                    assertThat((Long) row.get(0), equalTo(5L)); // null
                } else {
                    assertThat((String) row.get(1), containsString("lookup_"));
                    if (row.get(1).equals("lookup_0")
                        || row.get(1).equals("lookup_1")
                        || row.get(1).equals("lookup_4")
                        || row.get(1).equals("lookup_9")) {
                        // squares
                        assertThat((Long) row.get(0), equalTo(2L));
                    } else {
                        assertThat((Long) row.get(0), equalTo(1L));
                    }
                }
            }
        }
    }

    public void testLookupJoinWithAliases() throws IOException {
        setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup_local", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup_remote", 10);

        setupAlias(LOCAL_CLUSTER, "values_lookup_local", "values_lookup");
        setupAlias(REMOTE_CLUSTER_1, "values_lookup_remote", "values_lookup");

        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        ) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(20));
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testLookupJoinWithDatemath() throws IOException {
        setupClusters(2);

        ZonedDateTime nowUtc = ZonedDateTime.now(ZoneOffset.UTC);
        ZonedDateTime nextMidnight = nowUtc.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
        // If we're too close to midnight, we could create index with one day and query with another, and it'd fail.
        assumeTrue("Skip if too close to midnight", Duration.between(nowUtc, nextMidnight).toMinutes() >= 5);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);
        String lookupIndexName = "values_lookup_" + nowUtc.format(formatter);

        populateLookupIndex(LOCAL_CLUSTER, lookupIndexName, 10);
        populateLookupIndex(REMOTE_CLUSTER_1, lookupIndexName, 10);

        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN \"<values_lookup_{now/d}>\" ON lookup_key",
                randomBoolean()
            )
        ) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(20));
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testLookupJoinMissingRemoteIndex() throws IOException {
        setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);

        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        ) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(10));
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

            var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster.getFailures(), not(empty()));
            var failure = remoteCluster.getFailures().get(0);
            assertThat(failure.reason(), containsString("lookup index [values_lookup] is not available in remote cluster [cluster-a]"));
        }
        // Without local
        // FIXME: this is inconsistent due to how field-caps works - if there's no index at all, it fails, but if there's one but not
        // another, it succeeds. Ideally, this would be empty result with remote1 skipped, but field-caps fails.
        var ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:values_lookup]"));

        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        // then missing index is an error
        ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("lookup index [values_lookup] is not available in remote cluster [cluster-a]"));
    }

    public void testLookupJoinMissingRemoteIndexTwoRemotes() throws IOException {
        setupClusters(3);
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);

        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        // FIXME: inconsistent with the previous test, remote1:values_lookup still missing, but now it succeeds with remote1 skipped
        try (
            EsqlQueryResponse resp = runQuery(
                "FROM *:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(10));
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.getClusters().size(), equalTo(2));

            var remoteCluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster1.getFailures(), not(empty()));
            var failure = remoteCluster1.getFailures().get(0);
            assertThat(failure.reason(), containsString("lookup index [values_lookup] is not available in remote cluster [cluster-a]"));
            var remoteCluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
            assertThat(remoteCluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        }
    }

    public void testLookupJoinMissingLocalIndex() throws IOException {
        setupClusters(2);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);

        var ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("lookup index [values_lookup] is not available in local cluster"));

        // Without local in the query it's ok
        try (
            EsqlQueryResponse resp = runQuery(
                "FROM c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(10));
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag", "remote_tag"));
            assertThat(columns, not(hasItems("local_tag")));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.getClusters().size(), equalTo(1));

            var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        }
    }

    public void testLookupJoinMissingLocalIndexAfterPipelineBreaker() throws IOException {
        setupClusters(2);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);

        expectThrows(
            VerificationException.class,
            containsString("LOOKUP JOIN with remote indices can't be executed after [STATS lookup_key = max(lookup_key)]"),
            () -> runQuery(
                "FROM c*:logs-* | EVAL lookup_key = v | STATS lookup_key = max(lookup_key) | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        );
    }

    public void testLookupJoinMissingKey() throws IOException {
        setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);

        setSkipUnavailable(REMOTE_CLUSTER_1, true);

        Exception ex;

        try (
            // Using local_tag as key which is not present in remote index
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL local_tag = to_string(v) | LOOKUP JOIN values_lookup ON local_tag",
                randomBoolean()
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(20));
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.getClusters().size(), equalTo(2));

            var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            // FIXME: verify whether we need to skip or succeed here
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));

            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            var remoteTagIndex = columns.indexOf("remote_tag");
            var lookupNameIndex = columns.indexOf("lookup_name");
            var tagIndex = columns.indexOf("tag");
            assertThat(remoteTagIndex, greaterThanOrEqualTo(0));
            for (var row : values) {
                // remote tag column should be null
                assertNull(row.get(remoteTagIndex));
                if (row.get(tagIndex).equals("local")) {
                    // local value should be present
                    assertThat((String) row.get(lookupNameIndex), containsString("lookup_"));
                } else {
                    // remote value should be null
                    assertNull(row.get(lookupNameIndex));
                }
            }
        }

        try (
            // Using remote_tag as key which is not present in local index
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL remote_tag = to_string(v) | LOOKUP JOIN values_lookup ON remote_tag",
                randomBoolean()
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(20));
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.getClusters().size(), equalTo(2));

            var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));

            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            var localTagIndex = columns.indexOf("local_tag");
            var remoteTagIndex = columns.indexOf("remote_tag");
            var lookupNameIndex = columns.indexOf("lookup_name");
            var tagIndex = columns.indexOf("tag");
            assertThat(localTagIndex, greaterThanOrEqualTo(0));
            for (var row : values) {
                // remote tag column should be null
                assertNull(row.get(localTagIndex));
                if (row.get(tagIndex).equals("remote") && List.of("0", "1", "4", "9").contains((String) row.get(remoteTagIndex))) {
                    // remote value should be present
                    assertThat((String) row.get(lookupNameIndex), containsString("lookup_"));
                } else {
                    // local value should be null
                    assertNull(row.get(lookupNameIndex));
                }
            }
        }

        // TODO: verify whether this should be an error or not when the key field is missing
        ex = expectThrows(VerificationException.class, () -> runQuery("FROM c*:logs-* | LOOKUP JOIN values_lookup ON v", randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown column [v] in right side of join"));

        ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM c*:logs-* | EVAL local_tag = to_string(v) | LOOKUP JOIN values_lookup ON local_tag", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("Unknown column [local_tag] in right side of join"));

        // Add KEEP clause to try and trick the field-caps result parser into returning empty mapping
        ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-* | LOOKUP JOIN values_lookup ON v | KEEP v", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("Unknown column [v] in right side of join"));

        ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-*,c*:logs-* | LOOKUP JOIN values_lookup ON v | KEEP v", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("Unknown column [v] in right side of join"));

        ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM c*:logs-* | LOOKUP JOIN values_lookup ON v | KEEP v", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("Unknown column [v] in right side of join"));

        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        try (
            // Using local_tag as key which is not present in remote index
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL local_tag = to_string(v) | LOOKUP JOIN values_lookup ON local_tag",
                randomBoolean()
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(20));
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.getClusters().size(), equalTo(2));

            var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            // FIXME: verify whether we need to succeed or fail here
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        }

        // Add KEEP clause to try and trick the field-caps result parser into returning empty mapping
        ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM c*:logs-* | LOOKUP JOIN values_lookup ON v | KEEP v", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("Unknown column [v] in right side of join"));

        ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-*,c*:logs-* | LOOKUP JOIN values_lookup ON v | KEEP v", randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("Unknown column [v] in right side of join"));
    }

    public void testLookupJoinEmptyIndex() throws IOException {
        setupClusters(2);
        populateEmptyIndices(LOCAL_CLUSTER, "values_lookup");
        populateEmptyIndices(REMOTE_CLUSTER_1, "values_lookup");

        // Should work the same with both settings
        setSkipUnavailable(REMOTE_CLUSTER_1, randomBoolean());

        Exception ex;
        for (String index : List.of("values_lookup", "values_lookup_map_lookup")) {
            ex = expectThrows(
                VerificationException.class,
                () -> runQuery("FROM logs-* | LOOKUP JOIN " + index + " ON v | KEEP v", randomBoolean())
            );
            assertThat(ex.getMessage(), containsString("Unknown column [v] in right side of join"));
            ex = expectThrows(
                VerificationException.class,
                () -> runQuery("FROM c*:logs-* | LOOKUP JOIN " + index + " ON v | KEEP v", randomBoolean())
            );
            assertThat(ex.getMessage(), containsString("Unknown column [v] in right side of join"));
        }

        ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-* | LOOKUP JOIN values_lookup_map ON v | KEEP v", randomBoolean())
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "Lookup Join requires a single lookup mode index; "
                    + "[values_lookup_map] resolves to [values_lookup_map] in [standard] mode"
            )
        );
    }

    public void testLookupJoinIndexMode() throws IOException {
        setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateIndex(REMOTE_CLUSTER_1, "values_lookup", randomIntBetween(1, 3), 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key",
                randomBoolean()
            )
        ) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasItems("lookup_key", "lookup_name", "lookup_tag", "v", "tag"));

            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(10));
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();

            var localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));

            var remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster.getFailures(), not(empty()));
            var failure = remoteCluster.getFailures().get(0);
            assertThat(
                failure.reason(),
                containsString(
                    "Lookup Join requires a single lookup mode index; "
                        + "[values_lookup] resolves to [cluster-a:values_lookup] in [standard] mode"
                )
            );
        }

        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        // then missing index is an error
        var ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "Lookup Join requires a single lookup mode index; [values_lookup] resolves to [cluster-a:values_lookup] in [standard] mode"
            )
        );
    }

    public void testLookupJoinFieldTypes() throws IOException {
        setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10, "keyword");

        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        var ex = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-*,c*:logs-* | EVAL lookup_key = v | LOOKUP JOIN values_lookup ON lookup_key", randomBoolean())
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "Cannot use field [lookup_key] due to ambiguities being mapped as [2] incompatible types:"
                    + " [keyword] in [cluster-a:values_lookup], [long] in [values_lookup]"
            )
        );

        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,c*:logs-* | EVAL lookup_name = v::keyword | LOOKUP JOIN values_lookup ON lookup_name",
                randomBoolean()
            )
        ) {
            var columns = resp.columns().stream().map(ColumnInfoImpl::name).toList();
            assertThat(columns, hasSize(9));
            var lookupKeyIndex = columns.indexOf("lookup_key");
            assertThat(lookupKeyIndex, greaterThanOrEqualTo(0));
            var keyColumn = resp.columns().get(lookupKeyIndex);
            assertThat(keyColumn.type(), equalTo(DataType.UNSUPPORTED));
            assertThat(keyColumn.originalTypes(), hasItems("keyword", "long"));
            List<List<Object>> values = getValuesList(resp);
            for (var row : values) {
                assertNull(row.get(lookupKeyIndex));
            }
        }
    }

    public void testAlwaysAppliesTheFilter() throws IOException {
        setupClusters(3);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        var defaultSettings = Settings.builder();
        createIndexWithDocument(LOCAL_CLUSTER, "data", defaultSettings, Map.of("key", 1, "f1", 1));
        createIndexWithDocument(REMOTE_CLUSTER_1, "data", defaultSettings, Map.of("key", 2, "f2", 2));
        createIndexWithDocument(REMOTE_CLUSTER_2, "data", defaultSettings, Map.of("key", 3, "f3", 3));

        try (var r = runQuery(syncEsqlQueryRequest("FROM data,*:data | WHERE f1 == 1").filter(new TermQueryBuilder("f2", 2)))) {
            assertThat(getValuesList(r), hasSize(0));
        }
    }

    public void testLookupJoinRetryAnalysis() throws IOException {
        setupClusters(3);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        var defaultSettings = Settings.builder();
        createIndexWithDocument(LOCAL_CLUSTER, "data", defaultSettings, Map.of("key", 1, "f1", 1));
        createIndexWithDocument(REMOTE_CLUSTER_1, "data", defaultSettings, Map.of("key", 2, "f2", 2));
        createIndexWithDocument(REMOTE_CLUSTER_2, "data", defaultSettings, Map.of("key", 3, "f3", 3));

        var lookupSettings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP);
        createIndexWithDocument(LOCAL_CLUSTER, "lookup", lookupSettings, Map.of("key", 1, "location", "local"));
        createIndexWithDocument(REMOTE_CLUSTER_1, "lookup", lookupSettings, Map.of("key", 2, "location", "remote-1"));
        // lookup is intentionally absent on REMOTE_CLUSTER_2

        // The following query uses filter f2=2 that narrows down execution only to REMOTE_CLUSTER_1 index however,
        // later it uses `WHERE f1 == 1` esql condition that to an attribute present only on the local cluster index.
        // This causes analysis to fail and retry the entire query without a filter.
        // The second analysis executes against all cluster indices and should discover that lookup is absent on REMOTE_CLUSTER_2.
        expectThrows(
            VerificationException.class,
            containsString("lookup index [lookup] is not available in remote cluster [remote-b]"),
            () -> runQuery(
                syncEsqlQueryRequest("FROM data,*:data | LOOKUP JOIN lookup ON key | WHERE f1 == 1").filter(new TermQueryBuilder("f2", 2))
            )
        );
    }

    private void createIndexWithDocument(String clusterAlias, String indexName, Settings.Builder settings, Map<String, Object> source) {
        var client = client(clusterAlias);
        client.admin().indices().prepareCreate(indexName).setSettings(settings).get();
        client.prepareIndex(indexName).setSource(source).get();
        client.admin().indices().prepareRefresh(indexName).get();
    }

    protected Map<String, Object> setupClustersAndLookups() throws IOException {
        var setupData = setupClusters(2);
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 25);
        return setupData;
    }

    public void setupHostsEnrich() {
        // the hosts policy are identical on every node
        Map<String, String> allHosts = Map.of("192.168.1.2", "Windows");
        Client client = client(LOCAL_CLUSTER);
        client.admin().indices().prepareCreate("hosts").setMapping("ip", "type=ip", "os", "type=keyword").get();
        for (Map.Entry<String, String> h : allHosts.entrySet()) {
            client.prepareIndex("hosts").setSource("ip", h.getKey(), "os", h.getValue()).get();
        }
        client.admin().indices().prepareRefresh("hosts").get();
        EnrichPolicy hostPolicy = new EnrichPolicy("match", null, List.of("hosts"), "ip", List.of("ip", "os"));
        client.execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "hosts", hostPolicy))
            .actionGet();
        client.execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "hosts"))
            .actionGet();
        assertAcked(client.admin().indices().prepareDelete("hosts"));
    }

    private static void assertCCSExecutionInfoDetails(EsqlExecutionInfo executionInfo) {
        assertNotNull(executionInfo);
        assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        assertTrue(executionInfo.isCrossClusterSearch());
        List<EsqlExecutionInfo.Cluster> clusters = executionInfo.clusterAliases().stream().map(executionInfo::getCluster).toList();

        for (EsqlExecutionInfo.Cluster cluster : clusters) {
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(0));
        }
    }

    protected void setupAlias(String clusterAlias, String indexName, String aliasName) {
        Client client = client(clusterAlias);
        IndicesAliasesRequestBuilder indicesAliasesRequestBuilder = client.admin()
            .indices()
            .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName).alias(aliasName));
        assertAcked(client.admin().indices().aliases(indicesAliasesRequestBuilder.request()));
    }

    protected void populateEmptyIndices(String clusterAlias, String indexName) {
        Client client = client(clusterAlias);
        // Empty body
        assertAcked(client.admin().indices().prepareCreate(indexName));
        client.admin().indices().prepareRefresh(indexName).get();
        // mappings + settings
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName + "_map_lookup")
                .setMapping()
                .setSettings(Settings.builder().put("index.mode", "lookup"))
        );
        client.admin().indices().prepareRefresh(indexName + "_map_lookup").get();
        // mappings only
        assertAcked(client.admin().indices().prepareCreate(indexName + "_map").setMapping());
        client.admin().indices().prepareRefresh(indexName + "_map").get();
    }

}
