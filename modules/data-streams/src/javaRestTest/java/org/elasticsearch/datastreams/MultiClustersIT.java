/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.http.HttpHost;
import org.apache.lucene.tests.util.English;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.datastreams.AbstractDataStreamIT.createDataStream;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.LOGS_TEMPLATE;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.STANDARD_TEMPLATE;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.indexDocument;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.putTemplate;
import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MultiClustersIT extends ESRestTestCase {
    static List<Document> localLogsDocs = null;
    static List<Document> remoteLogsDocs = null;
    static List<Document> localStandardDocs = null;
    static List<Document> remoteStandardDocs = null;

    public static ElasticsearchCluster remoteCluster = ElasticsearchCluster.local()
        .name("remote_cluster")
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    public static ElasticsearchCluster localCluster = ElasticsearchCluster.local()
        .name("local_cluster")
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
        .setting("cluster.remote.remote_cluster.seeds", () -> "\"" + remoteCluster.getTransportEndpoint(0) + "\"")
        .setting("cluster.remote.connections_per_cluster", "1")
        .setting("cluster.remote.remote_cluster.skip_unavailable", "false")
        .build();

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private RestClient localClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(localCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    private record Document(long timestamp, String cluster, String hostName, long pid, String method, long messageId, String message) {

        @SuppressWarnings("unchecked")
        static Document fromHit(Map<String, Object> hit) {
            long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis(hit.get("@timestamp").toString());
            String cluster = (String) hit.get("cluster");
            String hostName = (String) hit.get("host.name");
            if (hostName == null) {
                Map<String, Object> host = (Map<String, Object>) hit.get("host");
                hostName = (String) host.get("name");
            }
            long pid = ((Number) hit.get("pid")).longValue();
            String method = (String) hit.get("method");
            long messageId = ((Number) hit.get("message_id")).longValue();
            String message = (String) hit.get("message");
            return new Document(timestamp, cluster, hostName, pid, method, messageId, message);
        }

        String toJson() throws IOException {
            XContentBuilder builder = JsonXContent.contentBuilder()
                .startObject()
                .field("@timestamp", timestamp)
                .field("cluster", cluster)
                .field("host.name", hostName)
                .field("pid", pid)
                .field("method", method)
                .field("message_id", messageId)
                .field("message", message)
                .endObject();
            return Strings.toString(builder);
        }
    }

    static String randomHostName() {
        return randomFrom("qa-", "staging-", "prod-") + between(1, 3);
    }

    static List<Document> indexDocuments(RestClient client, String cluster, String index, int startMessageId) throws IOException {
        int numDocs = between(0, 100);
        List<Document> docs = new ArrayList<>(numDocs);
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-09-15T00:00:00Z");
        for (int i = 0; i < numDocs; i++) {
            timestamp += between(0, 5) * 1000L;
            long pid = randomLongBetween(1, 10);
            String method = randomFrom("GET", "PUT", "POST", "DELETE");
            String message = English.intToEnglish(between(1, 1000000));
            docs.add(new Document(timestamp, cluster, randomHostName(), pid, method, startMessageId + i, message));
        }
        Randomness.shuffle(docs);
        for (Document doc : docs) {
            indexDocument(client, index, doc.toJson());
            if (rarely()) {
                refresh(client, index);
            }
        }
        refresh(client, index);
        return docs;
    }

    @Before
    public void setUpIndices() throws Exception {
        if (localLogsDocs != null) {
            return;
        }
        try (RestClient client = localClusterClient()) {
            putTemplate(client, "logs-template", LOGS_TEMPLATE);
            putTemplate(client, "standard-template", STANDARD_TEMPLATE);

            createDataStream(client, "logs-apache-kafka");
            localLogsDocs = indexDocuments(client, "local", "logs-apache-kafka", 0);
            assertDocCount(client, "logs-apache-kafka", localLogsDocs.size());

            createDataStream(client, "standard-apache-kafka");
            localStandardDocs = indexDocuments(client, "local", "standard-apache-kafka", 1000);
            assertDocCount(client, "standard-apache-kafka", localStandardDocs.size());
        }
        try (RestClient client = remoteClusterClient()) {
            putTemplate(client, "logs-template", LOGS_TEMPLATE);
            putTemplate(client, "standard-template", STANDARD_TEMPLATE);

            createDataStream(client, "logs-apache-kafka");
            remoteLogsDocs = indexDocuments(client, "remote", "logs-apache-kafka", 2000);
            assertDocCount(client, "logs-apache-kafka", remoteLogsDocs.size());

            createDataStream(client, "standard-apache-kafka");
            remoteStandardDocs = indexDocuments(client, "remote", "standard-apache-kafka", 3000);
            assertDocCount(client, "standard-apache-kafka", remoteStandardDocs.size());
        }
    }

    public void testSource() throws IOException {
        XContentBuilder searchSource = JsonXContent.contentBuilder().startObject().field("_source", true).field("size", 500);
        final boolean sorted = randomBoolean();
        if (sorted) {
            searchSource.startArray("sort");
            searchSource.value("message_id");
            searchSource.endArray();
        }
        final Predicate<String> filterHost;
        if (randomBoolean()) {
            String host = randomHostName();
            filterHost = s -> s.equals(host);
            searchSource.startObject("query");
            searchSource.startObject("term");
            searchSource.startObject("host.name");
            searchSource.field("value", host);
            searchSource.endObject();
            searchSource.endObject();
            searchSource.endObject();
        } else {
            filterHost = s -> true;
        }
        searchSource.endObject();
        // remote only
        {
            var request = new Request("POST", "/*:l*,*:s*/_search");
            request.setJsonEntity(Strings.toString(searchSource));
            if (randomBoolean()) {
                request.addParameter("ccs_minimize_roundtrips", Boolean.toString(randomBoolean()));
            }
            Response resp = client().performRequest(request);
            assertOK(resp);
            Stream<Document> hits = extractHits(resp).stream().map(Document::fromHit);
            if (sorted == false) {
                hits = hits.sorted(Comparator.comparingLong(Document::messageId));
            }
            var expectedHits = Stream.of(remoteLogsDocs, remoteStandardDocs)
                .flatMap(Collection::stream)
                .filter(d -> filterHost.test(d.hostName))
                .sorted(Comparator.comparingLong(Document::messageId))
                .toList();
            assertThat(hits.toList(), equalTo(expectedHits));
        }
        // both clusters
        {
            var request = new Request("POST", "/*,*:*/_search");
            request.setJsonEntity(Strings.toString(searchSource));
            if (randomBoolean()) {
                request.addParameter("ccs_minimize_roundtrips", Boolean.toString(randomBoolean()));
            }
            Response resp = client().performRequest(request);
            assertOK(resp);
            Stream<Document> hits = extractHits(resp).stream().map(Document::fromHit);
            if (sorted == false) {
                hits = hits.sorted(Comparator.comparingLong(Document::messageId));
            }
            var expectedHits = Stream.of(localLogsDocs, localStandardDocs, remoteLogsDocs, remoteStandardDocs)
                .flatMap(Collection::stream)
                .filter(d -> filterHost.test(d.hostName))
                .sorted(Comparator.comparingLong(Document::messageId))
                .toList();
            assertThat(hits.toList(), equalTo(expectedHits));
        }

    }

    public void testFields() throws IOException {
        XContentBuilder searchSource = JsonXContent.contentBuilder()
            .startObject()
            .array("fields", "message_id", "host.name")
            .field("size", 500);
        final boolean sorted = randomBoolean();
        if (sorted) {
            searchSource.startArray("sort");
            searchSource.value("message_id");
            searchSource.endArray();
        }
        final Predicate<String> filterHost;
        if (randomBoolean()) {
            String host = randomHostName();
            filterHost = s -> s.equals(host);
            searchSource.startObject("query");
            searchSource.startObject("term");
            searchSource.startObject("host.name");
            searchSource.field("value", host);
            searchSource.endObject();
            searchSource.endObject();
            searchSource.endObject();
        } else {
            filterHost = s -> true;
        }
        searchSource.endObject();
        record Fields(long messageId, String hostName) {
            @SuppressWarnings("unchecked")
            static Fields fromResponse(Map<String, Object> hit) {
                List<String> hostName = (List<String>) hit.get("host.name");
                assertThat(hostName, hasSize(1));
                List<Number> messageId = (List<Number>) hit.get("message_id");
                assertThat(messageId, hasSize(1));
                return new Fields(messageId.getFirst().longValue(), hostName.getFirst());
            }
        }
        // remote only
        {
            var request = new Request("POST", "/*:l*,*:s*/_search");
            request.setJsonEntity(Strings.toString(searchSource));
            if (randomBoolean()) {
                request.addParameter("ccs_minimize_roundtrips", Boolean.toString(randomBoolean()));
            }
            Response resp = client().performRequest(request);
            assertOK(resp);
            Stream<Fields> hits = extractFields(resp).stream().map(Fields::fromResponse);
            if (sorted == false) {
                hits = hits.sorted(Comparator.comparingLong(Fields::messageId));
            }
            var expectedHits = Stream.of(remoteLogsDocs, remoteStandardDocs)
                .flatMap(Collection::stream)
                .filter(d -> filterHost.test(d.hostName))
                .map(d -> new Fields(d.messageId, d.hostName))
                .sorted(Comparator.comparingLong(Fields::messageId))
                .toList();
            assertThat(hits.toList(), equalTo(expectedHits));
        }
        // both clusters
        {
            var request = new Request("POST", "/*,*:*/_search");
            request.setJsonEntity(Strings.toString(searchSource));
            if (randomBoolean()) {
                request.addParameter("ccs_minimize_roundtrips", Boolean.toString(randomBoolean()));
            }
            Response resp = client().performRequest(request);
            assertOK(resp);
            Stream<Fields> hits = extractFields(resp).stream().map(Fields::fromResponse);
            if (sorted == false) {
                hits = hits.sorted(Comparator.comparingLong(Fields::messageId));
            }
            var expectedHits = Stream.of(localLogsDocs, localStandardDocs, remoteLogsDocs, remoteStandardDocs)
                .flatMap(Collection::stream)
                .filter(d -> filterHost.test(d.hostName))
                .map(d -> new Fields(d.messageId, d.hostName))
                .sorted(Comparator.comparingLong(Fields::messageId))
                .toList();
            assertThat(hits.toList(), equalTo(expectedHits));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> extractHits(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> hitsMap = (Map<String, Object>) map.get("hits");
        final List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        return hitsList.stream().map(hit -> (Map<String, Object>) hit.get("_source")).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> extractFields(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> hitsMap = (Map<String, Object>) map.get("hits");
        final List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        return hitsList.stream().map(hit -> (Map<String, Object>) hit.get("fields")).toList();
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }
}
