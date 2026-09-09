/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.AbstractFromDatasetSubqueryRestTestCase;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Pins the dropped-null-element notice of a Parquet {@code LIST} read to the response, on a multi-node cluster, for
 * every external distribution mode.
 *
 * <p>An ES|QL multivalue cannot hold null, so a list element that is null is dropped on read and the reader announces
 * the loss with one {@code Warning} per affected column (elastic/esql-planning#1799). The notice must travel through
 * the driver's warning channel: a scan node's own response headers do not reach the client when the scan was shipped
 * away from the coordinator. The existing coverage — {@code ListColumnParityTests} at the decoder and the parquet
 * csv-spec suites on a single-node cluster — exercises neither placement, because on one node the coordinator
 * <em>is</em> the data node.
 *
 * <p>So this suite runs the identical read under {@code coordinator_only}, {@code round_robin}, and {@code adaptive}
 * from every coordinator, verifies from the profiles that at least one round-robin assignment differs from its
 * coordinator-only counterpart, and requires the notice in every response. A mode that returns the right values with
 * no notice is the silent-loss regression the notice exists to prevent.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetNullListElementWarningIT extends AbstractFromDatasetSubqueryRestTestCase {

    private static final String DATA_SOURCE = "null_list_elem_s3_ds";
    private static final String DATASET = "null_list_elem_s3";
    private static final String BLOB_KEY = WAREHOUSE + "/standalone/null_list_elements.parquet";

    /** Every external distribution mode {@code QueryPragmas#EXTERNAL_DISTRIBUTION} accepts. */
    private static final List<String> DISTRIBUTION_MODES = List.of("coordinator_only", "round_robin", "adaptive");

    private static final String SUMMARY_WARNING =
        "Parquet lists with null elements were read with those elements omitted; an ES|QL multivalued field cannot hold null";
    private static final String COLUMN_WARNING = "Parquet list column [ints] contains lists with null elements; "
        + "the column returns fewer values than the file holds";

    /** The benign notice ES|QL adds for a query without an explicit {@code LIMIT}; not the subject here. */
    private static final String DEFAULT_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    /**
     * Two nodes so a distributed assignment has somewhere to go: with one node every strategy resolves to the same
     * JVM and the placement axis this suite is about disappears.
     */
    public static ElasticsearchCluster cluster = Clusters.multiNodeTestClusterWithEncryption(() -> s3Fixture.getAddress(), 2);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @AfterClass
    public static void cleanupRegistry() throws IOException {
        deleteIgnoringMissing("/_query/dataset/" + DATASET);
        deleteIgnoringMissing("/_query/data_source/" + DATA_SOURCE);
    }

    /**
     * {@code ints} holds {@code [1,2,3]}, {@code [null,1]}, {@code [4]}: six elements, five of them non-null. The
     * value assertions pin the drop semantics (the read returns five) and the warning assertion pins that the drop is
     * announced.
     */
    public void testDroppedNullElementIsAnnouncedInEveryDistributionMode() throws Exception {
        BackendFixture s3Backend = new S3BackendFixture(s3Fixture);
        s3Backend.uploadBlob(BLOB_KEY, nullListElementParquetBytes());
        putDataSource(DATA_SOURCE, s3Backend.dataSourceType(), s3Backend.dataSourceSettings());
        putDataset(DATASET, DATA_SOURCE, s3Backend.resourceUri(BLOB_KEY), Map.of());

        String query = "FROM " + DATASET + " | STATS rows = COUNT(*), elements = SUM(MV_COUNT(ints))";

        // Every combination is run before anything is asserted, so a failure report names all of them rather than
        // stopping at the first. Each mode is issued once per node: under a distributing mode the one split lands on a
        // fixed node, so sweeping the coordinator across the cluster gives the assignment a chance to differ. The
        // profile assertion below proves that it actually did rather than assuming placement from the pragma alone.
        Map<String, QueryOutcome> outcomes = new LinkedHashMap<>();
        Map<String, RestClient> coordinators = perNodeClients();
        for (String mode : DISTRIBUTION_MODES) {
            for (Map.Entry<String, RestClient> coordinator : coordinators.entrySet()) {
                outcomes.put(mode + " @ " + coordinator.getKey(), runQueryWithMode(coordinator.getValue(), query, mode));
            }
        }
        outcomes.forEach((label, outcome) -> {
            assertThat("[" + label + "] rows", outcome.longValue("rows"), equalTo(3L));
            assertThat("[" + label + "] elements (the null element is dropped)", outcome.longValue("elements"), equalTo(5L));
        });
        Map<String, String> report = new LinkedHashMap<>();
        outcomes.forEach((label, outcome) -> report.put(label, "scan on " + outcome.scanNodes() + " warnings " + outcome.warnings()));
        outcomes.forEach(
            (label, outcome) -> assertFalse(
                "[" + label + "] profile names no external scan node; per run: " + report,
                outcome.scanNodes().isEmpty()
            )
        );
        boolean observedOffCoordinatorAssignment = coordinators.keySet()
            .stream()
            .anyMatch(
                coordinator -> outcomes.get("coordinator_only @ " + coordinator)
                    .scanNodes()
                    .equals(outcomes.get("round_robin @ " + coordinator).scanNodes()) == false
            );
        assertTrue(
            "round_robin never changed the external scan node relative to coordinator_only for the same coordinator; per run: " + report,
            observedOffCoordinatorAssignment
        );
        outcomes.forEach((label, outcome) -> {
            assertThat("[" + label + "] summary notice; per run: " + report, outcome.warnings(), hasItem(SUMMARY_WARNING));
            assertThat("[" + label + "] per-column notice; per run: " + report, outcome.warnings(), hasItem(COLUMN_WARNING));
        });
    }

    /**
     * One client per cluster node, each pinned to that node so it is the coordinator for every request it carries.
     * The shared {@code client()} spreads requests over all nodes, which would leave the placement being tested to
     * chance. Built once per suite and closed in {@link #closePerNodeClients()}.
     */
    private Map<String, RestClient> perNodeClients() throws IOException {
        if (perNodeClients == null) {
            Map<String, RestClient> clients = new LinkedHashMap<>();
            for (String address : cluster.getHttpAddresses().split(",")) {
                HttpHost host = HttpHost.create(address.startsWith("http") ? address : "http://" + address);
                clients.put(address, buildClient(restClientSettings(), new HttpHost[] { host }));
            }
            perNodeClients = clients;
        }
        return perNodeClients;
    }

    private Map<String, RestClient> perNodeClients;

    @After
    public void closePerNodeClients() throws IOException {
        if (perNodeClients != null) {
            IOUtils.close(perNodeClients.values());
            perNodeClients = null;
        }
    }

    /** A single {@code _query} result: the response body plus the {@code Warning} headers it carried. */
    private record QueryOutcome(Map<String, Object> body, List<String> warnings) {
        @SuppressWarnings("unchecked")
        long longValue(String columnName) {
            List<Map<String, Object>> columns = (List<Map<String, Object>>) body.get("columns");
            for (int i = 0; i < columns.size(); i++) {
                if (columnName.equals(columns.get(i).get("name"))) {
                    return ((Number) ((List<List<Object>>) body.get("values")).get(0).get(i)).longValue();
                }
            }
            throw new AssertionError("column [" + columnName + "] not in response " + body);
        }

        /**
         * The names of the nodes that actually ran the external scan, read off the request profile. Reported alongside
         * the warnings so a failure says where the read happened rather than only that the notice went missing.
         */
        @SuppressWarnings("unchecked")
        Set<String> scanNodes() {
            Map<String, Object> profile = (Map<String, Object>) body.get("profile");
            if (profile == null) {
                return Set.of();
            }
            Set<String> nodes = new LinkedHashSet<>();
            for (Map<String, Object> driver : (List<Map<String, Object>>) profile.get("drivers")) {
                for (Map<String, Object> operator : (List<Map<String, Object>>) driver.get("operators")) {
                    if (String.valueOf(operator.get("operator")).contains("ExternalDataSourceOperator")) {
                        nodes.add(String.valueOf(driver.get("node_name")));
                        break;
                    }
                }
            }
            return nodes;
        }
    }

    /**
     * Runs {@code query} with {@code external_distribution} pinned to {@code mode}, returning the body together with
     * the engine warnings minus the ambient no-limit notice. Warnings are collected rather than asserted by the
     * client, because their presence is what the test measures.
     */
    private static QueryOutcome runQueryWithMode(RestClient coordinator, String query, String mode) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", query).field("profile", true).field("accept_pragma_risks", true);
            b.startObject("pragma").field("external_distribution", mode).endObject();
            b.endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        req.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response r = coordinator.performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        List<String> warnings = new ArrayList<>(r.getWarnings());
        warnings.remove(DEFAULT_LIMIT_WARNING);
        return new QueryOutcome(entityAsMap(r), warnings);
    }

    /**
     * A three-row, one-row-group file with a single 3-level {@code LIST} column of optional {@code int64} elements.
     * The middle row's leading element is null: {@link Group#addGroup} with nothing appended writes a present list
     * entry whose value is absent, which is exactly the definition level the reader recognises as a droppable
     * element (as opposed to a null or empty list).
     */
    private static byte[] nullListElementParquetBytes() throws IOException {
        MessageType schema = new MessageType(
            "null_list_elements",
            Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT64).named("ints")
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(byteArrayOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            writer.write(listRow(factory, 1L, 2L, 3L));
            writer.write(listRow(factory, null, 1L));
            writer.write(listRow(factory, 4L));
        }
        return baos.toByteArray();
    }

    private static Group listRow(SimpleGroupFactory factory, Long... elements) {
        Group row = factory.newGroup();
        Group list = row.addGroup("ints");
        for (Long element : elements) {
            Group entry = list.addGroup("list");
            if (element != null) {
                entry.append("element", element);
            }
        }
        return row;
    }

    private static OutputFile byteArrayOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionOutputStream(baos);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    private static PositionOutputStream positionOutputStream(ByteArrayOutputStream baos) {
        return new PositionOutputStream() {
            private long position = 0;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) {
                baos.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                baos.write(b, off, len);
                position += len;
            }
        };
    }
}
