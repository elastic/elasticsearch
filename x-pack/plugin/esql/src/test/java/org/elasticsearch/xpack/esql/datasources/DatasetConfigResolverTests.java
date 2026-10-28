/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * A plan reaching a node names its dataset; the node must read under the definition it has registered for that name,
 * not under whatever config and path the plan carried. These tests send plans whose config selects attacker-chosen
 * credentials and check that {@link DatasetConfigResolver}, and {@link PlannerUtils#localPlan} which applies it for
 * both plan shapes a data node receives, replaces them with the registered ones or refuses the plan.
 */
public class DatasetConfigResolverTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final String DATASET = "logs";
    private static final String PARENT = "s3_parent";
    private static final String RESOURCE = "s3://bucket/data/*.parquet";

    /** The config an attacker ships: federated identity under a role of their choosing. */
    private static final Map<String, Object> HOSTILE_CONFIG = Map.of(
        "auth",
        "federated_identity",
        "role_arn",
        "arn:aws:iam::123456789012:role/attacker",
        "endpoint",
        "https://attacker.example"
    );

    public void testRebindReplacesPlanConfigWithRegisteredDefinition() {
        ProjectMetadata project = project(
            dataSource(Map.of("auth", "anonymous", "region", "eu-west-1")),
            dataset(Map.of("format", "parquet"))
        );
        ExternalSourceExec exec = exec(HOSTILE_CONFIG, DATASET).withSplits(splits("s3://bucket/data/a.parquet"));

        ExternalSourceExec rebound = new DatasetConfigResolver(project, false).rebind(exec);

        assertRegisteredConfig(rebound, project);
    }

    /** The coordinator ships this shape when it distributes discovered splits: no fragment, the exec sits under the sink. */
    public void testLocalPlanRebindsBareExec() {
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset(Map.of("format", "parquet")));
        ExternalSourceExec exec = exec(HOSTILE_CONFIG, DATASET).withSplits(splits("s3://bucket/data/a.parquet"));
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, exec.output(), false, exec);

        PhysicalPlan planned = localPlan(sink, List.of(), new DatasetConfigResolver(project, false));

        ExternalSourceExec rebound = findExec(planned);
        assertRegisteredConfig(rebound, project);
        assertThat(rebound.splits(), equalTo(exec.splits()));
    }

    /** The other shape: the logical relation inside a fragment, expanded by the local mapper on this node. */
    public void testLocalPlanRebindsFragmentRelation() {
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset(Map.of("format", "parquet")));
        ExternalRelation relation = relation(HOSTILE_CONFIG, DATASET);
        ExchangeSinkExec sink = new ExchangeSinkExec(SRC, relation.output(), false, new FragmentExec(relation));
        List<ExternalSplit> nodeSplits = splits("s3://bucket/data/b.parquet");

        PhysicalPlan planned = localPlan(sink, nodeSplits, new DatasetConfigResolver(project, false));

        ExternalSourceExec rebound = findExec(planned);
        assertRegisteredConfig(rebound, project);
        assertThat(rebound.splits(), equalTo(nodeSplits));
    }

    public void testRebindIsIdempotent() {
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset(Map.of("format", "parquet")));
        DatasetConfigResolver resolver = new DatasetConfigResolver(project, false);

        ExternalSourceExec once = resolver.rebind(exec(HOSTILE_CONFIG, DATASET));
        ExternalSourceExec twice = resolver.rebind(once);

        assertThat(twice, equalTo(once));
    }

    /** No dataset name means no definition to read under; on a release build that is refused, whatever the config. */
    public void testNullDatasetNameRefusedOnReleaseBuild() {
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset(Map.of()));
        ExternalSourceExec exec = exec(randomBoolean() ? Map.of() : HOSTILE_CONFIG, null);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new DatasetConfigResolver(project, false).rebind(exec)
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("external dataset [] is not registered on this node"));
    }

    /** The inline EXTERNAL command exists on snapshot builds alone and plans a source with no dataset behind it. */
    public void testNullDatasetNamePassesUnchangedOnSnapshotBuild() {
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset(Map.of()));
        ExternalSourceExec exec = exec(Map.of("format", "parquet"), null);

        assertThat(new DatasetConfigResolver(project, true).rebind(exec), equalTo(exec));
    }

    public void testUnregisteredDatasetRefused() {
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset(Map.of()));
        ExternalSourceExec exec = exec(HOSTILE_CONFIG, "other");

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new DatasetConfigResolver(project, randomBoolean()).rebind(exec)
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("external dataset [other] is not registered on this node"));
    }

    /** A dataset whose data source is gone gets the same refusal, so the error does not say which half is missing. */
    public void testDatasetWithoutParentRefused() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of()))
            .datasets(Map.of(DATASET, dataset(Map.of())))
            .build();
        ExternalSourceExec exec = exec(HOSTILE_CONFIG, DATASET);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new DatasetConfigResolver(project, randomBoolean()).rebind(exec)
        );
        assertThat(e.getMessage(), equalTo("external dataset [logs] is not registered on this node"));
    }

    /** The plan's definition version is what discovery ran under; a node holding another definition must not read. */
    public void testDefinitionVersionMismatchRefused() {
        DataSource parent = dataSource(Map.of("auth", "anonymous"));
        Dataset dataset = dataset(Map.of("format", "parquet"));
        ProjectMetadata planned = project(parent, dataset);
        Map<String, Object> planConfig = DatasetRewriter.datasetConfig(parent, dataset);

        // Same version on both sides: rebound.
        ExternalSourceExec exec = exec(planConfig, DATASET);
        assertRegisteredConfig(new DatasetConfigResolver(planned, false).rebind(exec), planned);

        // The node's copy of the dataset was edited since: refused with a distinct message.
        ProjectMetadata edited = project(parent, dataset(Map.of("format", "csv")));
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new DatasetConfigResolver(edited, false).rebind(exec)
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("external dataset [logs] definition changed while the query was planned"));
    }

    /** Splits are plan-carried too; with the registered credentials in hand they may only name the dataset's own files. */
    public void testSplitOutsideResourceRefused() {
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset(Map.of()));
        DatasetConfigResolver resolver = new DatasetConfigResolver(project, false);

        // Under the resource pattern: kept, including through a coalesced group.
        List<ExternalSplit> inside = splits("s3://bucket/data/a.parquet", "s3://bucket/data/b.parquet");
        ExternalSourceExec kept = resolver.rebind(exec(HOSTILE_CONFIG, DATASET).withSplits(inside));
        assertThat(kept.splits(), equalTo(inside));
        List<ExternalSplit> coalesced = List.of(new CoalescedSplit("parquet", inside));
        assertThat(resolver.rebind(exec(HOSTILE_CONFIG, DATASET).withSplits(coalesced)).splits(), equalTo(coalesced));

        // Outside the prefix: another key in the bucket, another bucket, another store.
        for (String outside : List.of("s3://bucket/secret/x.parquet", "s3://other/data/a.parquet", "gs://bucket/data/a.parquet")) {
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> resolver.rebind(exec(HOSTILE_CONFIG, DATASET).withSplits(splits("s3://bucket/data/a.parquet", outside)))
            );
            assertThat(e.getMessage(), containsString("external dataset [logs] does not cover split [" + outside + "]"));
        }
        // Nested inside a coalesced group too.
        List<ExternalSplit> hidden = List.of(
            new CoalescedSplit("parquet", splits("s3://bucket/data/a.parquet", "s3://bucket/secret/x.parquet"))
        );
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> resolver.rebind(exec(HOSTILE_CONFIG, DATASET).withSplits(hidden))
        );
        assertThat(e.getMessage(), containsString("does not cover split [s3://bucket/secret/x.parquet]"));
    }

    /**
     * A resource may list several paths, comma-separated; a split under any one of them is covered. The list is cut the
     * way the listing cuts it, so the comma of a brace group stays inside its glob, and an HTTP resource is one path.
     */
    public void testCommaSeparatedResourceCoversEachSegment() {
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String listed = "s3://bucket/a/**/*.parquet, s3://bucket/b/x.{parquet,orc}";
        Dataset dataset = new Dataset(DATASET, new DataSourceReference(PARENT), listed, null, Map.of());
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset);
        DatasetConfigResolver resolver = new DatasetConfigResolver(project, false);

        List<ExternalSplit> inside = splits("s3://bucket/a/2024/f.parquet", "s3://bucket/b/x.parquet");
        ExternalSourceExec rebound = resolver.rebind(exec(HOSTILE_CONFIG, DATASET).withSplits(inside));
        assertThat(rebound.splits(), equalTo(inside));
        assertThat(rebound.sourcePath(), equalTo(listed));

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> resolver.rebind(exec(HOSTILE_CONFIG, DATASET).withSplits(splits("s3://bucket/a/f.parquet", "s3://bucket/c/f.parquet")))
        );
        assertThat(e.getMessage(), containsString("external dataset [logs] does not cover split [s3://bucket/c/f.parquet]"));

        assertTrue(isWithin("http://host/a,b.csv", StoragePath.of("http://host/a,b.csv")));
    }

    /** Only file-backed splits carry a path to check; any other split shape is refused for a registered dataset. */
    public void testNonFileSplitRefused() {
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), dataset(Map.of()));
        ExternalSplit opaque = new ExternalSplit() {
            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String getWriteableName() {
                return "opaque";
            }

            @Override
            public void writeTo(StreamOutput out) {}
        };

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new DatasetConfigResolver(project, false).rebind(exec(HOSTILE_CONFIG, DATASET).withSplits(List.of(opaque)))
        );
        assertThat(e.getMessage(), containsString("external dataset [logs] does not cover split ["));
    }

    /** What "within the resource" admits, store by store. */
    public void testIsWithin() {
        // Azure's Hadoop form keeps the container in user-info: another container on the same account is another store.
        StoragePath azure = StoragePath.of("wasbs://logs@acct.blob.core.windows.net/data/*.parquet");
        assertTrue(isWithin("wasbs://logs@acct.blob.core.windows.net/data/x.parquet", azure));
        assertFalse(isWithin("wasbs://secrets@acct.blob.core.windows.net/data/x.parquet", azure));
        assertFalse(isWithin("wasbs://acct.blob.core.windows.net/data/x.parquet", azure));

        // A parent segment steps out of a directory while still starting with it. Against a glob the matcher already
        // refuses it, since `*` cannot cross `/`; against a directory resource only the parent-segment guard does.
        StoragePath local = StoragePath.of("file:///srv/data/*.csv");
        assertTrue(isWithin("file:///srv/data/a.csv", local));
        assertFalse(isWithin("file:///srv/data/../private/a.csv", local));
        assertFalse(isWithin("file:///srv/data/../private/a.csv", StoragePath.of("file:///srv/data/")));

        // A single object admits only itself; a directory admits everything under it.
        assertTrue(isWithin("s3://bucket/data/file.parquet", StoragePath.of("s3://bucket/data/file.parquet")));
        assertFalse(isWithin("s3://bucket/data/file.parquet.bak", StoragePath.of("s3://bucket/data/file.parquet")));
        assertTrue(isWithin("s3://bucket/data/2024/a.parquet", StoragePath.of("s3://bucket/data/")));
        assertFalse(isWithin("s3://bucket/other/a.parquet", StoragePath.of("s3://bucket/data/")));

        StoragePath http = StoragePath.of("https://host/export.csv?table=public");
        assertTrue(isWithin("https://host/export.csv?table=public", http));
        assertFalse(isWithin("https://host/export.csv?table=secrets", http));

        // A glob admits only paths that match its full pattern, including when the first segment contains a wildcard.
        assertTrue(isWithin("s3://bucket/data/2024/a.parquet", StoragePath.of("s3://bucket/data/*/*.parquet")));
        assertFalse(isWithin("s3://bucket/data/2024/a.csv", StoragePath.of("s3://bucket/data/*/*.parquet")));
        assertTrue(isWithin("s3://bucket/data-a.parquet", StoragePath.of("s3://bucket/data*.parquet")));
        assertFalse(isWithin("s3://bucket/secret/x.parquet", StoragePath.of("s3://bucket/data*.parquet")));
        assertFalse(isWithin("s3://bucket/data-a.csv", StoragePath.of("s3://bucket/data*.parquet")));
        assertFalse(isWithin("s3://bucket/dataset/a.parquet", StoragePath.of("s3://bucket/data*")));

        // Scheme and host compare case-insensitively; another port is another store.
        assertTrue(isWithin("S3://BUCKET/data/a.parquet", StoragePath.of("s3://bucket/data/*.parquet")));
        assertFalse(isWithin("http://host:9001/data/a.csv", StoragePath.of("http://host:9000/data/*.csv")));
    }

    private static boolean isWithin(String path, StoragePath resource) {
        return DatasetConfigResolver.isWithin(StoragePath.of(path), resource);
    }

    /** A resource this node's path parser rejects is a definition it cannot bind to; same message as an unknown name. */
    public void testUnparsableResourceRefusedAsNotRegistered() {
        Dataset broken = new Dataset(DATASET, new DataSourceReference(PARENT), "no-scheme-here", null, Map.of());
        ProjectMetadata project = project(dataSource(Map.of("auth", "anonymous")), broken);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new DatasetConfigResolver(project, false).rebind(exec(HOSTILE_CONFIG, DATASET))
        );
        assertThat(e.getMessage(), equalTo("external dataset [logs] is not registered on this node"));
    }

    /** A plan with an empty config is not a way to the node's default client once the dataset's data source has settings. */
    public void testEmptyPlanConfigGetsRegisteredCredentials() {
        ProjectMetadata project = project(dataSource(Map.of("access_key", "AKIA", "secret_key", "s3cr3t")), dataset(Map.of()));

        ExternalSourceExec rebound = new DatasetConfigResolver(project, false).rebind(exec(Map.of(), DATASET));

        Map<String, Object> storage = ExternalSourceResolver.storageConfig(rebound.config());
        assertThat(storage.get("access_key"), equalTo("AKIA"));
        assertThat(storage.get("secret_key"), equalTo("s3cr3t"));
    }

    /**
     * A data source registered with no settings reads with the node's default client; that is the operator's choice at
     * PUT time and the rebind keeps it. What changes is that only a registered dataset can get there.
     */
    public void testDataSourceWithoutSettingsKeepsDefaultClient() {
        ProjectMetadata project = project(dataSource(Map.of()), dataset(Map.of("format", "parquet")));

        ExternalSourceExec rebound = new DatasetConfigResolver(project, false).rebind(exec(HOSTILE_CONFIG, DATASET));

        assertThat(rebound.config(), not(hasKey(ExternalSourceResolver.DATASOURCE_CONFIG_KEY)));
        assertThat(rebound.config(), not(hasKey("role_arn")));
        assertThat(rebound.config().get("format"), equalTo("parquet"));
    }

    // --- helpers ---

    private static void assertRegisteredConfig(ExternalSourceExec rebound, ProjectMetadata project) {
        Dataset dataset = DatasetMetadata.get(project).get(DATASET);
        DataSource parent = DataSourceMetadata.get(project).get(PARENT);
        assertThat(rebound.config(), equalTo(DatasetRewriter.datasetConfig(parent, dataset)));
        assertThat(rebound.sourcePath(), equalTo(RESOURCE));
        assertThat(rebound.datasetName(), equalTo(DATASET));
        Map<String, Object> storage = ExternalSourceResolver.storageConfig(rebound.config());
        assertThat(storage, not(hasKey("role_arn")));
        assertThat(storage.get("endpoint"), not(equalTo("https://attacker.example")));
    }

    private static PhysicalPlan localPlan(PhysicalPlan plan, List<ExternalSplit> nodeSplits, DatasetConfigResolver resolver) {
        return PlannerUtils.localPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            TEST_CFG,
            FoldContext.small(),
            plan,
            TEST_SEARCH_STATS,
            null,
            nodeSplits,
            null,
            resolver
        );
    }

    private static ExternalSourceExec findExec(PhysicalPlan plan) {
        List<ExternalSourceExec> execs = plan.collect(ExternalSourceExec.class);
        assertThat(execs.size(), equalTo(1));
        return execs.get(0);
    }

    private static List<Attribute> output() {
        return List.of(new FieldAttribute(SRC, "a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
    }

    private static ExternalSourceExec exec(Map<String, Object> config, String datasetName) {
        return new ExternalSourceExec(SRC, "s3://attacker/whatever/*.parquet", "parquet", output(), config, Map.of(), null, 10)
            .withDatasetName(datasetName);
    }

    private static ExternalRelation relation(Map<String, Object> config, String datasetName) {
        List<Attribute> output = output();
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(output, "parquet", RESOURCE, null, null, Map.of(), config);
        return new ExternalRelation(SRC, "s3://attacker/whatever/*.parquet", metadata, output, FileList.UNRESOLVED, Map.of(), datasetName);
    }

    private static List<ExternalSplit> splits(String... paths) {
        return Arrays.stream(paths)
            .map(p -> (ExternalSplit) new FileSplit("parquet", StoragePath.of(p), 0, 1024, "parquet", Map.of(), Map.of()))
            .toList();
    }

    private static DataSource dataSource(Map<String, String> settings) {
        Map<String, DataSourceSetting> typed = new HashMap<>();
        settings.forEach((k, v) -> typed.put(k, new DataSourceSetting(v, k.equals("secret_key"))));
        return new DataSource(PARENT, "s3", null, typed);
    }

    private static Dataset dataset(Map<String, Object> settings) {
        return new Dataset(DATASET, new DataSourceReference(PARENT), RESOURCE, null, settings);
    }

    private static ProjectMetadata project(DataSource parent, Dataset dataset) {
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of(parent.name(), parent)))
            .datasets(Map.of(dataset.name(), dataset))
            .build();
    }
}
