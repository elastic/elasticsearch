/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;
import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class FetchRequestTests extends AbstractWireSerializingTestCase<FetchRequest> {
    @Override
    protected Writeable.Reader<FetchRequest> instanceReader() {
        return in -> new FetchRequest(in, new SerializationTestUtils.TestNameIdMapper());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> writeables = new ArrayList<>();
        writeables.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        writeables.addAll(new EsqlPlugin().getNamedWriteables());
        return new NamedWriteableRegistry(writeables);
    }

    @Override
    protected FetchRequest createTestInstance() {
        FetchRequest request = new FetchRequest(
            randomAlphaOfLength(10),
            randomBoolean() ? "" : randomAlphaOfLength(5),
            new OriginalIndices(
                generateRandomStringArray(5, 10, false, false),
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            ),
            randomList(0, 5, FetchRequestTests::randomShardDocs),
            randomConfiguration(),
            fetchPlan(randomList(1, 4, FetchRequestTests::randomField)),
            randomList(0, 5, FetchRequestTests::randomContextId)
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());
        return request;
    }

    @Override
    protected FetchRequest mutateInstance(FetchRequest instance) {
        String sessionId = instance.sessionId();
        String clusterAlias = instance.clusterAlias();
        List<FetchRequest.ShardDocs> shards = instance.shards();
        PhysicalPlan fetchPlan = instance.fetchPlan();
        List<ShardSearchContextId> releaseAfter = instance.releaseAfter();
        switch (between(0, 4)) {
            case 0 -> sessionId = randomValueOtherThan(sessionId, () -> randomAlphaOfLength(10));
            case 1 -> clusterAlias = randomValueOtherThan(clusterAlias, () -> randomAlphaOfLength(5));
            case 2 -> shards = randomValueOtherThan(shards, () -> randomList(0, 5, FetchRequestTests::randomShardDocs));
            case 3 -> fetchPlan = randomValueOtherThan(fetchPlan, () -> fetchPlan(randomList(1, 4, FetchRequestTests::randomField)));
            case 4 -> releaseAfter = randomValueOtherThan(releaseAfter, () -> randomList(0, 5, FetchRequestTests::randomContextId));
            default -> throw new AssertionError("unknown mutation");
        }
        FetchRequest mutated = new FetchRequest(
            sessionId,
            clusterAlias,
            new OriginalIndices(instance.indices(), instance.indicesOptions()),
            shards,
            instance.configuration(),
            fetchPlan,
            releaseAfter
        );
        mutated.setParentTask(instance.getParentTask());
        return mutated;
    }

    /**
     * Runs of one segment each, with gaps that need more than one byte, round trip unchanged.
     */
    public void testDocsRoundTripAsRuns() throws IOException {
        FetchRequest.ShardDocs docs = new FetchRequest.ShardDocs(
            new ShardId("index", "uuid", 0),
            randomContextId(),
            new int[] { 0, 0, 0, 2, 2, 7 },
            new int[] { 1, 5, 100_000, 0, 9, 3 }
        );

        assertThat(copyShardDocs(docs), equalTo(docs));
        FetchRequest.ShardDocs empty = new FetchRequest.ShardDocs(
            new ShardId("index", "uuid", 0),
            randomContextId(),
            new int[0],
            new int[0]
        );
        assertThat(copyShardDocs(empty), equalTo(empty));
    }

    /**
     * The data node returns rows in the order of the documents, so a request never lists them in another order.
     */
    public void testRejectsUnsortedDocs() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        IllegalArgumentException duplicate = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchRequest.ShardDocs(shardId, randomContextId(), new int[] { 1, 1 }, new int[] { 4, 4 })
        );
        assertThat(duplicate.getMessage(), containsString("sorted by segment and doc without duplicates"));
        expectThrows(
            IllegalArgumentException.class,
            () -> new FetchRequest.ShardDocs(shardId, randomContextId(), new int[] { 2, 1 }, new int[] { 0, 5 })
        );
    }

    /**
     * Runs that don't add up to the docs on the wire fail the read instead of loading other documents.
     */
    public void testRejectsRunsThatDontMatchTheDocs() throws IOException {
        assertThat(readMalformed(new int[] { 0 }, new int[] { 3 }, new int[] { 1, 2 }), equalTo("a run of [3] at [0] of [2] docs"));
        assertThat(readMalformed(new int[] { 0 }, new int[] { 0 }, new int[] { 1 }), equalTo("a run of [0] at [0] of [1] docs"));
        assertThat(readMalformed(new int[] { 0 }, new int[] { 1 }, new int[] { 1, 2 }), equalTo("the runs hold [1] of [2] docs"));
        assertThat(readMalformed(new int[] { 0, 1 }, new int[] { 1 }, new int[] { 1 }), equalTo("[2] segments for [1] runs"));
    }

    /**
     * A gap of zero repeats a doc and a gap past the largest int wraps around. Both break the order the node returns
     * rows in, so the read fails.
     */
    public void testRejectsGapsThatBreakTheOrder() throws IOException {
        assertThat(
            readMalformed(new int[] { 0 }, new int[] { 2 }, new int[] { 7, 0 }),
            containsString("sorted by segment and doc without duplicates")
        );
        assertThat(
            readMalformed(new int[] { 0 }, new int[] { 2 }, new int[] { Integer.MAX_VALUE, 1 }),
            equalTo("negative segment or doc at [1]")
        );
    }

    /**
     * When authorization leaves no index the request keeps its shards, so the data node fails each of them instead of
     * returning fewer rows. It frees nothing, like a free request without an index.
     */
    public void testKeepsItsShardsAndFreesNothingWhenAuthorizationLeavesNoIndex() {
        FetchRequest request = randomValueOtherThanMany(r -> r.shards().isEmpty(), this::createTestInstance);
        List<FetchRequest.ShardDocs> shards = request.shards();

        request.indices(NO_INDICES_OR_ALIASES_ARRAY);

        assertThat(request.shards(), equalTo(shards));
        assertThat(request.releaseAfter(), empty());
    }

    /**
     * The fetch plan reads no lookup tables, so they never travel with the request.
     */
    public void testShipsNoTables() {
        FetchRequest request = new FetchRequest(
            "session",
            "",
            new OriginalIndices(new String[] { "index" }, IndicesOptions.strictExpandOpen()),
            List.of(),
            randomConfiguration("FROM index", randomValueOtherThanMany(Map::isEmpty, () -> randomTables())),
            fetchPlan(List.of(randomField())),
            List.of()
        );
        assertThat(request.configuration().tables(), anEmptyMap());
    }

    /**
     * Only nodes that ran the query phase with document references get fetch requests.
     */
    public void testNeverReachesAnOlderNode() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(FetchRequest.ESQL_FETCH);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> copyInstance(createTestInstance(), older));
        assertThat(e.getMessage(), containsString("can't send a fetch request to a node on"));
    }

    static PhysicalPlan fetchPlan(List<Attribute> fetched) {
        Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        return new ProjectExec(
            Source.EMPTY,
            new FieldExtractExec(Source.EMPTY, new FetchSourceExec(Source.EMPTY, doc, 64), fetched, NONE),
            fetched
        );
    }

    private static Attribute randomField() {
        String name = randomAlphaOfLength(6);
        DataType type = randomFrom(DataType.KEYWORD, DataType.LONG, DataType.INTEGER, DataType.DOUBLE);
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static FetchRequest.ShardDocs randomShardDocs() {
        TreeSet<Long> unique = new TreeSet<>();
        int count = between(0, 50);
        while (unique.size() < count) {
            unique.add(((long) between(0, 20) << 32) | between(0, 200_000));
        }
        int[] segments = new int[count];
        int[] docs = new int[count];
        int i = 0;
        for (long segmentAndDoc : unique) {
            segments[i] = (int) (segmentAndDoc >>> 32);
            docs[i] = (int) segmentAndDoc;
            i++;
        }
        return new FetchRequest.ShardDocs(
            new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), between(0, 10)),
            randomContextId(),
            segments,
            docs
        );
    }

    private static ShardSearchContextId randomContextId() {
        return new ShardSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong(), randomBoolean() ? null : randomAlphaOfLength(8));
    }

    /**
     * Reads documents written as the given runs, and returns why the read failed.
     */
    private static String readMalformed(int[] runSegments, int[] runLengths, int[] gaps) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new ShardId("index", "uuid", 0).writeTo(out);
            randomContextId().writeTo(out);
            out.writeVIntArray(runSegments);
            out.writeVIntArray(runLengths);
            out.writeVIntArray(gaps);
            try (StreamInput in = out.bytes().streamInput()) {
                return expectThrows(IllegalArgumentException.class, () -> FetchRequest.ShardDocs.readFrom(in)).getMessage();
            }
        }
    }

    private static FetchRequest.ShardDocs copyShardDocs(FetchRequest.ShardDocs docs) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            docs.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                FetchRequest.ShardDocs copy = FetchRequest.ShardDocs.readFrom(in);
                assertThat(Arrays.toString(copy.docs()), copy.docs(), equalTo(docs.docs()));
                return copy;
            }
        }
    }
}
