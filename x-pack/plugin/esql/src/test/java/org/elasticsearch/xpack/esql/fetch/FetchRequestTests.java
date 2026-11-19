/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.IndicesRequest;
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
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

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
            randomList(1, 4, FetchRequestTests::randomField)
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());
        return request;
    }

    @Override
    protected FetchRequest mutateInstance(FetchRequest instance) {
        String sessionId = instance.sessionId();
        String clusterAlias = instance.clusterAlias();
        List<FetchRequest.ShardDocs> shards = instance.shards();
        List<Attribute> attributes = instance.attributes();
        switch (between(0, 3)) {
            case 0 -> sessionId = randomValueOtherThan(sessionId, () -> randomAlphaOfLength(10));
            case 1 -> clusterAlias = randomValueOtherThan(clusterAlias, () -> randomAlphaOfLength(5));
            case 2 -> shards = randomValueOtherThan(shards, () -> randomList(0, 5, FetchRequestTests::randomShardDocs));
            case 3 -> attributes = randomValueOtherThan(attributes, () -> randomList(1, 4, FetchRequestTests::randomField));
            default -> throw new AssertionError("unknown mutation");
        }
        FetchRequest mutated = new FetchRequest(
            sessionId,
            clusterAlias,
            new OriginalIndices(instance.indices(), instance.indicesOptions()),
            shards,
            instance.configuration(),
            attributes
        );
        mutated.setParentTask(instance.getParentTask());
        return mutated;
    }

    /**
     * Documents of several segments, docs that need more than one byte, and no documents at all round trip unchanged.
     */
    public void testDocsRoundTrip() throws IOException {
        FetchRequest.ShardDocs docs = new FetchRequest.ShardDocs(
            new ShardId("index", "uuid", 0),
            randomContextId(),
            randomBoolean(),
            new int[] { 0, 0, 0, 2, 2, 7 },
            new int[] { 1, 5, 100_000, 0, 9, 3 }
        );

        assertThat(copyShardDocs(docs), equalTo(docs));
        FetchRequest.ShardDocs empty = new FetchRequest.ShardDocs(
            new ShardId("index", "uuid", 0),
            randomContextId(),
            randomBoolean(),
            new int[0],
            new int[0]
        );
        assertThat(copyShardDocs(empty), equalTo(empty));
    }

    /**
     * Whether the node frees the context of a shard once it answered survives the wire, for both answers.
     */
    public void testFreeAfterFetchRoundTrips() throws IOException {
        for (boolean freeAfterFetch : new boolean[] { true, false }) {
            FetchRequest.ShardDocs docs = new FetchRequest.ShardDocs(
                new ShardId("index", "uuid", 0),
                randomContextId(),
                freeAfterFetch,
                new int[] { 0 },
                new int[] { 1 }
            );

            assertThat(copyShardDocs(docs).freeAfterFetch(), equalTo(freeAfterFetch));
        }
    }

    /**
     * The data node returns rows in the order of the documents, so a request never lists them in another order.
     */
    public void testRejectsUnsortedDocs() {
        ShardId shardId = new ShardId("index", "uuid", 0);
        IllegalArgumentException duplicate = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchRequest.ShardDocs(shardId, randomContextId(), randomBoolean(), new int[] { 1, 1 }, new int[] { 4, 4 })
        );
        assertThat(duplicate.getMessage(), containsString("sorted by segment and doc without duplicates"));
        expectThrows(
            IllegalArgumentException.class,
            () -> new FetchRequest.ShardDocs(shardId, randomContextId(), randomBoolean(), new int[] { 2, 1 }, new int[] { 0, 5 })
        );
    }

    /**
     * Unsorted documents on the wire fail the read instead of loading rows in another order.
     */
    public void testRejectsUnsortedDocsOnTheWire() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new ShardId("index", "uuid", 0).writeTo(out);
            randomContextId().writeTo(out);
            out.writeBoolean(randomBoolean());
            out.writeVIntArray(new int[] { 0, 0 });
            out.writeVIntArray(new int[] { 7, 3 });
            try (StreamInput in = out.bytes().streamInput()) {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FetchRequest.ShardDocs.readFrom(in));
                assertThat(e.getMessage(), containsString("sorted by segment and doc without duplicates"));
            }
        }
    }

    /**
     * Security replaces the index expressions of a fetch request with the indices and aliases the user may read, and
     * the node reads only shards of what they resolve to.
     */
    public void testSecurityReplacesTheIndices() {
        FetchRequest request = createTestInstance();
        assertThat(request, instanceOf(IndicesRequest.Replaceable.class));

        String[] allowed = generateRandomStringArray(3, 10, false, false);
        assertThat(request.indices(allowed), sameInstance(request));

        assertThat(request.indices(), equalTo(allowed));
    }

    /**
     * A {@code _doc} column can't leave the node that holds its shards.
     */
    public void testRefusesToLoadDocuments() {
        Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FetchRequest(
                "session",
                "",
                new OriginalIndices(new String[] { "index" }, IndicesOptions.strictExpandOpen()),
                List.of(),
                randomConfiguration(),
                List.of(randomField(), doc)
            )
        );
        assertThat(e.getMessage(), containsString("a fetch can't load"));
    }

    /**
     * The fetch reads no lookup tables, so they never travel with the request.
     */
    public void testShipsNoTables() {
        FetchRequest request = new FetchRequest(
            "session",
            "",
            new OriginalIndices(new String[] { "index" }, IndicesOptions.strictExpandOpen()),
            List.of(),
            randomConfiguration("FROM index", randomValueOtherThanMany(Map::isEmpty, () -> randomTables())),
            List.of(randomField())
        );
        assertThat(request.configuration().tables(), anEmptyMap());
    }

    /**
     * A gateway node that forwards a fetch request to another node of its cluster builds a task of it to check its type,
     * without a parent. The request still has the parent its coordinator gave it, so its task is a cancellable one.
     */
    public void testATaskBuiltWithoutAParentByAGatewayIsCancellable() {
        FetchRequest request = createTestInstance();
        request.setParentTask(new TaskId("coordinator", 7));

        Task task = request.createTask(1, "transport", FetchService.FETCH_ACTION_NAME, TaskId.EMPTY_TASK_ID, Map.of());

        assertThat(task, instanceOf(CancellableTask.class));
    }

    /**
     * A fetch request is a child of the query that selected its documents, so cancelling the query cancels it.
     */
    public void testARequestWithoutAParentIsRefused() {
        FetchRequest request = createTestInstance();
        request.setParentTask(TaskId.EMPTY_TASK_ID);

        expectThrows(
            AssertionError.class,
            () -> request.createTask(1, "transport", FetchService.FETCH_ACTION_NAME, TaskId.EMPTY_TASK_ID, Map.of())
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
            randomBoolean(),
            segments,
            docs
        );
    }

    private static ShardSearchContextId randomContextId() {
        return new ShardSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong(), randomBoolean() ? null : randomAlphaOfLength(8));
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
