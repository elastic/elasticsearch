/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.EXPLAIN;
import static org.elasticsearch.xpack.esql.into.IntoPageWriter.Factory.Indexing.INDEX;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.Outcome.CREATED;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.Outcome.REFUSED;
import static org.elasticsearch.xpack.esql.into.IntoTestUtils.stats;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IntoOperatorTests extends ComputeTestCase {
    private static final List<Attribute> COLUMNS = List.of(new ReferenceAttribute(Source.EMPTY, "h", DataType.KEYWORD));

    @SuppressWarnings("this-escape")
    private final ThreadPool threadPool = new TestThreadPool(getTestName());
    private final IntoTestUtils.CapturingClient client = new IntoTestUtils.CapturingClient(threadPool);
    @SuppressWarnings("this-escape")
    private final BlockFactory blockFactory = blockFactory();
    private final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    private final IntoPageWriter.Factory sink = IntoPageWriter.Factory.createFactory(client, IntoTestUtils.DESTINATION, COLUMNS, INDEX);

    @After
    public void tearDownOperator() throws Exception {
        while (client.listeners.isEmpty() == false) {
            client.fail(new RuntimeException("test cleanup"));
        }
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testStatsRowWaitsForBulks() {
        Operator op = new IntoOperator.Factory(sink, Function.identity()).get(driverContext);
        op.addInput(page("h1", "h2"));
        op.finish();
        assertThat(op.getOutput(), nullValue());
        assertFalse(op.isBlocked().listener().isDone());

        client.answer(CREATED, CREATED);
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(2, 2)));
        assertTrue(op.isFinished());
    }

    public void testZeroPagesEmitsZeroZero() {
        Operator op = new IntoOperator.Factory(sink, Function.identity()).get(driverContext);
        op.finish();
        assertTrue(op.canProduceMoreDataWithoutExtraInput());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(0, 0)));
        assertTrue(op.isFinished());
    }

    public void testRecordedFailureIsRethrown() {
        Operator op = new IntoOperator.Factory(sink, Function.identity()).get(driverContext);
        op.addInput(page("ok", "bad"));
        op.finish();
        client.answer(CREATED, REFUSED);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, op::getOutput);
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testFailureFastAtAddInput() {
        Operator op = new IntoOperator.Factory(sink, Function.identity()).get(driverContext);
        op.addInput(page("bad"));
        client.answer(REFUSED);
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> op.addInput(page("next")));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(client.bulks, empty());
    }

    public void testCheckedExceptionSurfacesAsElasticsearchException() {
        Operator op = new IntoOperator.Factory(sink, Function.identity()).get(driverContext);
        op.addInput(page("h1"));
        op.finish();
        client.fail(new IOException("simulated IO failure"));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, op::getOutput);
        assertThat(e.getMessage(), containsString("simulated IO failure"));
    }

    public void testExplainModeDiscardsPages() {
        IntoPageWriter.Factory explainSink = IntoPageWriter.Factory.createFactory(client, IntoTestUtils.DESTINATION, COLUMNS, EXPLAIN);
        Operator op = new IntoOperator.Factory(explainSink, Function.identity()).get(driverContext);
        op.addInput(page("explained"));
        op.finish();
        assertThat(client.bulks, empty());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(0, 0)));
        assertTrue(op.isFinished());
    }

    private Page page(String... values) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (String value : values) {
                builder.appendBytesRef(new BytesRef(value));
            }
            return new Page(builder.build());
        }
    }
}
