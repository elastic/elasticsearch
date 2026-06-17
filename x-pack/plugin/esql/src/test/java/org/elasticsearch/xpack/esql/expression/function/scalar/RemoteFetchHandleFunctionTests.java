/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.plugin.RemoteFetchHandle;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Keeps evaluator-level checks hand-rolled because generic scalar-function fixtures build rows via
 * {@code BlockUtils.fromListRow}, which does not support DOC blocks.
 */
public class RemoteFetchHandleFunctionTests extends ESTestCase {
    public void testEncodesDocColumnIntoRemoteFetchHandles() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        ReferenceAttribute doc = new ReferenceAttribute(Source.EMPTY, null, "_doc", DataType.DOC_DATA_TYPE);
        Layout.Builder layout = new Layout.Builder();
        layout.append(doc);

        Page input = null;
        Page output = null;
        try (
            DocBlock.Builder docBuilder = DocBlock.newBlockBuilder(driverContext.blockFactory(), 2);
            IntBlock.Builder sortBuilder = driverContext.blockFactory().newIntBlockBuilder(2);
            EvalOperator operator = new EvalOperator(
                driverContext,
                EvalMapper.toEvaluator(
                    FoldContext.small(),
                    new RemoteFetchHandleFunction(Source.EMPTY, doc, "node-a", "session-a"),
                    layout.build()
                ).get(driverContext)
            )
        ) {
            docBuilder.appendShard(1).appendSegment(2).appendDoc(10);
            docBuilder.appendShard(3).appendSegment(4).appendDoc(20);
            sortBuilder.appendInt(100);
            sortBuilder.appendInt(200);
            input = new Page(docBuilder.build(), sortBuilder.build());

            operator.addInput(input);
            input = null;
            output = operator.getOutput();

            assertEquals(3, output.getBlockCount());
            IntBlock sortValues = output.getBlock(1);
            assertEquals(100, sortValues.getInt(0));
            assertEquals(200, sortValues.getInt(1));

            BytesRefBlock handles = output.getBlock(2);
            assertEquals(2, handles.getPositionCount());
            assertEquals(new RemoteFetchHandle("node-a", "session-a", 1, 2, 10), decode(handles, 0));
            assertEquals(new RemoteFetchHandle("node-a", "session-a", 3, 4, 20), decode(handles, 1));
        } finally {
            if (input != null) {
                input.releaseBlocks();
            }
            if (output != null) {
                output.releaseBlocks();
            }
        }
    }

    public void testRejectsNonDocAttribute() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new RemoteFetchHandleFunction(
                Source.EMPTY,
                new ReferenceAttribute(Source.EMPTY, null, "not_doc", DataType.KEYWORD),
                "node-a",
                "session-a"
            )
        );
        assertThat(e.getMessage(), containsString("requires _doc input"));
    }

    public void testRejectsNonAttributeExpressionOnReplaceChildren() {
        RemoteFetchHandleFunction function = new RemoteFetchHandleFunction(
            Source.EMPTY,
            new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false),
            "node-a",
            "session-a"
        );
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> function.replaceChildren(List.of(new Literal(Source.EMPTY, null, DataType.NULL)))
        );
        assertThat(e.getMessage(), containsString("requires _doc attribute input"));
    }

    private static RemoteFetchHandle decode(BytesRefBlock handles, int position) {
        BytesRef scratch = new BytesRef();
        return RemoteFetchHandle.fromBytesRef(handles.getBytesRef(handles.getFirstValueIndex(position), scratch));
    }
}
