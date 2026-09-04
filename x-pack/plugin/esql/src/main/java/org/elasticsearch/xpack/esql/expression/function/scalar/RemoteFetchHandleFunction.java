/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plugin.RemoteFetchHandle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Internal scalar that converts node-local {@code _doc} values into transport-safe remote fetch handles.
 * <p>
 * Each handle encodes enough routing information for follow-up fetches: the owning node, retained session, and
 * doc identity ({@code shard/segment/doc}) for the originating reader. Handles are only valid while the retained
 * session is alive; they are not a durable document identifier.
 * <p>
 * This function is intentionally not registered as user-visible ES|QL syntax. The remote fetch planner can build an
 * {@link org.elasticsearch.xpack.esql.plan.physical.EvalExec} containing this expression when it needs to carry
 * node-local doc references through a generic exchange.
 */
public class RemoteFetchHandleFunction extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RemoteFetchHandleFunction",
        RemoteFetchHandleFunction::new
    );

    /**
     * Attribute that evaluates to metadata {@code _doc} values.
     */
    private final Attribute doc;
    /*
     * Node/retained-session routing intentionally participates in expression identity through equals/hashCode, so
     * equivalent _doc expressions for different retained sessions cannot be deduplicated together. The current
     * planner creates a single handle expression per query, and future multi-fetch planning should preserve this
     * distinction.
     */
    private final String nodeId;
    /**
     * Identifies the retained shard-context session that makes the encoded doc ids usable for follow-up fetches.
     */
    private final String retainedSessionId;

    public RemoteFetchHandleFunction(Source source, Attribute doc, String nodeId, String retainedSessionId) {
        super(source, List.of(doc));
        this.doc = doc;
        if (doc.typeResolved().resolved()
            && (doc.dataType() != DataType.DOC_DATA_TYPE || MetadataAttribute.DOC.equals(doc.name()) == false)) {
            throw new IllegalStateException("remote fetch handle requires _doc input but got [" + doc.dataType() + ":" + doc.name() + "]");
        }
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.retainedSessionId = Objects.requireNonNull(retainedSessionId, "retainedSessionId");
    }

    private RemoteFetchHandleFunction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            requireDocAttribute(in.readNamedWriteable(Expression.class)),
            in.readString(),
            in.readString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(doc);
        out.writeString(nodeId);
        out.writeString(retainedSessionId);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(doc, type -> type == DataType.DOC_DATA_TYPE, sourceText(), FIRST, "_doc");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RemoteFetchHandleFunction(source(), requireDocAttribute(newChildren.get(0)), nodeId, retainedSessionId);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RemoteFetchHandleFunction::new, doc, nodeId, retainedSessionId);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) {
            return false;
        }

        RemoteFetchHandleFunction other = (RemoteFetchHandleFunction) o;
        return Objects.equals(nodeId, other.nodeId) && Objects.equals(retainedSessionId, other.retainedSessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nodeId, retainedSessionId);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory docEvaluator = toEvaluator.apply(doc);
        // Keep this evaluator hand-written: each row serializes a RemoteFetchHandle into bytes using a reused scratch stream.
        return driverContext -> new Evaluator(driverContext, docEvaluator.get(driverContext), nodeId, retainedSessionId);
    }

    Attribute doc() {
        return doc;
    }

    String nodeId() {
        return nodeId;
    }

    String retainedSessionId() {
        return retainedSessionId;
    }

    private static Attribute requireDocAttribute(Expression expression) {
        if (expression instanceof Attribute attribute) {
            return attribute;
        }
        throw new IllegalStateException("remote fetch handle requires _doc attribute input but got [" + expression.nodeName() + "]");
    }

    private static final class Evaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final DriverContext driverContext;
        private final ExpressionEvaluator doc;
        private final String nodeId;
        private final String retainedSessionId;

        private Evaluator(DriverContext driverContext, ExpressionEvaluator doc, String nodeId, String retainedSessionId) {
            this.driverContext = driverContext;
            this.doc = doc;
            this.nodeId = nodeId;
            this.retainedSessionId = retainedSessionId;
        }

        @Override
        public Block eval(Page page) {
            try (
                Block block = doc.eval(page);
                BytesRefBlock.Builder handleBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(page.getPositionCount());
                BytesStreamOutput scratch = new BytesStreamOutput()
            ) {
                if (block instanceof DocBlock == false) {
                    throw new IllegalStateException(
                        "remote fetch handle requires a _doc block but got [" + block.getClass().getName() + "]"
                    );
                }
                // DocBlock is always single-valued and non-null by construction.
                DocVector docVector = ((DocBlock) block).asVector();
                for (int position = 0; position < page.getPositionCount(); position++) {
                    scratch.reset();
                    try {
                        RemoteFetchHandle.encodeTo(
                            scratch,
                            nodeId,
                            retainedSessionId,
                            docVector.shards().getInt(position),
                            docVector.segments().getInt(position),
                            docVector.docs().getInt(position)
                        );
                    } catch (IOException e) {
                        throw new UncheckedIOException("failed to encode remote fetch handle", e);
                    }
                    handleBuilder.appendBytesRef(scratch.bytes().toBytesRef());
                }
                return handleBuilder.build();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + doc.baseRamBytesUsed() + RamUsageEstimator.sizeOf(nodeId) + RamUsageEstimator.sizeOf(
                retainedSessionId
            );
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(doc);
        }
    }
}
