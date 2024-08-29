/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.Entry.of;

/**
 * A utility class that consists solely of static methods that describe how to serialize and
 * deserialize QL and ESQL plan types.
 * <P>
 * All types that require to be serialized should have a pair of co-located `readFoo` and `writeFoo`
 * methods that deserialize and serialize respectively.
 * <P>
 * A type can be named or non-named. A named type has a name written to the stream before its
 * contents (similar to NamedWriteable), whereas a non-named type does not (similar to Writable).
 * Named types allow to determine specific deserialization implementations for more general types,
 * e.g. Literal, which is an Expression. Named types must have an entries in the namedTypeEntries
 * list.
 */
public final class PlanNamedTypes {

    private PlanNamedTypes() {}

    /**
     * Determines the writeable name of the give class. The simple class name is commonly used for
     * {@link NamedWriteable}s and is sufficient here too, but it could be almost anything else.
     */
    public static String name(Class<?> cls) {
        return cls.getSimpleName();
    }

    /**
     * List of named type entries that link concrete names to stream reader and writer implementations.
     * Entries have the form:  category,  name,  serializer method,  deserializer method.
     */
    public static List<PlanNameRegistry.Entry> namedTypeEntries() {
        List<PlanNameRegistry.Entry> declared = List.of(
            // Physical Plan Nodes
            of(PhysicalPlan.class, AggregateExec.ENTRY),
            of(PhysicalPlan.class, DissectExec.ENTRY),
            of(PhysicalPlan.class, EsQueryExec.ENTRY),
            of(PhysicalPlan.class, EsSourceExec.ENTRY),
            of(PhysicalPlan.class, EvalExec.ENTRY),
            of(PhysicalPlan.class, EnrichExec.class, PlanNamedTypes::writeEnrichExec, PlanNamedTypes::readEnrichExec),
            of(PhysicalPlan.class, ExchangeExec.class, PlanNamedTypes::writeExchangeExec, PlanNamedTypes::readExchangeExec),
            of(PhysicalPlan.class, ExchangeSinkExec.class, PlanNamedTypes::writeExchangeSinkExec, PlanNamedTypes::readExchangeSinkExec),
            of(
                PhysicalPlan.class,
                ExchangeSourceExec.class,
                PlanNamedTypes::writeExchangeSourceExec,
                PlanNamedTypes::readExchangeSourceExec
            ),
            of(PhysicalPlan.class, FieldExtractExec.class, PlanNamedTypes::writeFieldExtractExec, PlanNamedTypes::readFieldExtractExec),
            of(PhysicalPlan.class, FilterExec.class, PlanNamedTypes::writeFilterExec, PlanNamedTypes::readFilterExec),
            of(PhysicalPlan.class, FragmentExec.class, PlanNamedTypes::writeFragmentExec, PlanNamedTypes::readFragmentExec),
            of(PhysicalPlan.class, GrokExec.class, PlanNamedTypes::writeGrokExec, PlanNamedTypes::readGrokExec),
            of(PhysicalPlan.class, LimitExec.class, PlanNamedTypes::writeLimitExec, PlanNamedTypes::readLimitExec),
            of(PhysicalPlan.class, LocalSourceExec.class, (out, v) -> v.writeTo(out), LocalSourceExec::new),
            of(PhysicalPlan.class, HashJoinExec.class, (out, v) -> v.writeTo(out), HashJoinExec::new),
            of(PhysicalPlan.class, MvExpandExec.class, PlanNamedTypes::writeMvExpandExec, PlanNamedTypes::readMvExpandExec),
            of(PhysicalPlan.class, OrderExec.class, PlanNamedTypes::writeOrderExec, PlanNamedTypes::readOrderExec),
            of(PhysicalPlan.class, ProjectExec.class, PlanNamedTypes::writeProjectExec, PlanNamedTypes::readProjectExec),
            of(PhysicalPlan.class, RowExec.class, PlanNamedTypes::writeRowExec, PlanNamedTypes::readRowExec),
            of(PhysicalPlan.class, ShowExec.class, PlanNamedTypes::writeShowExec, PlanNamedTypes::readShowExec),
            of(PhysicalPlan.class, TopNExec.class, PlanNamedTypes::writeTopNExec, PlanNamedTypes::readTopNExec)
        );
        return declared;
    }

    // -- physical plan nodes
    static EnrichExec readEnrichExec(PlanStreamInput in) throws IOException {
        final Source source = Source.readFrom(in);
        final PhysicalPlan child = in.readPhysicalPlanNode();
        final NamedExpression matchField = in.readNamedWriteable(NamedExpression.class);
        final String policyName = in.readString();
        final String matchType = (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) ? in.readString() : "match";
        final String policyMatchField = in.readString();
        final Map<String, String> concreteIndices;
        final Enrich.Mode mode;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            mode = in.readEnum(Enrich.Mode.class);
            concreteIndices = in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            mode = Enrich.Mode.ANY;
            EsIndex esIndex = new EsIndex(in);
            if (esIndex.concreteIndices().size() != 1) {
                throw new IllegalStateException("expected a single concrete enrich index; got " + esIndex.concreteIndices());
            }
            concreteIndices = Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, Iterables.get(esIndex.concreteIndices(), 0));
        }
        return new EnrichExec(
            source,
            child,
            mode,
            matchType,
            matchField,
            policyName,
            policyMatchField,
            concreteIndices,
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    static void writeEnrichExec(PlanStreamOutput out, EnrichExec enrich) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(enrich.child());
        out.writeNamedWriteable(enrich.matchField());
        out.writeString(enrich.policyName());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeString(enrich.matchType());
        }
        out.writeString(enrich.policyMatchField());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeEnum(enrich.mode());
            out.writeMap(enrich.concreteIndices(), StreamOutput::writeString, StreamOutput::writeString);
        } else {
            if (enrich.concreteIndices().keySet().equals(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))) {
                String concreteIndex = enrich.concreteIndices().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                new EsIndex(concreteIndex, Map.of(), Set.of(concreteIndex)).writeTo(out);
            } else {
                throw new IllegalStateException("expected a single concrete enrich index; got " + enrich.concreteIndices());
            }
        }
        out.writeNamedWriteableCollection(enrich.enrichFields());
    }

    static ExchangeExec readExchangeExec(PlanStreamInput in) throws IOException {
        return new ExchangeExec(
            Source.readFrom(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.readPhysicalPlanNode()
        );
    }

    static void writeExchangeExec(PlanStreamOutput out, ExchangeExec exchangeExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(exchangeExec.output());
        out.writeBoolean(exchangeExec.isInBetweenAggs());
        out.writePhysicalPlanNode(exchangeExec.child());
    }

    static ExchangeSinkExec readExchangeSinkExec(PlanStreamInput in) throws IOException {
        return new ExchangeSinkExec(
            Source.readFrom(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.readPhysicalPlanNode()
        );
    }

    static void writeExchangeSinkExec(PlanStreamOutput out, ExchangeSinkExec exchangeSinkExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(exchangeSinkExec.output());
        out.writeBoolean(exchangeSinkExec.isIntermediateAgg());
        out.writePhysicalPlanNode(exchangeSinkExec.child());
    }

    static ExchangeSourceExec readExchangeSourceExec(PlanStreamInput in) throws IOException {
        return new ExchangeSourceExec(Source.readFrom(in), in.readNamedWriteableCollectionAsList(Attribute.class), in.readBoolean());
    }

    static void writeExchangeSourceExec(PlanStreamOutput out, ExchangeSourceExec exchangeSourceExec) throws IOException {
        out.writeNamedWriteableCollection(exchangeSourceExec.output());
        out.writeBoolean(exchangeSourceExec.isIntermediateAgg());
    }

    static FieldExtractExec readFieldExtractExec(PlanStreamInput in) throws IOException {
        return new FieldExtractExec(Source.readFrom(in), in.readPhysicalPlanNode(), in.readNamedWriteableCollectionAsList(Attribute.class));
    }

    static void writeFieldExtractExec(PlanStreamOutput out, FieldExtractExec fieldExtractExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(fieldExtractExec.child());
        out.writeNamedWriteableCollection(fieldExtractExec.attributesToExtract());
    }

    static FilterExec readFilterExec(PlanStreamInput in) throws IOException {
        return new FilterExec(Source.readFrom(in), in.readPhysicalPlanNode(), in.readNamedWriteable(Expression.class));
    }

    static void writeFilterExec(PlanStreamOutput out, FilterExec filterExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(filterExec.child());
        out.writeNamedWriteable(filterExec.condition());
    }

    static FragmentExec readFragmentExec(PlanStreamInput in) throws IOException {
        return new FragmentExec(
            Source.readFrom(in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readOptionalVInt(),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readOptionalPhysicalPlanNode() : null
        );
    }

    static void writeFragmentExec(PlanStreamOutput out, FragmentExec fragmentExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(fragmentExec.fragment());
        out.writeOptionalNamedWriteable(fragmentExec.esFilter());
        out.writeOptionalVInt(fragmentExec.estimatedRowSize());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeOptionalPhysicalPlanNode(fragmentExec.reducer());
        }
    }

    static GrokExec readGrokExec(PlanStreamInput in) throws IOException {
        Source source;
        return new GrokExec(
            source = Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readNamedWriteable(Expression.class),
            Grok.pattern(source, in.readString()),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    static void writeGrokExec(PlanStreamOutput out, GrokExec grokExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(grokExec.child());
        out.writeNamedWriteable(grokExec.inputExpression());
        out.writeString(grokExec.pattern().pattern());
        out.writeNamedWriteableCollection(grokExec.extractedFields());
    }

    static LimitExec readLimitExec(PlanStreamInput in) throws IOException {
        return new LimitExec(Source.readFrom(in), in.readPhysicalPlanNode(), in.readNamedWriteable(Expression.class));
    }

    static void writeLimitExec(PlanStreamOutput out, LimitExec limitExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(limitExec.child());
        out.writeNamedWriteable(limitExec.limit());
    }

    static MvExpandExec readMvExpandExec(PlanStreamInput in) throws IOException {
        return new MvExpandExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readNamedWriteable(NamedExpression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    static void writeMvExpandExec(PlanStreamOutput out, MvExpandExec mvExpandExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(mvExpandExec.child());
        out.writeNamedWriteable(mvExpandExec.target());
        out.writeNamedWriteable(mvExpandExec.expanded());
    }

    static OrderExec readOrderExec(PlanStreamInput in) throws IOException {
        return new OrderExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readCollectionAsList(org.elasticsearch.xpack.esql.expression.Order::new)
        );
    }

    static void writeOrderExec(PlanStreamOutput out, OrderExec orderExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(orderExec.child());
        out.writeCollection(orderExec.order());
    }

    static ProjectExec readProjectExec(PlanStreamInput in) throws IOException {
        return new ProjectExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    static void writeProjectExec(PlanStreamOutput out, ProjectExec projectExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(projectExec.child());
        out.writeNamedWriteableCollection(projectExec.projections());
    }

    static RowExec readRowExec(PlanStreamInput in) throws IOException {
        return new RowExec(Source.readFrom(in), in.readCollectionAsList(Alias::new));
    }

    static void writeRowExec(PlanStreamOutput out, RowExec rowExec) throws IOException {
        assert rowExec.children().size() == 0;
        Source.EMPTY.writeTo(out);
        out.writeCollection(rowExec.fields());
    }

    @SuppressWarnings("unchecked")
    static ShowExec readShowExec(PlanStreamInput in) throws IOException {
        return new ShowExec(
            Source.readFrom(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            (List<List<Object>>) in.readGenericValue()
        );
    }

    static void writeShowExec(PlanStreamOutput out, ShowExec showExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(showExec.output());
        out.writeGenericValue(showExec.values());
    }

    static TopNExec readTopNExec(PlanStreamInput in) throws IOException {
        return new TopNExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readCollectionAsList(org.elasticsearch.xpack.esql.expression.Order::new),
            in.readNamedWriteable(Expression.class),
            in.readOptionalVInt()
        );
    }

    static void writeTopNExec(PlanStreamOutput out, TopNExec topNExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(topNExec.child());
        out.writeCollection(topNExec.order());
        out.writeNamedWriteable(topNExec.limit());
        out.writeOptionalVInt(topNExec.estimatedRowSize());
    }
}
