/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Order;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Dissect.Parser;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
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
import static org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanReader.readerFromPlanReader;
import static org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanWriter.writerFromPlanWriter;

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
            of(PhysicalPlan.class, AggregateExec.class, PlanNamedTypes::writeAggregateExec, PlanNamedTypes::readAggregateExec),
            of(PhysicalPlan.class, DissectExec.class, PlanNamedTypes::writeDissectExec, PlanNamedTypes::readDissectExec),
            of(PhysicalPlan.class, EsQueryExec.class, PlanNamedTypes::writeEsQueryExec, PlanNamedTypes::readEsQueryExec),
            of(PhysicalPlan.class, EsSourceExec.class, PlanNamedTypes::writeEsSourceExec, PlanNamedTypes::readEsSourceExec),
            of(PhysicalPlan.class, EvalExec.class, PlanNamedTypes::writeEvalExec, PlanNamedTypes::readEvalExec),
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
            of(PhysicalPlan.class, TopNExec.class, PlanNamedTypes::writeTopNExec, PlanNamedTypes::readTopNExec),
            // Logical Plan Nodes - a subset of plans that end up being actually serialized
            of(LogicalPlan.class, Aggregate.class, Aggregate::writeAggregate, Aggregate::new),
            of(LogicalPlan.class, Dissect.class, PlanNamedTypes::writeDissect, PlanNamedTypes::readDissect),
            of(LogicalPlan.class, EsRelation.class, PlanNamedTypes::writeEsRelation, PlanNamedTypes::readEsRelation),
            of(LogicalPlan.class, Eval.class, PlanNamedTypes::writeEval, PlanNamedTypes::readEval),
            of(LogicalPlan.class, Enrich.class, PlanNamedTypes::writeEnrich, PlanNamedTypes::readEnrich),
            of(LogicalPlan.class, EsqlProject.class, PlanNamedTypes::writeEsqlProject, PlanNamedTypes::readEsqlProject),
            of(LogicalPlan.class, Filter.class, PlanNamedTypes::writeFilter, PlanNamedTypes::readFilter),
            of(LogicalPlan.class, Grok.class, PlanNamedTypes::writeGrok, PlanNamedTypes::readGrok),
            of(LogicalPlan.class, InlineStats.class, (PlanStreamOutput out, InlineStats v) -> v.writeTo(out), InlineStats::new),
            of(LogicalPlan.class, Join.ENTRY),
            of(LogicalPlan.class, Limit.class, PlanNamedTypes::writeLimit, PlanNamedTypes::readLimit),
            of(LogicalPlan.class, LocalRelation.ENTRY),
            of(LogicalPlan.class, Lookup.ENTRY),
            of(LogicalPlan.class, MvExpand.class, PlanNamedTypes::writeMvExpand, PlanNamedTypes::readMvExpand),
            of(LogicalPlan.class, OrderBy.class, PlanNamedTypes::writeOrderBy, PlanNamedTypes::readOrderBy),
            of(LogicalPlan.class, Project.class, PlanNamedTypes::writeProject, PlanNamedTypes::readProject),
            of(LogicalPlan.class, TopN.ENTRY)
        );
        return declared;
    }

    // -- physical plan nodes
    static AggregateExec readAggregateExec(PlanStreamInput in) throws IOException {
        return new AggregateExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            in.readEnum(AggregateExec.Mode.class),
            in.readOptionalVInt()
        );
    }

    static void writeAggregateExec(PlanStreamOutput out, AggregateExec aggregateExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(aggregateExec.child());
        out.writeNamedWriteableCollection(aggregateExec.groupings());
        out.writeNamedWriteableCollection(aggregateExec.aggregates());
        out.writeEnum(aggregateExec.getMode());
        out.writeOptionalVInt(aggregateExec.estimatedRowSize());
    }

    static DissectExec readDissectExec(PlanStreamInput in) throws IOException {
        return new DissectExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readNamedWriteable(Expression.class),
            readDissectParser(in),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    static void writeDissectExec(PlanStreamOutput out, DissectExec dissectExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(dissectExec.child());
        out.writeNamedWriteable(dissectExec.inputExpression());
        writeDissectParser(out, dissectExec.parser());
        out.writeNamedWriteableCollection(dissectExec.extractedFields());
    }

    static EsQueryExec readEsQueryExec(PlanStreamInput in) throws IOException {
        return new EsQueryExec(
            Source.readFrom(in),
            readEsIndex(in),
            readIndexMode(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readOptionalNamed(Expression.class),
            in.readOptionalCollectionAsList(readerFromPlanReader(PlanNamedTypes::readFieldSort)),
            in.readOptionalVInt()
        );
    }

    static void writeEsQueryExec(PlanStreamOutput out, EsQueryExec esQueryExec) throws IOException {
        assert esQueryExec.children().size() == 0;
        Source.EMPTY.writeTo(out);
        writeEsIndex(out, esQueryExec.index());
        writeIndexMode(out, esQueryExec.indexMode());
        out.writeNamedWriteableCollection(esQueryExec.output());
        out.writeOptionalNamedWriteable(esQueryExec.query());
        out.writeOptionalNamedWriteable(esQueryExec.limit());
        out.writeOptionalCollection(esQueryExec.sorts(), writerFromPlanWriter(PlanNamedTypes::writeFieldSort));
        out.writeOptionalInt(esQueryExec.estimatedRowSize());
    }

    static EsSourceExec readEsSourceExec(PlanStreamInput in) throws IOException {
        return new EsSourceExec(
            Source.readFrom(in),
            readEsIndex(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            readIndexMode(in)
        );
    }

    static void writeEsSourceExec(PlanStreamOutput out, EsSourceExec esSourceExec) throws IOException {
        Source.EMPTY.writeTo(out);
        writeEsIndex(out, esSourceExec.index());
        out.writeNamedWriteableCollection(esSourceExec.output());
        out.writeOptionalNamedWriteable(esSourceExec.query());
        writeIndexMode(out, esSourceExec.indexMode());
    }

    static IndexMode readIndexMode(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_ADD_INDEX_MODE_TO_SOURCE)) {
            return IndexMode.fromString(in.readString());
        } else {
            return IndexMode.STANDARD;
        }
    }

    static void writeIndexMode(StreamOutput out, IndexMode indexMode) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ADD_INDEX_MODE_TO_SOURCE)) {
            out.writeString(indexMode.getName());
        } else if (indexMode != IndexMode.STANDARD) {
            throw new IllegalStateException("not ready to support index mode [" + indexMode + "]");
        }
    }

    static EvalExec readEvalExec(PlanStreamInput in) throws IOException {
        return new EvalExec(Source.readFrom(in), in.readPhysicalPlanNode(), in.readCollectionAsList(Alias::new));
    }

    static void writeEvalExec(PlanStreamOutput out, EvalExec evalExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(evalExec.child());
        out.writeCollection(evalExec.fields());
    }

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
            EsIndex esIndex = readEsIndex(in);
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
                writeEsIndex(out, new EsIndex(concreteIndex, Map.of(), Set.of(concreteIndex)));
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
            in.readLogicalPlanNode(),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readOptionalVInt(),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readOptionalPhysicalPlanNode() : null
        );
    }

    static void writeFragmentExec(PlanStreamOutput out, FragmentExec fragmentExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(fragmentExec.fragment());
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

    static Dissect readDissect(PlanStreamInput in) throws IOException {
        return new Dissect(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readNamedWriteable(Expression.class),
            readDissectParser(in),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    static void writeDissect(PlanStreamOutput out, Dissect dissect) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(dissect.child());
        out.writeNamedWriteable(dissect.input());
        writeDissectParser(out, dissect.parser());
        out.writeNamedWriteableCollection(dissect.extractedFields());
    }

    static EsRelation readEsRelation(PlanStreamInput in) throws IOException {
        Source source = Source.readFrom(in);
        EsIndex esIndex = readEsIndex(in);
        List<Attribute> attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        if (supportingEsSourceOptions(in.getTransportVersion())) {
            readEsSourceOptions(in); // consume optional strings sent by remote
        }
        final IndexMode indexMode = readIndexMode(in);
        boolean frozen = in.readBoolean();
        return new EsRelation(source, esIndex, attributes, indexMode, frozen);
    }

    static void writeEsRelation(PlanStreamOutput out, EsRelation relation) throws IOException {
        assert relation.children().size() == 0;
        Source.EMPTY.writeTo(out);
        writeEsIndex(out, relation.index());
        out.writeNamedWriteableCollection(relation.output());
        if (supportingEsSourceOptions(out.getTransportVersion())) {
            writeEsSourceOptions(out); // write (null) string fillers expected by remote
        }
        writeIndexMode(out, relation.indexMode());
        out.writeBoolean(relation.frozen());
    }

    private static boolean supportingEsSourceOptions(TransportVersion version) {
        return version.onOrAfter(TransportVersions.V_8_14_0) && version.before(TransportVersions.ESQL_REMOVE_ES_SOURCE_OPTIONS);
    }

    private static void readEsSourceOptions(PlanStreamInput in) throws IOException {
        // allowNoIndices
        in.readOptionalString();
        // ignoreUnavailable
        in.readOptionalString();
        // preference
        in.readOptionalString();
    }

    private static void writeEsSourceOptions(PlanStreamOutput out) throws IOException {
        // allowNoIndices
        out.writeOptionalString(null);
        // ignoreUnavailable
        out.writeOptionalString(null);
        // preference
        out.writeOptionalString(null);
    }

    static Eval readEval(PlanStreamInput in) throws IOException {
        return new Eval(Source.readFrom(in), in.readLogicalPlanNode(), in.readCollectionAsList(Alias::new));
    }

    static void writeEval(PlanStreamOutput out, Eval eval) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(eval.child());
        out.writeCollection(eval.fields());
    }

    static Enrich readEnrich(PlanStreamInput in) throws IOException {
        Enrich.Mode mode = Enrich.Mode.ANY;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            mode = in.readEnum(Enrich.Mode.class);
        }
        final Source source = Source.readFrom(in);
        final LogicalPlan child = in.readLogicalPlanNode();
        final Expression policyName = in.readNamedWriteable(Expression.class);
        final NamedExpression matchField = in.readNamedWriteable(NamedExpression.class);
        if (in.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            in.readString(); // discard the old policy name
        }
        final EnrichPolicy policy = new EnrichPolicy(in);
        final Map<String, String> concreteIndices;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            concreteIndices = in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            EsIndex esIndex = readEsIndex(in);
            if (esIndex.concreteIndices().size() > 1) {
                throw new IllegalStateException("expected a single enrich index; got " + esIndex);
            }
            concreteIndices = Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, Iterables.get(esIndex.concreteIndices(), 0));
        }
        return new Enrich(
            source,
            child,
            mode,
            policyName,
            matchField,
            policy,
            concreteIndices,
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    static void writeEnrich(PlanStreamOutput out, Enrich enrich) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeEnum(enrich.mode());
        }

        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(enrich.child());
        out.writeNamedWriteable(enrich.policyName());
        out.writeNamedWriteable(enrich.matchField());
        if (out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            out.writeString(BytesRefs.toString(enrich.policyName().fold())); // old policy name
        }
        enrich.policy().writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeMap(enrich.concreteIndices(), StreamOutput::writeString, StreamOutput::writeString);
        } else {
            Map<String, String> concreteIndices = enrich.concreteIndices();
            if (concreteIndices.keySet().equals(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))) {
                String enrichIndex = concreteIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                EsIndex esIndex = new EsIndex(enrichIndex, Map.of(), Set.of(enrichIndex));
                writeEsIndex(out, esIndex);
            } else {
                throw new IllegalStateException("expected a single enrich index; got " + concreteIndices);
            }
        }
        out.writeNamedWriteableCollection(enrich.enrichFields());
    }

    static EsqlProject readEsqlProject(PlanStreamInput in) throws IOException {
        return new EsqlProject(Source.readFrom(in), in.readLogicalPlanNode(), in.readNamedWriteableCollectionAsList(NamedExpression.class));
    }

    static void writeEsqlProject(PlanStreamOutput out, EsqlProject project) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(project.child());
        out.writeNamedWriteableCollection(project.projections());
    }

    static Filter readFilter(PlanStreamInput in) throws IOException {
        return new Filter(Source.readFrom(in), in.readLogicalPlanNode(), in.readNamedWriteable(Expression.class));
    }

    static void writeFilter(PlanStreamOutput out, Filter filter) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(filter.child());
        out.writeNamedWriteable(filter.condition());
    }

    static Grok readGrok(PlanStreamInput in) throws IOException {
        Source source;
        return new Grok(
            source = Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readNamedWriteable(Expression.class),
            Grok.pattern(source, in.readString()),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    static void writeGrok(PlanStreamOutput out, Grok grok) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(grok.child());
        out.writeNamedWriteable(grok.input());
        out.writeString(grok.parser().pattern());
        out.writeNamedWriteableCollection(grok.extractedFields());
    }

    static Limit readLimit(PlanStreamInput in) throws IOException {
        return new Limit(Source.readFrom(in), in.readNamedWriteable(Expression.class), in.readLogicalPlanNode());
    }

    static void writeLimit(PlanStreamOutput out, Limit limit) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(limit.limit());
        out.writeLogicalPlanNode(limit.child());
    }

    static MvExpand readMvExpand(PlanStreamInput in) throws IOException {
        return new MvExpand(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readNamedWriteable(NamedExpression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    static void writeMvExpand(PlanStreamOutput out, MvExpand mvExpand) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(mvExpand.child());
        out.writeNamedWriteable(mvExpand.target());
        out.writeNamedWriteable(mvExpand.expanded());
    }

    static OrderBy readOrderBy(PlanStreamInput in) throws IOException {
        return new OrderBy(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readCollectionAsList(org.elasticsearch.xpack.esql.expression.Order::new)
        );
    }

    static void writeOrderBy(PlanStreamOutput out, OrderBy order) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(order.child());
        out.writeCollection(order.order());
    }

    static Project readProject(PlanStreamInput in) throws IOException {
        return new Project(Source.readFrom(in), in.readLogicalPlanNode(), in.readNamedWriteableCollectionAsList(NamedExpression.class));
    }

    static void writeProject(PlanStreamOutput out, Project project) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(project.child());
        out.writeNamedWriteableCollection(project.projections());
    }

    // -- ancillary supporting classes of plan nodes, etc

    static EsQueryExec.FieldSort readFieldSort(PlanStreamInput in) throws IOException {
        return new EsQueryExec.FieldSort(
            FieldAttribute.readFrom(in),
            in.readEnum(Order.OrderDirection.class),
            in.readEnum(Order.NullsPosition.class)
        );
    }

    static void writeFieldSort(PlanStreamOutput out, EsQueryExec.FieldSort fieldSort) throws IOException {
        fieldSort.field().writeTo(out);
        out.writeEnum(fieldSort.direction());
        out.writeEnum(fieldSort.nulls());
    }

    @SuppressWarnings("unchecked")
    static EsIndex readEsIndex(PlanStreamInput in) throws IOException {
        return new EsIndex(
            in.readString(),
            in.readImmutableMap(StreamInput::readString, i -> i.readNamedWriteable(EsField.class)),
            (Set<String>) in.readGenericValue()
        );
    }

    static void writeEsIndex(PlanStreamOutput out, EsIndex esIndex) throws IOException {
        out.writeString(esIndex.name());
        out.writeMap(esIndex.mapping(), StreamOutput::writeNamedWriteable);
        out.writeGenericValue(esIndex.concreteIndices());
    }

    static Parser readDissectParser(PlanStreamInput in) throws IOException {
        String pattern = in.readString();
        String appendSeparator = in.readString();
        return new Parser(pattern, appendSeparator, new DissectParser(pattern, appendSeparator));
    }

    static void writeDissectParser(PlanStreamOutput out, Parser dissectParser) throws IOException {
        out.writeString(dissectParser.pattern());
        out.writeString(dissectParser.appendSeparator());
    }
}
