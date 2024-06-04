/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.TriFunction;
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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Order;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.plan.logical.Filter;
import org.elasticsearch.xpack.esql.core.plan.logical.Limit;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIP;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersion;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateDiff;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Now;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.IpPrefix;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan2;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cbrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.E;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pi;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Signum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tau;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvConcat;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDedupe;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvFirst;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLast;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSort;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvZip;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialDisjoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StX;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StY;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Left;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Locate;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Right;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Split;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Dissect.Parser;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
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
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
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
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
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

    static final Class<org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction> QL_UNARY_SCLR_CLS =
        org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction.class;

    static final Class<UnaryScalarFunction> ESQL_UNARY_SCLR_CLS = UnaryScalarFunction.class;

    /**
     * List of named type entries that link concrete names to stream reader and writer implementations.
     * Entries have the form:  category,  name,  serializer method,  deserializer method.
     */
    public static List<PlanNameRegistry.Entry> namedTypeEntries() {
        return List.of(
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
            of(PhysicalPlan.class, MvExpandExec.class, PlanNamedTypes::writeMvExpandExec, PlanNamedTypes::readMvExpandExec),
            of(PhysicalPlan.class, OrderExec.class, PlanNamedTypes::writeOrderExec, PlanNamedTypes::readOrderExec),
            of(PhysicalPlan.class, ProjectExec.class, PlanNamedTypes::writeProjectExec, PlanNamedTypes::readProjectExec),
            of(PhysicalPlan.class, RowExec.class, PlanNamedTypes::writeRowExec, PlanNamedTypes::readRowExec),
            of(PhysicalPlan.class, ShowExec.class, PlanNamedTypes::writeShowExec, PlanNamedTypes::readShowExec),
            of(PhysicalPlan.class, TopNExec.class, PlanNamedTypes::writeTopNExec, PlanNamedTypes::readTopNExec),
            // Logical Plan Nodes - a subset of plans that end up being actually serialized
            of(LogicalPlan.class, Aggregate.class, PlanNamedTypes::writeAggregate, PlanNamedTypes::readAggregate),
            of(LogicalPlan.class, Dissect.class, PlanNamedTypes::writeDissect, PlanNamedTypes::readDissect),
            of(LogicalPlan.class, EsRelation.class, PlanNamedTypes::writeEsRelation, PlanNamedTypes::readEsRelation),
            of(LogicalPlan.class, Eval.class, PlanNamedTypes::writeEval, PlanNamedTypes::readEval),
            of(LogicalPlan.class, Enrich.class, PlanNamedTypes::writeEnrich, PlanNamedTypes::readEnrich),
            of(LogicalPlan.class, EsqlProject.class, PlanNamedTypes::writeEsqlProject, PlanNamedTypes::readEsqlProject),
            of(LogicalPlan.class, Filter.class, PlanNamedTypes::writeFilter, PlanNamedTypes::readFilter),
            of(LogicalPlan.class, Grok.class, PlanNamedTypes::writeGrok, PlanNamedTypes::readGrok),
            of(LogicalPlan.class, Limit.class, PlanNamedTypes::writeLimit, PlanNamedTypes::readLimit),
            of(LogicalPlan.class, MvExpand.class, PlanNamedTypes::writeMvExpand, PlanNamedTypes::readMvExpand),
            of(LogicalPlan.class, OrderBy.class, PlanNamedTypes::writeOrderBy, PlanNamedTypes::readOrderBy),
            of(LogicalPlan.class, Project.class, PlanNamedTypes::writeProject, PlanNamedTypes::readProject),
            of(LogicalPlan.class, TopN.class, PlanNamedTypes::writeTopN, PlanNamedTypes::readTopN),
            // Attributes
            of(NamedExpression.class, FieldAttribute.class, (o, a) -> a.writeTo(o), FieldAttribute::new),
            of(NamedExpression.class, ReferenceAttribute.class, (o, a) -> a.writeTo(o), ReferenceAttribute::new),
            of(NamedExpression.class, MetadataAttribute.class, (o, a) -> a.writeTo(o), MetadataAttribute::new),
            of(NamedExpression.class, UnsupportedAttribute.class, (o, a) -> a.writeTo(o), UnsupportedAttribute::new),
            // NamedExpressions
            of(NamedExpression.class, Alias.class, PlanNamedTypes::writeAlias, PlanNamedTypes::readAlias),
            // BinaryComparison
            of(EsqlBinaryComparison.class, Equals.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(EsqlBinaryComparison.class, NotEquals.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(EsqlBinaryComparison.class, GreaterThan.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(EsqlBinaryComparison.class, GreaterThanOrEqual.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(EsqlBinaryComparison.class, LessThan.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(EsqlBinaryComparison.class, LessThanOrEqual.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            // InsensitiveEquals
            of(
                InsensitiveEquals.class,
                InsensitiveEquals.class,
                PlanNamedTypes::writeInsensitiveEquals,
                PlanNamedTypes::readInsensitiveEquals
            ),
            // InComparison
            of(ScalarFunction.class, In.class, PlanNamedTypes::writeInComparison, PlanNamedTypes::readInComparison),
            // RegexMatch
            of(RegexMatch.class, WildcardLike.class, PlanNamedTypes::writeWildcardLike, PlanNamedTypes::readWildcardLike),
            of(RegexMatch.class, RLike.class, PlanNamedTypes::writeRLike, PlanNamedTypes::readRLike),
            // BinaryLogic
            of(BinaryLogic.class, And.class, PlanNamedTypes::writeBinaryLogic, PlanNamedTypes::readBinaryLogic),
            of(BinaryLogic.class, Or.class, PlanNamedTypes::writeBinaryLogic, PlanNamedTypes::readBinaryLogic),
            // UnaryScalarFunction
            of(QL_UNARY_SCLR_CLS, IsNotNull.class, PlanNamedTypes::writeQLUnaryScalar, PlanNamedTypes::readQLUnaryScalar),
            of(QL_UNARY_SCLR_CLS, IsNull.class, PlanNamedTypes::writeQLUnaryScalar, PlanNamedTypes::readQLUnaryScalar),
            of(QL_UNARY_SCLR_CLS, Not.class, PlanNamedTypes::writeQLUnaryScalar, PlanNamedTypes::readQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Neg.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Abs.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Acos.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Asin.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Atan.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Cbrt.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Ceil.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Cos.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Cosh.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Floor.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, FromBase64.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Length.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Log10.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, LTrim.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, RTrim.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Signum.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Sin.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Sinh.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Sqrt.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, StX.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, StY.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Tan.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Tanh.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToBase64.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToBoolean.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToCartesianPoint.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToDatetime.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToDegrees.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToDouble.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToGeoShape.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToCartesianShape.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToGeoPoint.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToIP.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToInteger.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToLong.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToRadians.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToString.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToUnsignedLong.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToVersion.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Trim.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            // ScalarFunction
            of(ScalarFunction.class, Atan2.class, PlanNamedTypes::writeAtan2, PlanNamedTypes::readAtan2),
            of(ScalarFunction.class, Case.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, CIDRMatch.class, PlanNamedTypes::writeCIDRMatch, PlanNamedTypes::readCIDRMatch),
            of(ScalarFunction.class, Coalesce.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, Concat.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, DateDiff.class, PlanNamedTypes::writeDateDiff, PlanNamedTypes::readDateDiff),
            of(ScalarFunction.class, DateExtract.class, PlanNamedTypes::writeDateExtract, PlanNamedTypes::readDateExtract),
            of(ScalarFunction.class, DateFormat.class, PlanNamedTypes::writeDateFormat, PlanNamedTypes::readDateFormat),
            of(ScalarFunction.class, DateParse.class, PlanNamedTypes::writeDateTimeParse, PlanNamedTypes::readDateTimeParse),
            of(ScalarFunction.class, DateTrunc.class, PlanNamedTypes::writeDateTrunc, PlanNamedTypes::readDateTrunc),
            of(ScalarFunction.class, E.class, PlanNamedTypes::writeNoArgScalar, PlanNamedTypes::readNoArgScalar),
            of(ScalarFunction.class, Greatest.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, IpPrefix.class, (out, prefix) -> prefix.writeTo(out), IpPrefix::readFrom),
            of(ScalarFunction.class, Least.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, Log.class, PlanNamedTypes::writeLog, PlanNamedTypes::readLog),
            of(ScalarFunction.class, Now.class, PlanNamedTypes::writeNow, PlanNamedTypes::readNow),
            of(ScalarFunction.class, Pi.class, PlanNamedTypes::writeNoArgScalar, PlanNamedTypes::readNoArgScalar),
            of(ScalarFunction.class, Round.class, PlanNamedTypes::writeRound, PlanNamedTypes::readRound),
            of(ScalarFunction.class, Pow.class, PlanNamedTypes::writePow, PlanNamedTypes::readPow),
            of(ScalarFunction.class, StartsWith.class, PlanNamedTypes::writeStartsWith, PlanNamedTypes::readStartsWith),
            of(ScalarFunction.class, EndsWith.class, PlanNamedTypes::writeEndsWith, PlanNamedTypes::readEndsWith),
            of(ScalarFunction.class, SpatialIntersects.class, PlanNamedTypes::writeSpatialRelatesFunction, PlanNamedTypes::readIntersects),
            of(ScalarFunction.class, SpatialDisjoint.class, PlanNamedTypes::writeSpatialRelatesFunction, PlanNamedTypes::readDisjoint),
            of(ScalarFunction.class, SpatialContains.class, PlanNamedTypes::writeSpatialRelatesFunction, PlanNamedTypes::readContains),
            of(ScalarFunction.class, SpatialWithin.class, PlanNamedTypes::writeSpatialRelatesFunction, PlanNamedTypes::readWithin),
            of(ScalarFunction.class, Substring.class, PlanNamedTypes::writeSubstring, PlanNamedTypes::readSubstring),
            of(ScalarFunction.class, Locate.class, PlanNamedTypes::writeLocate, PlanNamedTypes::readLocate),
            of(ScalarFunction.class, Left.class, PlanNamedTypes::writeLeft, PlanNamedTypes::readLeft),
            of(ScalarFunction.class, Right.class, PlanNamedTypes::writeRight, PlanNamedTypes::readRight),
            of(ScalarFunction.class, Split.class, PlanNamedTypes::writeSplit, PlanNamedTypes::readSplit),
            of(ScalarFunction.class, Tau.class, PlanNamedTypes::writeNoArgScalar, PlanNamedTypes::readNoArgScalar),
            of(ScalarFunction.class, Replace.class, PlanNamedTypes::writeReplace, PlanNamedTypes::readReplace),
            of(ScalarFunction.class, ToLower.class, PlanNamedTypes::writeToLower, PlanNamedTypes::readToLower),
            of(ScalarFunction.class, ToUpper.class, PlanNamedTypes::writeToUpper, PlanNamedTypes::readToUpper),
            // ArithmeticOperations
            of(ArithmeticOperation.class, Add.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            of(ArithmeticOperation.class, Sub.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            of(ArithmeticOperation.class, Mul.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            of(ArithmeticOperation.class, Div.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            of(ArithmeticOperation.class, Mod.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            // GroupingFunctions
            of(GroupingFunction.class, Bucket.class, PlanNamedTypes::writeBucket, PlanNamedTypes::readBucket),
            // AggregateFunctions
            of(AggregateFunction.class, Avg.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Count.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, CountDistinct.class, PlanNamedTypes::writeCountDistinct, PlanNamedTypes::readCountDistinct),
            of(AggregateFunction.class, Min.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Max.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Median.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, MedianAbsoluteDeviation.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Percentile.class, PlanNamedTypes::writePercentile, PlanNamedTypes::readPercentile),
            of(AggregateFunction.class, SpatialCentroid.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Sum.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Values.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            // Multivalue functions
            of(ScalarFunction.class, MvAvg.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvCount.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvConcat.class, PlanNamedTypes::writeMvConcat, PlanNamedTypes::readMvConcat),
            of(ScalarFunction.class, MvDedupe.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvFirst.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvLast.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvMax.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvMedian.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvMin.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvSort.class, PlanNamedTypes::writeMvSort, PlanNamedTypes::readMvSort),
            of(ScalarFunction.class, MvSlice.class, PlanNamedTypes::writeMvSlice, PlanNamedTypes::readMvSlice),
            of(ScalarFunction.class, MvSum.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvZip.class, PlanNamedTypes::writeMvZip, PlanNamedTypes::readMvZip),
            // Expressions (other)
            of(Expression.class, Literal.class, PlanNamedTypes::writeLiteral, PlanNamedTypes::readLiteral),
            of(Expression.class, Order.class, PlanNamedTypes::writeOrder, PlanNamedTypes::readOrder)
        );
    }

    // -- physical plan nodes
    static AggregateExec readAggregateExec(PlanStreamInput in) throws IOException {
        return new AggregateExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression)),
            readNamedExpressions(in),
            in.readEnum(AggregateExec.Mode.class),
            in.readOptionalVInt()
        );
    }

    static void writeAggregateExec(PlanStreamOutput out, AggregateExec aggregateExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(aggregateExec.child());
        out.writeCollection(aggregateExec.groupings(), writerFromPlanWriter(PlanStreamOutput::writeExpression));
        writeNamedExpressions(out, aggregateExec.aggregates());
        out.writeEnum(aggregateExec.getMode());
        out.writeOptionalVInt(aggregateExec.estimatedRowSize());
    }

    static DissectExec readDissectExec(PlanStreamInput in) throws IOException {
        return new DissectExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readExpression(),
            readDissectParser(in),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    static void writeDissectExec(PlanStreamOutput out, DissectExec dissectExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(dissectExec.child());
        out.writeExpression(dissectExec.inputExpression());
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
        out.writeOptionalExpression(esQueryExec.limit());
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
        return new EvalExec(Source.readFrom(in), in.readPhysicalPlanNode(), readAliases(in));
    }

    static void writeEvalExec(PlanStreamOutput out, EvalExec evalExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(evalExec.child());
        writeAliases(out, evalExec.fields());
    }

    static EnrichExec readEnrichExec(PlanStreamInput in) throws IOException {
        final Source source = Source.readFrom(in);
        final PhysicalPlan child = in.readPhysicalPlanNode();
        final NamedExpression matchField = in.readNamedExpression();
        final String policyName = in.readString();
        final String matchType = (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_EXTENDED_ENRICH_TYPES))
            ? in.readString()
            : "match";
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
            readNamedExpressions(in)
        );
    }

    static void writeEnrichExec(PlanStreamOutput out, EnrichExec enrich) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(enrich.child());
        out.writeNamedExpression(enrich.matchField());
        out.writeString(enrich.policyName());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_EXTENDED_ENRICH_TYPES)) {
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
        writeNamedExpressions(out, enrich.enrichFields());
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
        return new FilterExec(Source.readFrom(in), in.readPhysicalPlanNode(), in.readExpression());
    }

    static void writeFilterExec(PlanStreamOutput out, FilterExec filterExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(filterExec.child());
        out.writeExpression(filterExec.condition());
    }

    static FragmentExec readFragmentExec(PlanStreamInput in) throws IOException {
        return new FragmentExec(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readOptionalVInt(),
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_REDUCER_NODE_FRAGMENT) ? in.readOptionalPhysicalPlanNode() : null
        );
    }

    static void writeFragmentExec(PlanStreamOutput out, FragmentExec fragmentExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(fragmentExec.fragment());
        out.writeOptionalNamedWriteable(fragmentExec.esFilter());
        out.writeOptionalVInt(fragmentExec.estimatedRowSize());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_REDUCER_NODE_FRAGMENT)) {
            out.writeOptionalPhysicalPlanNode(fragmentExec.reducer());
        }
    }

    static GrokExec readGrokExec(PlanStreamInput in) throws IOException {
        Source source;
        return new GrokExec(
            source = Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readExpression(),
            Grok.pattern(source, in.readString()),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    static void writeGrokExec(PlanStreamOutput out, GrokExec grokExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(grokExec.child());
        out.writeExpression(grokExec.inputExpression());
        out.writeString(grokExec.pattern().pattern());
        out.writeNamedWriteableCollection(grokExec.extractedFields());
    }

    static LimitExec readLimitExec(PlanStreamInput in) throws IOException {
        return new LimitExec(Source.readFrom(in), in.readPhysicalPlanNode(), in.readNamed(Expression.class));
    }

    static void writeLimitExec(PlanStreamOutput out, LimitExec limitExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(limitExec.child());
        out.writeExpression(limitExec.limit());
    }

    static MvExpandExec readMvExpandExec(PlanStreamInput in) throws IOException {
        return new MvExpandExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readNamedExpression(),
            in.readNamedWriteable(Attribute.class)
        );
    }

    static void writeMvExpandExec(PlanStreamOutput out, MvExpandExec mvExpandExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(mvExpandExec.child());
        out.writeNamedExpression(mvExpandExec.target());
        out.writeNamedWriteable(mvExpandExec.expanded());
    }

    static OrderExec readOrderExec(PlanStreamInput in) throws IOException {
        return new OrderExec(
            Source.readFrom(in),
            in.readPhysicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readOrder))
        );
    }

    static void writeOrderExec(PlanStreamOutput out, OrderExec orderExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(orderExec.child());
        out.writeCollection(orderExec.order(), writerFromPlanWriter(PlanNamedTypes::writeOrder));
    }

    static ProjectExec readProjectExec(PlanStreamInput in) throws IOException {
        return new ProjectExec(Source.readFrom(in), in.readPhysicalPlanNode(), readNamedExpressions(in));
    }

    static void writeProjectExec(PlanStreamOutput out, ProjectExec projectExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(projectExec.child());
        writeNamedExpressions(out, projectExec.projections());
    }

    static RowExec readRowExec(PlanStreamInput in) throws IOException {
        return new RowExec(Source.readFrom(in), readAliases(in));
    }

    static void writeRowExec(PlanStreamOutput out, RowExec rowExec) throws IOException {
        assert rowExec.children().size() == 0;
        Source.EMPTY.writeTo(out);
        writeAliases(out, rowExec.fields());
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
            in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readOrder)),
            in.readNamed(Expression.class),
            in.readOptionalVInt()
        );
    }

    static void writeTopNExec(PlanStreamOutput out, TopNExec topNExec) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writePhysicalPlanNode(topNExec.child());
        out.writeCollection(topNExec.order(), writerFromPlanWriter(PlanNamedTypes::writeOrder));
        out.writeExpression(topNExec.limit());
        out.writeOptionalVInt(topNExec.estimatedRowSize());
    }

    // -- Logical plan nodes
    static Aggregate readAggregate(PlanStreamInput in) throws IOException {
        return new Aggregate(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression)),
            readNamedExpressions(in)
        );
    }

    static void writeAggregate(PlanStreamOutput out, Aggregate aggregate) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(aggregate.child());
        out.writeCollection(aggregate.groupings(), writerFromPlanWriter(PlanStreamOutput::writeExpression));
        writeNamedExpressions(out, aggregate.aggregates());
    }

    static Dissect readDissect(PlanStreamInput in) throws IOException {
        return new Dissect(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readExpression(),
            readDissectParser(in),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    static void writeDissect(PlanStreamOutput out, Dissect dissect) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(dissect.child());
        out.writeExpression(dissect.input());
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
        return version.onOrAfter(TransportVersions.ESQL_ES_SOURCE_OPTIONS)
            && version.before(TransportVersions.ESQL_REMOVE_ES_SOURCE_OPTIONS);
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
        return new Eval(Source.readFrom(in), in.readLogicalPlanNode(), readAliases(in));
    }

    static void writeEval(PlanStreamOutput out, Eval eval) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(eval.child());
        writeAliases(out, eval.fields());
    }

    static Enrich readEnrich(PlanStreamInput in) throws IOException {
        Enrich.Mode mode = Enrich.Mode.ANY;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            mode = in.readEnum(Enrich.Mode.class);
        }
        final Source source = Source.readFrom(in);
        final LogicalPlan child = in.readLogicalPlanNode();
        final Expression policyName = in.readExpression();
        final NamedExpression matchField = in.readNamedExpression();
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
        return new Enrich(source, child, mode, policyName, matchField, policy, concreteIndices, readNamedExpressions(in));
    }

    static void writeEnrich(PlanStreamOutput out, Enrich enrich) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeEnum(enrich.mode());
        }

        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(enrich.child());
        out.writeExpression(enrich.policyName());
        out.writeNamedExpression(enrich.matchField());
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
        writeNamedExpressions(out, enrich.enrichFields());
    }

    static EsqlProject readEsqlProject(PlanStreamInput in) throws IOException {
        return new EsqlProject(Source.readFrom(in), in.readLogicalPlanNode(), readNamedExpressions(in));
    }

    static void writeEsqlProject(PlanStreamOutput out, EsqlProject project) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(project.child());
        writeNamedExpressions(out, project.projections());
    }

    static Filter readFilter(PlanStreamInput in) throws IOException {
        return new Filter(Source.readFrom(in), in.readLogicalPlanNode(), in.readExpression());
    }

    static void writeFilter(PlanStreamOutput out, Filter filter) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(filter.child());
        out.writeExpression(filter.condition());
    }

    static Grok readGrok(PlanStreamInput in) throws IOException {
        Source source;
        return new Grok(
            source = Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readExpression(),
            Grok.pattern(source, in.readString()),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    static void writeGrok(PlanStreamOutput out, Grok grok) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(grok.child());
        out.writeExpression(grok.input());
        out.writeString(grok.parser().pattern());
        out.writeNamedWriteableCollection(grok.extractedFields());
    }

    static Limit readLimit(PlanStreamInput in) throws IOException {
        return new Limit(Source.readFrom(in), in.readNamed(Expression.class), in.readLogicalPlanNode());
    }

    static void writeLimit(PlanStreamOutput out, Limit limit) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeExpression(limit.limit());
        out.writeLogicalPlanNode(limit.child());
    }

    static MvExpand readMvExpand(PlanStreamInput in) throws IOException {
        return new MvExpand(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readNamedExpression(),
            in.readNamedWriteable(Attribute.class)
        );
    }

    static void writeMvExpand(PlanStreamOutput out, MvExpand mvExpand) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(mvExpand.child());
        out.writeNamedExpression(mvExpand.target());
        out.writeNamedWriteable(mvExpand.expanded());
    }

    static OrderBy readOrderBy(PlanStreamInput in) throws IOException {
        return new OrderBy(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readOrder))
        );
    }

    static void writeOrderBy(PlanStreamOutput out, OrderBy order) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(order.child());
        out.writeCollection(order.order(), writerFromPlanWriter(PlanNamedTypes::writeOrder));
    }

    static Project readProject(PlanStreamInput in) throws IOException {
        return new Project(Source.readFrom(in), in.readLogicalPlanNode(), readNamedExpressions(in));
    }

    static void writeProject(PlanStreamOutput out, Project project) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(project.child());
        writeNamedExpressions(out, project.projections());
    }

    static TopN readTopN(PlanStreamInput in) throws IOException {
        return new TopN(
            Source.readFrom(in),
            in.readLogicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readOrder)),
            in.readNamed(Expression.class)
        );
    }

    static void writeTopN(PlanStreamOutput out, TopN topN) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeLogicalPlanNode(topN.child());
        out.writeCollection(topN.order(), writerFromPlanWriter(PlanNamedTypes::writeOrder));
        out.writeExpression(topN.limit());
    }

    //
    // -- Attributes
    //

    private static List<NamedExpression> readNamedExpressions(PlanStreamInput in) throws IOException {
        return in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readNamedExpression));
    }

    static void writeNamedExpressions(PlanStreamOutput out, List<? extends NamedExpression> namedExpressions) throws IOException {
        out.writeCollection(namedExpressions, writerFromPlanWriter(PlanStreamOutput::writeNamedExpression));
    }

    private static List<Alias> readAliases(PlanStreamInput in) throws IOException {
        return in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readAlias));
    }

    static void writeAliases(PlanStreamOutput out, List<Alias> aliases) throws IOException {
        out.writeCollection(aliases, writerFromPlanWriter(PlanNamedTypes::writeAlias));
    }

    // -- BinaryComparison

    static EsqlBinaryComparison readBinComparison(PlanStreamInput in, String name) throws IOException {
        var source = Source.readFrom(in);
        EsqlBinaryComparison.BinaryComparisonOperation operation = EsqlBinaryComparison.BinaryComparisonOperation.readFromStream(in);
        var left = in.readExpression();
        var right = in.readExpression();
        // TODO: Remove zoneId entirely
        var zoneId = in.readOptionalZoneId();
        return operation.buildNewInstance(source, left, right);
    }

    static void writeBinComparison(PlanStreamOutput out, EsqlBinaryComparison binaryComparison) throws IOException {
        binaryComparison.source().writeTo(out);
        binaryComparison.getFunctionType().writeTo(out);
        out.writeExpression(binaryComparison.left());
        out.writeExpression(binaryComparison.right());
        out.writeOptionalZoneId(binaryComparison.zoneId());
    }

    // -- InsensitiveEquals
    static InsensitiveEquals readInsensitiveEquals(PlanStreamInput in, String name) throws IOException {
        var source = Source.readFrom(in);
        var left = in.readExpression();
        var right = in.readExpression();
        return new InsensitiveEquals(source, left, right);
    }

    static void writeInsensitiveEquals(PlanStreamOutput out, InsensitiveEquals eq) throws IOException {
        eq.source().writeTo(out);
        out.writeExpression(eq.left());
        out.writeExpression(eq.right());
    }

    // -- InComparison

    static In readInComparison(PlanStreamInput in) throws IOException {
        return new In(
            Source.readFrom(in),
            in.readExpression(),
            in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression))
        );
    }

    static void writeInComparison(PlanStreamOutput out, In in) throws IOException {
        in.source().writeTo(out);
        out.writeExpression(in.value());
        out.writeCollection(in.list(), writerFromPlanWriter(PlanStreamOutput::writeExpression));
    }

    // -- RegexMatch

    static WildcardLike readWildcardLike(PlanStreamInput in, String name) throws IOException {
        return new WildcardLike(Source.readFrom(in), in.readExpression(), new WildcardPattern(in.readString()));
    }

    static void writeWildcardLike(PlanStreamOutput out, WildcardLike like) throws IOException {
        like.source().writeTo(out);
        out.writeExpression(like.field());
        out.writeString(like.pattern().pattern());
    }

    static RLike readRLike(PlanStreamInput in, String name) throws IOException {
        return new RLike(Source.readFrom(in), in.readExpression(), new RLikePattern(in.readString()));
    }

    static void writeRLike(PlanStreamOutput out, RLike like) throws IOException {
        like.source().writeTo(out);
        out.writeExpression(like.field());
        out.writeString(like.pattern().asJavaRegex());
    }

    // -- BinaryLogic

    static final Map<String, TriFunction<Source, Expression, Expression, BinaryLogic>> BINARY_LOGIC_CTRS = Map.ofEntries(
        entry(name(And.class), And::new),
        entry(name(Or.class), Or::new)
    );

    static BinaryLogic readBinaryLogic(PlanStreamInput in, String name) throws IOException {
        var source = Source.readFrom(in);
        var left = in.readExpression();
        var right = in.readExpression();
        return BINARY_LOGIC_CTRS.get(name).apply(source, left, right);
    }

    static void writeBinaryLogic(PlanStreamOutput out, BinaryLogic binaryLogic) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeExpression(binaryLogic.left());
        out.writeExpression(binaryLogic.right());
    }

    // -- UnaryScalarFunction

    static final Map<String, BiFunction<Source, Expression, UnaryScalarFunction>> ESQL_UNARY_SCALAR_CTRS = Map.ofEntries(
        entry(name(Abs.class), Abs::new),
        entry(name(Acos.class), Acos::new),
        entry(name(Asin.class), Asin::new),
        entry(name(Atan.class), Atan::new),
        entry(name(Cbrt.class), Cbrt::new),
        entry(name(Ceil.class), Ceil::new),
        entry(name(Cos.class), Cos::new),
        entry(name(Cosh.class), Cosh::new),
        entry(name(Floor.class), Floor::new),
        entry(name(FromBase64.class), FromBase64::new),
        entry(name(Length.class), Length::new),
        entry(name(Log10.class), Log10::new),
        entry(name(LTrim.class), LTrim::new),
        entry(name(RTrim.class), RTrim::new),
        entry(name(Neg.class), Neg::new),
        entry(name(Signum.class), Signum::new),
        entry(name(Sin.class), Sin::new),
        entry(name(Sinh.class), Sinh::new),
        entry(name(Sqrt.class), Sqrt::new),
        entry(name(StX.class), StX::new),
        entry(name(StY.class), StY::new),
        entry(name(Tan.class), Tan::new),
        entry(name(Tanh.class), Tanh::new),
        entry(name(ToBase64.class), ToBase64::new),
        entry(name(ToBoolean.class), ToBoolean::new),
        entry(name(ToCartesianPoint.class), ToCartesianPoint::new),
        entry(name(ToDatetime.class), ToDatetime::new),
        entry(name(ToDegrees.class), ToDegrees::new),
        entry(name(ToDouble.class), ToDouble::new),
        entry(name(ToGeoShape.class), ToGeoShape::new),
        entry(name(ToCartesianShape.class), ToCartesianShape::new),
        entry(name(ToGeoPoint.class), ToGeoPoint::new),
        entry(name(ToIP.class), ToIP::new),
        entry(name(ToInteger.class), ToInteger::new),
        entry(name(ToLong.class), ToLong::new),
        entry(name(ToRadians.class), ToRadians::new),
        entry(name(ToString.class), ToString::new),
        entry(name(ToUnsignedLong.class), ToUnsignedLong::new),
        entry(name(ToVersion.class), ToVersion::new),
        entry(name(Trim.class), Trim::new)
    );

    static UnaryScalarFunction readESQLUnaryScalar(PlanStreamInput in, String name) throws IOException {
        var ctr = ESQL_UNARY_SCALAR_CTRS.get(name);
        if (ctr == null) {
            throw new IOException("Constructor for ESQLUnaryScalar not found for name:" + name);
        }
        return ctr.apply(Source.readFrom(in), in.readExpression());
    }

    static void writeESQLUnaryScalar(PlanStreamOutput out, UnaryScalarFunction function) throws IOException {
        function.source().writeTo(out);
        out.writeExpression(function.field());
    }

    static final Map<String, Function<Source, ScalarFunction>> NO_ARG_SCALAR_CTRS = Map.ofEntries(
        entry(name(E.class), E::new),
        entry(name(Pi.class), Pi::new),
        entry(name(Tau.class), Tau::new)
    );

    static ScalarFunction readNoArgScalar(PlanStreamInput in, String name) throws IOException {
        var ctr = NO_ARG_SCALAR_CTRS.get(name);
        if (ctr == null) {
            throw new IOException("Constructor not found:" + name);
        }
        return ctr.apply(Source.readFrom(in));
    }

    static void writeNoArgScalar(PlanStreamOutput out, ScalarFunction function) throws IOException {
        Source.EMPTY.writeTo(out);
    }

    static final Map<
        String,
        BiFunction<
            Source,
            Expression,
            org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction>> QL_UNARY_SCALAR_CTRS = Map.ofEntries(
                entry(name(IsNotNull.class), IsNotNull::new),
                entry(name(IsNull.class), IsNull::new),
                entry(name(Not.class), Not::new)
            );

    static org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction readQLUnaryScalar(
        PlanStreamInput in,
        String name
    ) throws IOException {
        var ctr = QL_UNARY_SCALAR_CTRS.get(name);
        if (ctr == null) {
            throw new IOException("Constructor for QLUnaryScalar not found for name:" + name);
        }
        return ctr.apply(Source.readFrom(in), in.readExpression());
    }

    static void writeQLUnaryScalar(
        PlanStreamOutput out,
        org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction function
    ) throws IOException {
        function.source().writeTo(out);
        out.writeExpression(function.field());
    }

    // -- ScalarFunction

    static Atan2 readAtan2(PlanStreamInput in) throws IOException {
        return new Atan2(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writeAtan2(PlanStreamOutput out, Atan2 atan2) throws IOException {
        atan2.source().writeTo(out);
        out.writeExpression(atan2.y());
        out.writeExpression(atan2.x());
    }

    static Bucket readBucket(PlanStreamInput in) throws IOException {
        return new Bucket(
            Source.readFrom(in),
            in.readExpression(),
            in.readExpression(),
            in.readOptionalNamed(Expression.class),
            in.readOptionalNamed(Expression.class)
        );
    }

    static void writeBucket(PlanStreamOutput out, Bucket bucket) throws IOException {
        bucket.source().writeTo(out);
        out.writeExpression(bucket.field());
        out.writeExpression(bucket.buckets());
        out.writeOptionalExpression(bucket.from());
        out.writeOptionalExpression(bucket.to());
    }

    static final Map<String, TriFunction<Source, Expression, List<Expression>, ScalarFunction>> VARARG_CTORS = Map.ofEntries(
        entry(name(Case.class), Case::new),
        entry(name(Coalesce.class), Coalesce::new),
        entry(name(Concat.class), Concat::new),
        entry(name(Greatest.class), Greatest::new),
        entry(name(Least.class), Least::new)
    );

    static ScalarFunction readVarag(PlanStreamInput in, String name) throws IOException {
        return VARARG_CTORS.get(name)
            .apply(
                Source.readFrom(in),
                in.readExpression(),
                in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression))
            );
    }

    static void writeVararg(PlanStreamOutput out, ScalarFunction vararg) throws IOException {
        vararg.source().writeTo(out);
        out.writeExpression(vararg.children().get(0));
        out.writeCollection(
            vararg.children().subList(1, vararg.children().size()),
            writerFromPlanWriter(PlanStreamOutput::writeExpression)
        );
    }

    static CountDistinct readCountDistinct(PlanStreamInput in) throws IOException {
        return new CountDistinct(Source.readFrom(in), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeCountDistinct(PlanStreamOutput out, CountDistinct countDistinct) throws IOException {
        List<Expression> fields = countDistinct.children();
        assert fields.size() == 1 || fields.size() == 2;
        Source.EMPTY.writeTo(out);
        out.writeExpression(fields.get(0));
        out.writeOptionalWriteable(fields.size() == 2 ? o -> out.writeExpression(fields.get(1)) : null);
    }

    static DateDiff readDateDiff(PlanStreamInput in) throws IOException {
        return new DateDiff(Source.readFrom(in), in.readExpression(), in.readExpression(), in.readExpression());
    }

    static void writeDateDiff(PlanStreamOutput out, DateDiff function) throws IOException {
        Source.EMPTY.writeTo(out);
        List<Expression> fields = function.children();
        assert fields.size() == 3;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
        out.writeExpression(fields.get(2));
    }

    static DateExtract readDateExtract(PlanStreamInput in) throws IOException {
        return new DateExtract(Source.readFrom(in), in.readExpression(), in.readExpression(), in.configuration());
    }

    static void writeDateExtract(PlanStreamOutput out, DateExtract function) throws IOException {
        function.source().writeTo(out);
        List<Expression> fields = function.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static DateFormat readDateFormat(PlanStreamInput in) throws IOException {
        return new DateFormat(Source.readFrom(in), in.readExpression(), in.readOptionalNamed(Expression.class), in.configuration());
    }

    static void writeDateFormat(PlanStreamOutput out, DateFormat dateFormat) throws IOException {
        dateFormat.source().writeTo(out);
        List<Expression> fields = dateFormat.children();
        assert fields.size() == 1 || fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeOptionalWriteable(fields.size() == 2 ? o -> out.writeExpression(fields.get(1)) : null);
    }

    static DateParse readDateTimeParse(PlanStreamInput in) throws IOException {
        return new DateParse(Source.readFrom(in), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeDateTimeParse(PlanStreamOutput out, DateParse function) throws IOException {
        function.source().writeTo(out);
        List<Expression> fields = function.children();
        assert fields.size() == 1 || fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeOptionalWriteable(fields.size() == 2 ? o -> out.writeExpression(fields.get(1)) : null);
    }

    static DateTrunc readDateTrunc(PlanStreamInput in) throws IOException {
        return new DateTrunc(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writeDateTrunc(PlanStreamOutput out, DateTrunc dateTrunc) throws IOException {
        dateTrunc.source().writeTo(out);
        List<Expression> fields = dateTrunc.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static SpatialIntersects readIntersects(PlanStreamInput in) throws IOException {
        return new SpatialIntersects(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static SpatialDisjoint readDisjoint(PlanStreamInput in) throws IOException {
        return new SpatialDisjoint(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static SpatialContains readContains(PlanStreamInput in) throws IOException {
        return new SpatialContains(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static SpatialWithin readWithin(PlanStreamInput in) throws IOException {
        return new SpatialWithin(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeSpatialRelatesFunction(PlanStreamOutput out, SpatialRelatesFunction spatialRelatesFunction) throws IOException {
        out.writeExpression(spatialRelatesFunction.left());
        out.writeExpression(spatialRelatesFunction.right());
    }

    static Now readNow(PlanStreamInput in) throws IOException {
        return new Now(Source.readFrom(in), in.configuration());
    }

    static void writeNow(PlanStreamOutput out, Now function) throws IOException {
        Source.EMPTY.writeTo(out);
    }

    static Round readRound(PlanStreamInput in) throws IOException {
        return new Round(Source.readFrom(in), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeRound(PlanStreamOutput out, Round round) throws IOException {
        round.source().writeTo(out);
        out.writeExpression(round.field());
        out.writeOptionalExpression(round.decimals());
    }

    static Pow readPow(PlanStreamInput in) throws IOException {
        return new Pow(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writePow(PlanStreamOutput out, Pow pow) throws IOException {
        pow.source().writeTo(out);
        out.writeExpression(pow.base());
        out.writeExpression(pow.exponent());
    }

    static Percentile readPercentile(PlanStreamInput in) throws IOException {
        return new Percentile(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writePercentile(PlanStreamOutput out, Percentile percentile) throws IOException {
        List<Expression> fields = percentile.children();
        assert fields.size() == 2 : "percentile() aggregation must have two arguments";
        Source.EMPTY.writeTo(out);
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static StartsWith readStartsWith(PlanStreamInput in) throws IOException {
        return new StartsWith(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writeStartsWith(PlanStreamOutput out, StartsWith startsWith) throws IOException {
        startsWith.source().writeTo(out);
        List<Expression> fields = startsWith.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static EndsWith readEndsWith(PlanStreamInput in) throws IOException {
        return new EndsWith(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writeEndsWith(PlanStreamOutput out, EndsWith endsWith) throws IOException {
        List<Expression> fields = endsWith.children();
        assert fields.size() == 2;
        Source.EMPTY.writeTo(out);
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static Substring readSubstring(PlanStreamInput in) throws IOException {
        return new Substring(Source.readFrom(in), in.readExpression(), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeSubstring(PlanStreamOutput out, Substring substring) throws IOException {
        substring.source().writeTo(out);
        List<Expression> fields = substring.children();
        assert fields.size() == 2 || fields.size() == 3;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
        out.writeOptionalWriteable(fields.size() == 3 ? o -> out.writeExpression(fields.get(2)) : null);
    }

    static Locate readLocate(PlanStreamInput in) throws IOException {
        return new Locate(Source.readFrom(in), in.readExpression(), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeLocate(PlanStreamOutput out, Locate locate) throws IOException {
        locate.source().writeTo(out);
        List<Expression> fields = locate.children();
        assert fields.size() == 2 || fields.size() == 3;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
        out.writeOptionalWriteable(fields.size() == 3 ? o -> out.writeExpression(fields.get(2)) : null);
    }

    static Replace readReplace(PlanStreamInput in) throws IOException {
        return new Replace(Source.EMPTY, in.readExpression(), in.readExpression(), in.readExpression());
    }

    static void writeReplace(PlanStreamOutput out, Replace replace) throws IOException {
        List<Expression> fields = replace.children();
        assert fields.size() == 3;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
        out.writeExpression(fields.get(2));
    }

    static ToLower readToLower(PlanStreamInput in) throws IOException {
        return new ToLower(Source.EMPTY, in.readExpression(), in.configuration());
    }

    static void writeToLower(PlanStreamOutput out, ToLower toLower) throws IOException {
        out.writeExpression(toLower.field());
    }

    static ToUpper readToUpper(PlanStreamInput in) throws IOException {
        return new ToUpper(Source.EMPTY, in.readExpression(), in.configuration());
    }

    static void writeToUpper(PlanStreamOutput out, ToUpper toUpper) throws IOException {
        out.writeExpression(toUpper.field());
    }

    static Left readLeft(PlanStreamInput in) throws IOException {
        return new Left(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writeLeft(PlanStreamOutput out, Left left) throws IOException {
        left.source().writeTo(out);
        List<Expression> fields = left.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static Right readRight(PlanStreamInput in) throws IOException {
        return new Right(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writeRight(PlanStreamOutput out, Right right) throws IOException {
        right.source().writeTo(out);
        List<Expression> fields = right.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static Split readSplit(PlanStreamInput in) throws IOException {
        return new Split(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writeSplit(PlanStreamOutput out, Split split) throws IOException {
        split.source().writeTo(out);
        out.writeExpression(split.left());
        out.writeExpression(split.right());
    }

    static CIDRMatch readCIDRMatch(PlanStreamInput in) throws IOException {
        return new CIDRMatch(
            Source.readFrom(in),
            in.readExpression(),
            in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression))
        );
    }

    static void writeCIDRMatch(PlanStreamOutput out, CIDRMatch cidrMatch) throws IOException {
        cidrMatch.source().writeTo(out);
        List<Expression> children = cidrMatch.children();
        assert children.size() > 1;
        out.writeExpression(children.get(0));
        out.writeCollection(children.subList(1, children.size()), writerFromPlanWriter(PlanStreamOutput::writeExpression));
    }

    // -- ArithmeticOperations

    static final Map<String, TriFunction<Source, Expression, Expression, ArithmeticOperation>> ARITHMETIC_CTRS = Map.ofEntries(
        entry(name(Add.class), Add::new),
        entry(name(Sub.class), Sub::new),
        entry(name(Mul.class), Mul::new),
        entry(name(Div.class), Div::new),
        entry(name(Mod.class), Mod::new)
    );

    static ArithmeticOperation readArithmeticOperation(PlanStreamInput in, String name) throws IOException {
        var source = Source.readFrom(in);
        var left = in.readExpression();
        var right = in.readExpression();
        return ARITHMETIC_CTRS.get(name).apply(source, left, right);
    }

    static void writeArithmeticOperation(PlanStreamOutput out, ArithmeticOperation arithmeticOperation) throws IOException {
        arithmeticOperation.source().writeTo(out);
        out.writeExpression(arithmeticOperation.left());
        out.writeExpression(arithmeticOperation.right());
    }

    // -- Aggregations
    static final Map<String, BiFunction<Source, Expression, AggregateFunction>> AGG_CTRS = Map.ofEntries(
        entry(name(Avg.class), Avg::new),
        entry(name(Count.class), Count::new),
        entry(name(Sum.class), Sum::new),
        entry(name(Min.class), Min::new),
        entry(name(Max.class), Max::new),
        entry(name(Median.class), Median::new),
        entry(name(MedianAbsoluteDeviation.class), MedianAbsoluteDeviation::new),
        entry(name(SpatialCentroid.class), SpatialCentroid::new),
        entry(name(Values.class), Values::new)
    );

    static AggregateFunction readAggFunction(PlanStreamInput in, String name) throws IOException {
        return AGG_CTRS.get(name).apply(Source.readFrom(in), in.readExpression());
    }

    static void writeAggFunction(PlanStreamOutput out, AggregateFunction aggregateFunction) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeExpression(aggregateFunction.field());
    }

    // -- Multivalue functions
    static final Map<String, BiFunction<Source, Expression, AbstractMultivalueFunction>> MV_CTRS = Map.ofEntries(
        entry(name(MvAvg.class), MvAvg::new),
        entry(name(MvCount.class), MvCount::new),
        entry(name(MvDedupe.class), MvDedupe::new),
        entry(name(MvFirst.class), MvFirst::new),
        entry(name(MvLast.class), MvLast::new),
        entry(name(MvMax.class), MvMax::new),
        entry(name(MvMedian.class), MvMedian::new),
        entry(name(MvMin.class), MvMin::new),
        entry(name(MvSum.class), MvSum::new)
    );

    static AbstractMultivalueFunction readMvFunction(PlanStreamInput in, String name) throws IOException {
        return MV_CTRS.get(name).apply(Source.readFrom(in), in.readExpression());
    }

    static void writeMvFunction(PlanStreamOutput out, AbstractMultivalueFunction fn) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeExpression(fn.field());
    }

    static MvConcat readMvConcat(PlanStreamInput in) throws IOException {
        return new MvConcat(Source.readFrom(in), in.readExpression(), in.readExpression());
    }

    static void writeMvConcat(PlanStreamOutput out, MvConcat fn) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeExpression(fn.left());
        out.writeExpression(fn.right());
    }

    // -- NamedExpressions

    static Alias readAlias(PlanStreamInput in) throws IOException {
        return new Alias(
            Source.readFrom(in),
            in.readString(),
            in.readOptionalString(),
            in.readNamed(Expression.class),
            NameId.readFrom(in),
            in.readBoolean()
        );
    }

    static void writeAlias(PlanStreamOutput out, Alias alias) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(alias.name());
        out.writeOptionalString(alias.qualifier());
        out.writeExpression(alias.child());
        alias.id().writeTo(out);
        out.writeBoolean(alias.synthetic());
    }

    // -- Expressions (other)

    static Literal readLiteral(PlanStreamInput in) throws IOException {
        Source source = Source.readFrom(in);
        Object value = in.readGenericValue();
        DataType dataType = DataType.readFrom(in);
        return new Literal(source, mapToLiteralValue(in, dataType, value), dataType);
    }

    static void writeLiteral(PlanStreamOutput out, Literal literal) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeGenericValue(mapFromLiteralValue(out, literal.dataType(), literal.value()));
        out.writeString(literal.dataType().typeName());
    }

    /**
     * Not all literal values are currently supported in StreamInput/StreamOutput as generic values.
     * This mapper allows for addition of new and interesting values without (yet) adding to StreamInput/Output.
     * This makes the most sense during the pre-GA version of ESQL. When we get near GA we might want to push this down.
     * <p>
     * For the spatial point type support we need to care about the fact that 8.12.0 uses encoded longs for serializing
     * while 8.13 uses WKB.
     */
    private static Object mapFromLiteralValue(PlanStreamOutput out, DataType dataType, Object value) {
        if (dataType == GEO_POINT || dataType == CARTESIAN_POINT) {
            // In 8.12.0 we serialized point literals as encoded longs, but now use WKB
            if (out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
                if (value instanceof List<?> list) {
                    return list.stream().map(v -> mapFromLiteralValue(out, dataType, v)).toList();
                }
                return wkbAsLong(dataType, (BytesRef) value);
            }
        }
        return value;
    }

    /**
     * Not all literal values are currently supported in StreamInput/StreamOutput as generic values.
     * This mapper allows for addition of new and interesting values without (yet) changing StreamInput/Output.
     */
    private static Object mapToLiteralValue(PlanStreamInput in, DataType dataType, Object value) {
        if (dataType == GEO_POINT || dataType == CARTESIAN_POINT) {
            // In 8.12.0 we serialized point literals as encoded longs, but now use WKB
            if (in.getTransportVersion().before(TransportVersions.V_8_13_0)) {
                if (value instanceof List<?> list) {
                    return list.stream().map(v -> mapToLiteralValue(in, dataType, v)).toList();
                }
                return longAsWKB(dataType, (Long) value);
            }
        }
        return value;
    }

    private static BytesRef longAsWKB(DataType dataType, long encoded) {
        return dataType == GEO_POINT ? GEO.longAsWkb(encoded) : CARTESIAN.longAsWkb(encoded);
    }

    private static long wkbAsLong(DataType dataType, BytesRef wkb) {
        return dataType == GEO_POINT ? GEO.wkbAsLong(wkb) : CARTESIAN.wkbAsLong(wkb);
    }

    static Order readOrder(PlanStreamInput in) throws IOException {
        return new org.elasticsearch.xpack.esql.expression.Order(
            Source.readFrom(in),
            in.readNamed(Expression.class),
            in.readEnum(Order.OrderDirection.class),
            in.readEnum(Order.NullsPosition.class)
        );
    }

    static void writeOrder(PlanStreamOutput out, Order order) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeExpression(order.child());
        out.writeEnum(order.direction());
        out.writeEnum(order.nullsPosition());
    }

    // -- ancillary supporting classes of plan nodes, etc

    static EsQueryExec.FieldSort readFieldSort(PlanStreamInput in) throws IOException {
        return new EsQueryExec.FieldSort(
            new FieldAttribute(in),
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

    static Log readLog(PlanStreamInput in) throws IOException {
        return new Log(Source.readFrom(in), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeLog(PlanStreamOutput out, Log log) throws IOException {
        log.source().writeTo(out);
        List<Expression> fields = log.children();
        assert fields.size() == 1 || fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeOptionalWriteable(fields.size() == 2 ? o -> out.writeExpression(fields.get(1)) : null);
    }

    static MvSort readMvSort(PlanStreamInput in) throws IOException {
        return new MvSort(Source.readFrom(in), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeMvSort(PlanStreamOutput out, MvSort mvSort) throws IOException {
        mvSort.source().writeTo(out);
        List<Expression> fields = mvSort.children();
        assert fields.size() == 1 || fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeOptionalWriteable(fields.size() == 2 ? o -> out.writeExpression(fields.get(1)) : null);
    }

    static MvSlice readMvSlice(PlanStreamInput in) throws IOException {
        return new MvSlice(Source.readFrom(in), in.readExpression(), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeMvSlice(PlanStreamOutput out, MvSlice fn) throws IOException {
        Source.EMPTY.writeTo(out);
        List<Expression> fields = fn.children();
        assert fields.size() == 2 || fields.size() == 3;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
        out.writeOptionalWriteable(fields.size() == 3 ? o -> out.writeExpression(fields.get(2)) : null);
    }

    static MvZip readMvZip(PlanStreamInput in) throws IOException {
        return new MvZip(Source.readFrom(in), in.readExpression(), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeMvZip(PlanStreamOutput out, MvZip fn) throws IOException {
        Source.EMPTY.writeTo(out);
        List<Expression> fields = fn.children();
        assert fields.size() == 2 || fields.size() == 3;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
        out.writeOptionalWriteable(fields.size() == 3 ? o -> out.writeExpression(fields.get(2)) : null);
    }
}
