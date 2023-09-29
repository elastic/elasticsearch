/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolution;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.RLike;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIP;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersion;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Now;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan2;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.AutoBucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.E;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.IsFinite;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.IsInfinite;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.IsNaN;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pi;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Left;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Right;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Split;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Dissect.Parser;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
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
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.MetadataAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DateEsField;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.type.TextEsField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Map.entry;
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

    static final Class<org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction> QL_UNARY_SCLR_CLS =
        org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction.class;

    static final Class<UnaryScalarFunction> ESQL_UNARY_SCLR_CLS = UnaryScalarFunction.class;

    /**
     * List of named type entries that link concrete names to stream reader and writer implementations.
     * Entries have the form;  category,  name,  serializer method,  deserializer method.
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
            of(LogicalPlan.class, Filter.class, PlanNamedTypes::writeFilter, PlanNamedTypes::readFilter),
            of(LogicalPlan.class, Grok.class, PlanNamedTypes::writeGrok, PlanNamedTypes::readGrok),
            of(LogicalPlan.class, Limit.class, PlanNamedTypes::writeLimit, PlanNamedTypes::readLimit),
            of(LogicalPlan.class, OrderBy.class, PlanNamedTypes::writeOrderBy, PlanNamedTypes::readOrderBy),
            of(LogicalPlan.class, Project.class, PlanNamedTypes::writeProject, PlanNamedTypes::readProject),
            of(LogicalPlan.class, TopN.class, PlanNamedTypes::writeTopN, PlanNamedTypes::readTopN),
            // Attributes
            of(Attribute.class, FieldAttribute.class, PlanNamedTypes::writeFieldAttribute, PlanNamedTypes::readFieldAttribute),
            of(Attribute.class, ReferenceAttribute.class, PlanNamedTypes::writeReferenceAttr, PlanNamedTypes::readReferenceAttr),
            of(Attribute.class, MetadataAttribute.class, PlanNamedTypes::writeMetadataAttr, PlanNamedTypes::readMetadataAttr),
            of(Attribute.class, UnsupportedAttribute.class, PlanNamedTypes::writeUnsupportedAttr, PlanNamedTypes::readUnsupportedAttr),
            // EsFields
            of(EsField.class, EsField.class, PlanNamedTypes::writeEsField, PlanNamedTypes::readEsField),
            of(EsField.class, DateEsField.class, PlanNamedTypes::writeDateEsField, PlanNamedTypes::readDateEsField),
            of(EsField.class, InvalidMappedField.class, PlanNamedTypes::writeInvalidMappedField, PlanNamedTypes::readInvalidMappedField),
            of(EsField.class, KeywordEsField.class, PlanNamedTypes::writeKeywordEsField, PlanNamedTypes::readKeywordEsField),
            of(EsField.class, TextEsField.class, PlanNamedTypes::writeTextEsField, PlanNamedTypes::readTextEsField),
            of(EsField.class, UnsupportedEsField.class, PlanNamedTypes::writeUnsupportedEsField, PlanNamedTypes::readUnsupportedEsField),
            // NamedExpressions
            of(NamedExpression.class, Alias.class, PlanNamedTypes::writeAlias, PlanNamedTypes::readAlias),
            // BinaryComparison
            of(BinaryComparison.class, Equals.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(BinaryComparison.class, NullEquals.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(BinaryComparison.class, NotEquals.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(BinaryComparison.class, GreaterThan.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(BinaryComparison.class, GreaterThanOrEqual.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(BinaryComparison.class, LessThan.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
            of(BinaryComparison.class, LessThanOrEqual.class, PlanNamedTypes::writeBinComparison, PlanNamedTypes::readBinComparison),
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
            of(ESQL_UNARY_SCLR_CLS, Ceil.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Cos.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Cosh.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Floor.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, IsFinite.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, IsInfinite.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, IsNaN.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Length.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Log10.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, LTrim.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, RTrim.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Sin.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Sinh.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Sqrt.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Tan.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, Tanh.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToBoolean.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToDatetime.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToDegrees.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
            of(ESQL_UNARY_SCLR_CLS, ToDouble.class, PlanNamedTypes::writeESQLUnaryScalar, PlanNamedTypes::readESQLUnaryScalar),
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
            of(ScalarFunction.class, AutoBucket.class, PlanNamedTypes::writeAutoBucket, PlanNamedTypes::readAutoBucket),
            of(ScalarFunction.class, Case.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, CIDRMatch.class, PlanNamedTypes::writeCIDRMatch, PlanNamedTypes::readCIDRMatch),
            of(ScalarFunction.class, Coalesce.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, Concat.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, DateExtract.class, PlanNamedTypes::writeDateExtract, PlanNamedTypes::readDateExtract),
            of(ScalarFunction.class, DateFormat.class, PlanNamedTypes::writeDateFormat, PlanNamedTypes::readDateFormat),
            of(ScalarFunction.class, DateParse.class, PlanNamedTypes::writeDateTimeParse, PlanNamedTypes::readDateTimeParse),
            of(ScalarFunction.class, DateTrunc.class, PlanNamedTypes::writeDateTrunc, PlanNamedTypes::readDateTrunc),
            of(ScalarFunction.class, E.class, PlanNamedTypes::writeNoArgScalar, PlanNamedTypes::readNoArgScalar),
            of(ScalarFunction.class, Greatest.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, Least.class, PlanNamedTypes::writeVararg, PlanNamedTypes::readVarag),
            of(ScalarFunction.class, Now.class, PlanNamedTypes::writeNow, PlanNamedTypes::readNow),
            of(ScalarFunction.class, Pi.class, PlanNamedTypes::writeNoArgScalar, PlanNamedTypes::readNoArgScalar),
            of(ScalarFunction.class, Round.class, PlanNamedTypes::writeRound, PlanNamedTypes::readRound),
            of(ScalarFunction.class, Pow.class, PlanNamedTypes::writePow, PlanNamedTypes::readPow),
            of(ScalarFunction.class, StartsWith.class, PlanNamedTypes::writeStartsWith, PlanNamedTypes::readStartsWith),
            of(ScalarFunction.class, EndsWith.class, PlanNamedTypes::writeEndsWith, PlanNamedTypes::readEndsWith),
            of(ScalarFunction.class, Substring.class, PlanNamedTypes::writeSubstring, PlanNamedTypes::readSubstring),
            of(ScalarFunction.class, Left.class, PlanNamedTypes::writeLeft, PlanNamedTypes::readLeft),
            of(ScalarFunction.class, Right.class, PlanNamedTypes::writeRight, PlanNamedTypes::readRight),
            of(ScalarFunction.class, Split.class, PlanNamedTypes::writeSplit, PlanNamedTypes::readSplit),
            of(ScalarFunction.class, Tau.class, PlanNamedTypes::writeNoArgScalar, PlanNamedTypes::readNoArgScalar),
            of(ScalarFunction.class, Replace.class, PlanNamedTypes::writeReplace, PlanNamedTypes::readReplace),
            // ArithmeticOperations
            of(ArithmeticOperation.class, Add.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            of(ArithmeticOperation.class, Sub.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            of(ArithmeticOperation.class, Mul.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            of(ArithmeticOperation.class, Div.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            of(ArithmeticOperation.class, Mod.class, PlanNamedTypes::writeArithmeticOperation, PlanNamedTypes::readArithmeticOperation),
            // AggregateFunctions
            of(AggregateFunction.class, Avg.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Count.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, CountDistinct.class, PlanNamedTypes::writeCountDistinct, PlanNamedTypes::readCountDistinct),
            of(AggregateFunction.class, Min.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Max.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Median.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, MedianAbsoluteDeviation.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            of(AggregateFunction.class, Percentile.class, PlanNamedTypes::writePercentile, PlanNamedTypes::readPercentile),
            of(AggregateFunction.class, Sum.class, PlanNamedTypes::writeAggFunction, PlanNamedTypes::readAggFunction),
            // Multivalue functions
            of(ScalarFunction.class, MvAvg.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvCount.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvConcat.class, PlanNamedTypes::writeMvConcat, PlanNamedTypes::readMvConcat),
            of(ScalarFunction.class, MvDedupe.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvMax.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvMedian.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvMin.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            of(ScalarFunction.class, MvSum.class, PlanNamedTypes::writeMvFunction, PlanNamedTypes::readMvFunction),
            // Expressions (other)
            of(Expression.class, Literal.class, PlanNamedTypes::writeLiteral, PlanNamedTypes::readLiteral),
            of(Expression.class, Order.class, PlanNamedTypes::writeOrder, PlanNamedTypes::readOrder)
        );
    }

    // -- physical plan nodes
    static AggregateExec readAggregateExec(PlanStreamInput in) throws IOException {
        return new AggregateExec(
            Source.EMPTY,
            in.readPhysicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression)),
            readNamedExpressions(in),
            in.readEnum(AggregateExec.Mode.class),
            in.readOptionalVInt()
        );
    }

    static void writeAggregateExec(PlanStreamOutput out, AggregateExec aggregateExec) throws IOException {
        out.writePhysicalPlanNode(aggregateExec.child());
        out.writeCollection(aggregateExec.groupings(), writerFromPlanWriter(PlanStreamOutput::writeExpression));
        writeNamedExpressions(out, aggregateExec.aggregates());
        out.writeEnum(aggregateExec.getMode());
        out.writeOptionalVInt(aggregateExec.estimatedRowSize());
    }

    static DissectExec readDissectExec(PlanStreamInput in) throws IOException {
        return new DissectExec(Source.EMPTY, in.readPhysicalPlanNode(), in.readExpression(), readDissectParser(in), readAttributes(in));
    }

    static void writeDissectExec(PlanStreamOutput out, DissectExec dissectExec) throws IOException {
        out.writePhysicalPlanNode(dissectExec.child());
        out.writeExpression(dissectExec.inputExpression());
        writeDissectParser(out, dissectExec.parser());
        writeAttributes(out, dissectExec.extractedFields());
    }

    static EsQueryExec readEsQueryExec(PlanStreamInput in) throws IOException {
        return new EsQueryExec(
            Source.EMPTY,
            readEsIndex(in),
            readAttributes(in),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readOptionalNamed(Expression.class),
            in.readOptionalCollectionAsList(readerFromPlanReader(PlanNamedTypes::readFieldSort)),
            in.readOptionalVInt()
        );
    }

    static void writeEsQueryExec(PlanStreamOutput out, EsQueryExec esQueryExec) throws IOException {
        assert esQueryExec.children().size() == 0;
        writeEsIndex(out, esQueryExec.index());
        writeAttributes(out, esQueryExec.output());
        out.writeOptionalNamedWriteable(esQueryExec.query());
        out.writeOptionalExpression(esQueryExec.limit());
        out.writeOptionalCollection(esQueryExec.sorts(), writerFromPlanWriter(PlanNamedTypes::writeFieldSort));
        out.writeOptionalInt(esQueryExec.estimatedRowSize());
    }

    static EsSourceExec readEsSourceExec(PlanStreamInput in) throws IOException {
        return new EsSourceExec(Source.EMPTY, readEsIndex(in), readAttributes(in), in.readOptionalNamedWriteable(QueryBuilder.class));
    }

    static void writeEsSourceExec(PlanStreamOutput out, EsSourceExec esSourceExec) throws IOException {
        writeEsIndex(out, esSourceExec.index());
        writeAttributes(out, esSourceExec.output());
        out.writeOptionalNamedWriteable(esSourceExec.query());
    }

    static EvalExec readEvalExec(PlanStreamInput in) throws IOException {
        return new EvalExec(Source.EMPTY, in.readPhysicalPlanNode(), readAliases(in));
    }

    static void writeEvalExec(PlanStreamOutput out, EvalExec evalExec) throws IOException {
        out.writePhysicalPlanNode(evalExec.child());
        writeAliases(out, evalExec.fields());
    }

    static EnrichExec readEnrichExec(PlanStreamInput in) throws IOException {
        return new EnrichExec(
            Source.EMPTY,
            in.readPhysicalPlanNode(),
            in.readNamedExpression(),
            in.readString(),
            in.readString(),
            readEsIndex(in),
            readNamedExpressions(in)
        );
    }

    static void writeEnrichExec(PlanStreamOutput out, EnrichExec enrich) throws IOException {
        out.writePhysicalPlanNode(enrich.child());
        out.writeNamedExpression(enrich.matchField());
        out.writeString(enrich.policyName());
        out.writeString(enrich.policyMatchField());
        writeEsIndex(out, enrich.enrichIndex());
        writeNamedExpressions(out, enrich.enrichFields());
    }

    static ExchangeExec readExchangeExec(PlanStreamInput in) throws IOException {
        return new ExchangeExec(Source.EMPTY, readAttributes(in), in.readBoolean(), in.readPhysicalPlanNode());
    }

    static void writeExchangeExec(PlanStreamOutput out, ExchangeExec exchangeExec) throws IOException {
        writeAttributes(out, exchangeExec.output());
        out.writeBoolean(exchangeExec.isInBetweenAggs());
        out.writePhysicalPlanNode(exchangeExec.child());
    }

    static ExchangeSinkExec readExchangeSinkExec(PlanStreamInput in) throws IOException {
        return new ExchangeSinkExec(Source.EMPTY, readAttributes(in), in.readPhysicalPlanNode());
    }

    static void writeExchangeSinkExec(PlanStreamOutput out, ExchangeSinkExec exchangeSinkExec) throws IOException {
        writeAttributes(out, exchangeSinkExec.output());
        out.writePhysicalPlanNode(exchangeSinkExec.child());
    }

    static ExchangeSourceExec readExchangeSourceExec(PlanStreamInput in) throws IOException {
        return new ExchangeSourceExec(Source.EMPTY, readAttributes(in), in.readBoolean());
    }

    static void writeExchangeSourceExec(PlanStreamOutput out, ExchangeSourceExec exchangeSourceExec) throws IOException {
        writeAttributes(out, exchangeSourceExec.output());
        out.writeBoolean(exchangeSourceExec.isIntermediateAgg());
    }

    static FieldExtractExec readFieldExtractExec(PlanStreamInput in) throws IOException {
        return new FieldExtractExec(Source.EMPTY, in.readPhysicalPlanNode(), readAttributes(in));
    }

    static void writeFieldExtractExec(PlanStreamOutput out, FieldExtractExec fieldExtractExec) throws IOException {
        out.writePhysicalPlanNode(fieldExtractExec.child());
        writeAttributes(out, fieldExtractExec.attributesToExtract());
    }

    static FilterExec readFilterExec(PlanStreamInput in) throws IOException {
        return new FilterExec(Source.EMPTY, in.readPhysicalPlanNode(), in.readExpression());
    }

    static void writeFilterExec(PlanStreamOutput out, FilterExec filterExec) throws IOException {
        out.writePhysicalPlanNode(filterExec.child());
        out.writeExpression(filterExec.condition());
    }

    static FragmentExec readFragmentExec(PlanStreamInput in) throws IOException {
        return new FragmentExec(
            Source.EMPTY,
            in.readLogicalPlanNode(),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readOptionalVInt()
        );
    }

    static void writeFragmentExec(PlanStreamOutput out, FragmentExec fragmentExec) throws IOException {
        out.writeLogicalPlanNode(fragmentExec.fragment());
        out.writeOptionalNamedWriteable(fragmentExec.esFilter());
        out.writeOptionalVInt(fragmentExec.estimatedRowSize());
    }

    static GrokExec readGrokExec(PlanStreamInput in) throws IOException {
        return new GrokExec(
            Source.EMPTY,
            in.readPhysicalPlanNode(),
            in.readExpression(),
            Grok.pattern(Source.EMPTY, in.readString()),
            readAttributes(in)
        );
    }

    static void writeGrokExec(PlanStreamOutput out, GrokExec grokExec) throws IOException {
        out.writePhysicalPlanNode(grokExec.child());
        out.writeExpression(grokExec.inputExpression());
        out.writeString(grokExec.pattern().pattern());
        writeAttributes(out, grokExec.extractedFields());
    }

    static LimitExec readLimitExec(PlanStreamInput in) throws IOException {
        return new LimitExec(Source.EMPTY, in.readPhysicalPlanNode(), in.readNamed(Expression.class));
    }

    static void writeLimitExec(PlanStreamOutput out, LimitExec limitExec) throws IOException {
        out.writePhysicalPlanNode(limitExec.child());
        out.writeExpression(limitExec.limit());
    }

    static MvExpandExec readMvExpandExec(PlanStreamInput in) throws IOException {
        return new MvExpandExec(Source.EMPTY, in.readPhysicalPlanNode(), in.readNamedExpression());
    }

    static void writeMvExpandExec(PlanStreamOutput out, MvExpandExec mvExpandExec) throws IOException {
        out.writePhysicalPlanNode(mvExpandExec.child());
        out.writeNamedExpression(mvExpandExec.target());
    }

    static OrderExec readOrderExec(PlanStreamInput in) throws IOException {
        return new OrderExec(
            Source.EMPTY,
            in.readPhysicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readOrder))
        );
    }

    static void writeOrderExec(PlanStreamOutput out, OrderExec orderExec) throws IOException {
        out.writePhysicalPlanNode(orderExec.child());
        out.writeCollection(orderExec.order(), writerFromPlanWriter(PlanNamedTypes::writeOrder));
    }

    static ProjectExec readProjectExec(PlanStreamInput in) throws IOException {
        return new ProjectExec(Source.EMPTY, in.readPhysicalPlanNode(), readNamedExpressions(in));
    }

    static void writeProjectExec(PlanStreamOutput out, ProjectExec projectExec) throws IOException {
        out.writePhysicalPlanNode(projectExec.child());
        writeNamedExpressions(out, projectExec.projections());
    }

    static RowExec readRowExec(PlanStreamInput in) throws IOException {
        return new RowExec(Source.EMPTY, readAliases(in));
    }

    static void writeRowExec(PlanStreamOutput out, RowExec rowExec) throws IOException {
        assert rowExec.children().size() == 0;
        writeAliases(out, rowExec.fields());
    }

    @SuppressWarnings("unchecked")
    static ShowExec readShowExec(PlanStreamInput in) throws IOException {
        return new ShowExec(Source.EMPTY, readAttributes(in), (List<List<Object>>) in.readGenericValue());
    }

    static void writeShowExec(PlanStreamOutput out, ShowExec showExec) throws IOException {
        writeAttributes(out, showExec.output());
        out.writeGenericValue(showExec.values());
    }

    static TopNExec readTopNExec(PlanStreamInput in) throws IOException {
        return new TopNExec(
            Source.EMPTY,
            in.readPhysicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readOrder)),
            in.readNamed(Expression.class),
            in.readOptionalVInt()
        );
    }

    static void writeTopNExec(PlanStreamOutput out, TopNExec topNExec) throws IOException {
        out.writePhysicalPlanNode(topNExec.child());
        out.writeCollection(topNExec.order(), writerFromPlanWriter(PlanNamedTypes::writeOrder));
        out.writeExpression(topNExec.limit());
        out.writeOptionalVInt(topNExec.estimatedRowSize());
    }

    // -- Logical plan nodes
    static Aggregate readAggregate(PlanStreamInput in) throws IOException {
        return new Aggregate(
            Source.EMPTY,
            in.readLogicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression)),
            readNamedExpressions(in)
        );
    }

    static void writeAggregate(PlanStreamOutput out, Aggregate aggregate) throws IOException {
        out.writeLogicalPlanNode(aggregate.child());
        out.writeCollection(aggregate.groupings(), writerFromPlanWriter(PlanStreamOutput::writeExpression));
        writeNamedExpressions(out, aggregate.aggregates());
    }

    static Dissect readDissect(PlanStreamInput in) throws IOException {
        return new Dissect(Source.EMPTY, in.readLogicalPlanNode(), in.readExpression(), readDissectParser(in), readAttributes(in));
    }

    static void writeDissect(PlanStreamOutput out, Dissect dissect) throws IOException {
        out.writeLogicalPlanNode(dissect.child());
        out.writeExpression(dissect.input());
        writeDissectParser(out, dissect.parser());
        writeAttributes(out, dissect.extractedFields());
    }

    static EsRelation readEsRelation(PlanStreamInput in) throws IOException {
        return new EsRelation(Source.EMPTY, readEsIndex(in), readAttributes(in));
    }

    static void writeEsRelation(PlanStreamOutput out, EsRelation relation) throws IOException {
        assert relation.children().size() == 0;
        writeEsIndex(out, relation.index());
        writeAttributes(out, relation.output());
    }

    static Eval readEval(PlanStreamInput in) throws IOException {
        return new Eval(Source.EMPTY, in.readLogicalPlanNode(), readAliases(in));
    }

    static void writeEval(PlanStreamOutput out, Eval eval) throws IOException {
        out.writeLogicalPlanNode(eval.child());
        writeAliases(out, eval.fields());
    }

    static Enrich readEnrich(PlanStreamInput in) throws IOException {
        return new Enrich(
            Source.EMPTY,
            in.readLogicalPlanNode(),
            in.readExpression(),
            in.readNamedExpression(),
            new EnrichPolicyResolution(in.readString(), new EnrichPolicy(in), IndexResolution.valid(readEsIndex(in))),
            readNamedExpressions(in)
        );
    }

    static void writeEnrich(PlanStreamOutput out, Enrich enrich) throws IOException {
        out.writeLogicalPlanNode(enrich.child());
        out.writeExpression(enrich.policyName());
        out.writeNamedExpression(enrich.matchField());
        out.writeString(enrich.policy().policyName());
        enrich.policy().policy().writeTo(out);
        writeEsIndex(out, enrich.policy().index().get());
        writeNamedExpressions(out, enrich.enrichFields());
    }

    static Filter readFilter(PlanStreamInput in) throws IOException {
        return new Filter(Source.EMPTY, in.readLogicalPlanNode(), in.readExpression());
    }

    static void writeFilter(PlanStreamOutput out, Filter filter) throws IOException {
        out.writeLogicalPlanNode(filter.child());
        out.writeExpression(filter.condition());
    }

    static Grok readGrok(PlanStreamInput in) throws IOException {
        return new Grok(
            Source.EMPTY,
            in.readLogicalPlanNode(),
            in.readExpression(),
            Grok.pattern(Source.EMPTY, in.readString()),
            readAttributes(in)
        );
    }

    static void writeGrok(PlanStreamOutput out, Grok grok) throws IOException {
        out.writeLogicalPlanNode(grok.child());
        out.writeExpression(grok.input());
        out.writeString(grok.parser().pattern());
        writeAttributes(out, grok.extractedFields());
    }

    static Limit readLimit(PlanStreamInput in) throws IOException {
        return new Limit(Source.EMPTY, in.readNamed(Expression.class), in.readLogicalPlanNode());
    }

    static void writeLimit(PlanStreamOutput out, Limit limit) throws IOException {
        out.writeExpression(limit.limit());
        out.writeLogicalPlanNode(limit.child());
    }

    static OrderBy readOrderBy(PlanStreamInput in) throws IOException {
        return new OrderBy(
            Source.EMPTY,
            in.readLogicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readOrder))
        );
    }

    static void writeOrderBy(PlanStreamOutput out, OrderBy order) throws IOException {
        out.writeLogicalPlanNode(order.child());
        out.writeCollection(order.order(), writerFromPlanWriter(PlanNamedTypes::writeOrder));
    }

    static Project readProject(PlanStreamInput in) throws IOException {
        return new Project(Source.EMPTY, in.readLogicalPlanNode(), readNamedExpressions(in));
    }

    static void writeProject(PlanStreamOutput out, Project project) throws IOException {
        out.writeLogicalPlanNode(project.child());
        writeNamedExpressions(out, project.projections());
    }

    static TopN readTopN(PlanStreamInput in) throws IOException {
        return new TopN(
            Source.EMPTY,
            in.readLogicalPlanNode(),
            in.readCollectionAsList(readerFromPlanReader(PlanNamedTypes::readOrder)),
            in.readNamed(Expression.class)
        );
    }

    static void writeTopN(PlanStreamOutput out, TopN topN) throws IOException {
        out.writeLogicalPlanNode(topN.child());
        out.writeCollection(topN.order(), writerFromPlanWriter(PlanNamedTypes::writeOrder));
        out.writeExpression(topN.limit());
    }

    //
    // -- Attributes
    //

    private static List<Attribute> readAttributes(PlanStreamInput in) throws IOException {
        return in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readAttribute));
    }

    static void writeAttributes(PlanStreamOutput out, List<Attribute> attributes) throws IOException {
        out.writeCollection(attributes, writerFromPlanWriter(PlanStreamOutput::writeAttribute));
    }

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

    static FieldAttribute readFieldAttribute(PlanStreamInput in) throws IOException {
        return new FieldAttribute(
            Source.EMPTY,
            in.readOptionalWithReader(PlanNamedTypes::readFieldAttribute),
            in.readString(),
            in.dataTypeFromTypeName(in.readString()),
            in.readEsFieldNamed(),
            in.readOptionalString(),
            in.readEnum(Nullability.class),
            in.nameIdFromLongValue(in.readLong()),
            in.readBoolean()
        );
    }

    static void writeFieldAttribute(PlanStreamOutput out, FieldAttribute fileAttribute) throws IOException {
        out.writeOptionalWriteable(fileAttribute.parent() == null ? null : o -> writeFieldAttribute(out, fileAttribute.parent()));
        out.writeString(fileAttribute.name());
        out.writeString(fileAttribute.dataType().typeName());
        out.writeNamed(EsField.class, fileAttribute.field());
        out.writeOptionalString(fileAttribute.qualifier());
        out.writeEnum(fileAttribute.nullable());
        out.writeLong(Long.parseLong(fileAttribute.id().toString()));
        out.writeBoolean(fileAttribute.synthetic());
    }

    static ReferenceAttribute readReferenceAttr(PlanStreamInput in) throws IOException {
        return new ReferenceAttribute(
            Source.EMPTY,
            in.readString(),
            in.dataTypeFromTypeName(in.readString()),
            in.readOptionalString(),
            in.readEnum(Nullability.class),
            in.nameIdFromLongValue(in.readLong()),
            in.readBoolean()
        );
    }

    static void writeReferenceAttr(PlanStreamOutput out, ReferenceAttribute referenceAttribute) throws IOException {
        out.writeString(referenceAttribute.name());
        out.writeString(referenceAttribute.dataType().typeName());
        out.writeOptionalString(referenceAttribute.qualifier());
        out.writeEnum(referenceAttribute.nullable());
        out.writeLong(Long.parseLong(referenceAttribute.id().toString()));
        out.writeBoolean(referenceAttribute.synthetic());
    }

    static MetadataAttribute readMetadataAttr(PlanStreamInput in) throws IOException {
        return new MetadataAttribute(
            Source.EMPTY,
            in.readString(),
            in.dataTypeFromTypeName(in.readString()),
            in.readOptionalString(),
            in.readEnum(Nullability.class),
            in.nameIdFromLongValue(in.readLong()),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    static void writeMetadataAttr(PlanStreamOutput out, MetadataAttribute metadataAttribute) throws IOException {
        out.writeString(metadataAttribute.name());
        out.writeString(metadataAttribute.dataType().typeName());
        out.writeOptionalString(metadataAttribute.qualifier());
        out.writeEnum(metadataAttribute.nullable());
        out.writeLong(Long.parseLong(metadataAttribute.id().toString()));
        out.writeBoolean(metadataAttribute.synthetic());
        out.writeBoolean(metadataAttribute.searchable());
    }

    static UnsupportedAttribute readUnsupportedAttr(PlanStreamInput in) throws IOException {
        return new UnsupportedAttribute(
            Source.EMPTY,
            in.readString(),
            readUnsupportedEsField(in),
            in.readOptionalString(),
            in.nameIdFromLongValue(in.readLong())
        );
    }

    static void writeUnsupportedAttr(PlanStreamOutput out, UnsupportedAttribute unsupportedAttribute) throws IOException {
        out.writeString(unsupportedAttribute.name());
        writeUnsupportedEsField(out, unsupportedAttribute.field());
        out.writeOptionalString(unsupportedAttribute.hasCustomMessage() ? unsupportedAttribute.unresolvedMessage() : null);
        out.writeLong(Long.parseLong(unsupportedAttribute.id().toString()));
    }

    // -- EsFields

    static EsField readEsField(PlanStreamInput in) throws IOException {
        return new EsField(
            in.readString(),
            in.dataTypeFromTypeName(in.readString()),
            in.readImmutableMap(StreamInput::readString, readerFromPlanReader(PlanStreamInput::readEsFieldNamed)),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    static void writeEsField(PlanStreamOutput out, EsField esField) throws IOException {
        out.writeString(esField.getName());
        out.writeString(esField.getDataType().typeName());
        out.writeMap(esField.getProperties(), (o, v) -> out.writeNamed(EsField.class, v));
        out.writeBoolean(esField.isAggregatable());
        out.writeBoolean(esField.isAlias());
    }

    static DateEsField readDateEsField(PlanStreamInput in) throws IOException {
        return DateEsField.dateEsField(
            in.readString(),
            in.readImmutableMap(StreamInput::readString, readerFromPlanReader(PlanStreamInput::readEsFieldNamed)),
            in.readBoolean()
        );
    }

    static void writeDateEsField(PlanStreamOutput out, DateEsField dateEsField) throws IOException {
        out.writeString(dateEsField.getName());
        out.writeMap(dateEsField.getProperties(), (o, v) -> out.writeNamed(EsField.class, v));
        out.writeBoolean(dateEsField.isAggregatable());
    }

    static InvalidMappedField readInvalidMappedField(PlanStreamInput in) throws IOException {
        return new InvalidMappedField(in.readString(), in.readString());
    }

    static void writeInvalidMappedField(PlanStreamOutput out, InvalidMappedField field) throws IOException {
        out.writeString(field.getName());
        out.writeString(field.errorMessage());
    }

    static KeywordEsField readKeywordEsField(PlanStreamInput in) throws IOException {
        return new KeywordEsField(
            in.readString(),
            in.readImmutableMap(StreamInput::readString, readerFromPlanReader(PlanStreamInput::readEsFieldNamed)),
            in.readBoolean(),
            in.readInt(),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    static void writeKeywordEsField(PlanStreamOutput out, KeywordEsField keywordEsField) throws IOException {
        out.writeString(keywordEsField.getName());
        out.writeMap(keywordEsField.getProperties(), (o, v) -> out.writeNamed(EsField.class, v));
        out.writeBoolean(keywordEsField.isAggregatable());
        out.writeInt(keywordEsField.getPrecision());
        out.writeBoolean(keywordEsField.getNormalized());
        out.writeBoolean(keywordEsField.isAlias());
    }

    static TextEsField readTextEsField(PlanStreamInput in) throws IOException {
        return new TextEsField(
            in.readString(),
            in.readImmutableMap(StreamInput::readString, readerFromPlanReader(PlanStreamInput::readEsFieldNamed)),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    static void writeTextEsField(PlanStreamOutput out, TextEsField textEsField) throws IOException {
        out.writeString(textEsField.getName());
        out.writeMap(textEsField.getProperties(), (o, v) -> out.writeNamed(EsField.class, v));
        out.writeBoolean(textEsField.isAggregatable());
        out.writeBoolean(textEsField.isAlias());
    }

    static UnsupportedEsField readUnsupportedEsField(PlanStreamInput in) throws IOException {
        return new UnsupportedEsField(
            in.readString(),
            in.readString(),
            in.readOptionalString(),
            in.readImmutableMap(StreamInput::readString, readerFromPlanReader(PlanStreamInput::readEsFieldNamed))
        );
    }

    static void writeUnsupportedEsField(PlanStreamOutput out, UnsupportedEsField unsupportedEsField) throws IOException {
        out.writeString(unsupportedEsField.getName());
        out.writeString(unsupportedEsField.getOriginalType());
        out.writeOptionalString(unsupportedEsField.getInherited());
        out.writeMap(unsupportedEsField.getProperties(), (o, v) -> out.writeNamed(EsField.class, v));
    }

    // -- BinaryComparison

    static BinaryComparison readBinComparison(PlanStreamInput in, String name) throws IOException {
        var operation = in.readEnum(BinaryComparisonProcessor.BinaryComparisonOperation.class);
        var left = in.readExpression();
        var right = in.readExpression();
        var zoneId = in.readOptionalZoneId();
        return switch (operation) {
            case EQ -> new Equals(Source.EMPTY, left, right, zoneId);
            case NULLEQ -> new NullEquals(Source.EMPTY, left, right, zoneId);
            case NEQ -> new NotEquals(Source.EMPTY, left, right, zoneId);
            case GT -> new GreaterThan(Source.EMPTY, left, right, zoneId);
            case GTE -> new GreaterThanOrEqual(Source.EMPTY, left, right, zoneId);
            case LT -> new LessThan(Source.EMPTY, left, right, zoneId);
            case LTE -> new LessThanOrEqual(Source.EMPTY, left, right, zoneId);
        };
    }

    static void writeBinComparison(PlanStreamOutput out, BinaryComparison binaryComparison) throws IOException {
        out.writeEnum(binaryComparison.function());
        out.writeExpression(binaryComparison.left());
        out.writeExpression(binaryComparison.right());
        out.writeOptionalZoneId(binaryComparison.zoneId());
    }

    // -- InComparison

    static In readInComparison(PlanStreamInput in) throws IOException {
        return new In(Source.EMPTY, in.readExpression(), in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression)));
    }

    static void writeInComparison(PlanStreamOutput out, In in) throws IOException {
        out.writeExpression(in.value());
        out.writeCollection(in.list(), writerFromPlanWriter(PlanStreamOutput::writeExpression));
    }

    // -- RegexMatch

    static WildcardLike readWildcardLike(PlanStreamInput in, String name) throws IOException {
        return new WildcardLike(Source.EMPTY, in.readExpression(), new WildcardPattern(in.readString()));
    }

    static void writeWildcardLike(PlanStreamOutput out, WildcardLike like) throws IOException {
        out.writeExpression(like.field());
        out.writeString(like.pattern().pattern());
    }

    static RLike readRLike(PlanStreamInput in, String name) throws IOException {
        return new RLike(Source.EMPTY, in.readExpression(), new RLikePattern(in.readString()));
    }

    static void writeRLike(PlanStreamOutput out, RLike like) throws IOException {
        out.writeExpression(like.field());
        out.writeString(like.pattern().asJavaRegex());
    }

    // -- BinaryLogic

    static final Map<String, TriFunction<Source, Expression, Expression, BinaryLogic>> BINARY_LOGIC_CTRS = Map.ofEntries(
        entry(name(And.class), And::new),
        entry(name(Or.class), Or::new)
    );

    static BinaryLogic readBinaryLogic(PlanStreamInput in, String name) throws IOException {
        var left = in.readExpression();
        var right = in.readExpression();
        return BINARY_LOGIC_CTRS.get(name).apply(Source.EMPTY, left, right);
    }

    static void writeBinaryLogic(PlanStreamOutput out, BinaryLogic binaryLogic) throws IOException {
        out.writeExpression(binaryLogic.left());
        out.writeExpression(binaryLogic.right());
    }

    // -- UnaryScalarFunction

    static final Map<String, BiFunction<Source, Expression, UnaryScalarFunction>> ESQL_UNARY_SCALAR_CTRS = Map.ofEntries(
        entry(name(Abs.class), Abs::new),
        entry(name(Acos.class), Acos::new),
        entry(name(Asin.class), Asin::new),
        entry(name(Atan.class), Atan::new),
        entry(name(Ceil.class), Ceil::new),
        entry(name(Cos.class), Cos::new),
        entry(name(Cosh.class), Cosh::new),
        entry(name(Floor.class), Floor::new),
        entry(name(IsFinite.class), IsFinite::new),
        entry(name(IsInfinite.class), IsInfinite::new),
        entry(name(IsNaN.class), IsNaN::new),
        entry(name(Length.class), Length::new),
        entry(name(Log10.class), Log10::new),
        entry(name(LTrim.class), LTrim::new),
        entry(name(RTrim.class), RTrim::new),
        entry(name(Neg.class), Neg::new),
        entry(name(Sin.class), Sin::new),
        entry(name(Sinh.class), Sinh::new),
        entry(name(Sqrt.class), Sqrt::new),
        entry(name(Tan.class), Tan::new),
        entry(name(Tanh.class), Tanh::new),
        entry(name(ToBoolean.class), ToBoolean::new),
        entry(name(ToDatetime.class), ToDatetime::new),
        entry(name(ToDegrees.class), ToDegrees::new),
        entry(name(ToDouble.class), ToDouble::new),
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
        return ctr.apply(Source.EMPTY, in.readExpression());
    }

    static void writeESQLUnaryScalar(PlanStreamOutput out, UnaryScalarFunction function) throws IOException {
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
        return ctr.apply(Source.EMPTY);
    }

    static void writeNoArgScalar(PlanStreamOutput out, ScalarFunction function) {}

    static final Map<
        String,
        BiFunction<Source, Expression, org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction>> QL_UNARY_SCALAR_CTRS =
            Map.ofEntries(
                entry(name(IsNotNull.class), IsNotNull::new),
                entry(name(IsNull.class), IsNull::new),
                entry(name(Not.class), Not::new)
            );

    static org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction readQLUnaryScalar(PlanStreamInput in, String name)
        throws IOException {
        var ctr = QL_UNARY_SCALAR_CTRS.get(name);
        if (ctr == null) {
            throw new IOException("Constructor for QLUnaryScalar not found for name:" + name);
        }
        return ctr.apply(Source.EMPTY, in.readExpression());
    }

    static void writeQLUnaryScalar(PlanStreamOutput out, org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction function)
        throws IOException {
        out.writeExpression(function.field());
    }

    // -- ScalarFunction

    static Atan2 readAtan2(PlanStreamInput in) throws IOException {
        return new Atan2(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeAtan2(PlanStreamOutput out, Atan2 atan2) throws IOException {
        out.writeExpression(atan2.y());
        out.writeExpression(atan2.x());
    }

    static AutoBucket readAutoBucket(PlanStreamInput in) throws IOException {
        return new AutoBucket(Source.EMPTY, in.readExpression(), in.readExpression(), in.readExpression(), in.readExpression());
    }

    static void writeAutoBucket(PlanStreamOutput out, AutoBucket bucket) throws IOException {
        out.writeExpression(bucket.field());
        out.writeExpression(bucket.buckets());
        out.writeExpression(bucket.from());
        out.writeExpression(bucket.to());
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
            .apply(Source.EMPTY, in.readExpression(), in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression)));
    }

    static void writeVararg(PlanStreamOutput out, ScalarFunction vararg) throws IOException {
        out.writeExpression(vararg.children().get(0));
        out.writeCollection(
            vararg.children().subList(1, vararg.children().size()),
            writerFromPlanWriter(PlanStreamOutput::writeExpression)
        );
    }

    static CountDistinct readCountDistinct(PlanStreamInput in) throws IOException {
        return new CountDistinct(Source.EMPTY, in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeCountDistinct(PlanStreamOutput out, CountDistinct countDistinct) throws IOException {
        List<Expression> fields = countDistinct.children();
        assert fields.size() == 1 || fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeOptionalWriteable(fields.size() == 2 ? o -> out.writeExpression(fields.get(1)) : null);
    }

    static DateExtract readDateExtract(PlanStreamInput in) throws IOException {
        return new DateExtract(Source.EMPTY, in.readExpression(), in.readExpression(), in.configuration());
    }

    static void writeDateExtract(PlanStreamOutput out, DateExtract function) throws IOException {
        List<Expression> fields = function.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static DateFormat readDateFormat(PlanStreamInput in) throws IOException {
        return new DateFormat(Source.EMPTY, in.readExpression(), in.readOptionalNamed(Expression.class), in.configuration());
    }

    static void writeDateFormat(PlanStreamOutput out, DateFormat dateFormat) throws IOException {
        List<Expression> fields = dateFormat.children();
        assert fields.size() == 1 || fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeOptionalWriteable(fields.size() == 2 ? o -> out.writeExpression(fields.get(1)) : null);
    }

    static DateParse readDateTimeParse(PlanStreamInput in) throws IOException {
        return new DateParse(Source.EMPTY, in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeDateTimeParse(PlanStreamOutput out, DateParse function) throws IOException {
        List<Expression> fields = function.children();
        assert fields.size() == 1 || fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeOptionalWriteable(fields.size() == 2 ? o -> out.writeExpression(fields.get(1)) : null);
    }

    static DateTrunc readDateTrunc(PlanStreamInput in) throws IOException {
        return new DateTrunc(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeDateTrunc(PlanStreamOutput out, DateTrunc dateTrunc) throws IOException {
        List<Expression> fields = dateTrunc.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static Now readNow(PlanStreamInput in) throws IOException {
        return new Now(Source.EMPTY, in.configuration());
    }

    static void writeNow(PlanStreamOutput out, Now function) {}

    static Round readRound(PlanStreamInput in) throws IOException {
        return new Round(Source.EMPTY, in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeRound(PlanStreamOutput out, Round round) throws IOException {
        out.writeExpression(round.field());
        out.writeOptionalExpression(round.decimals());
    }

    static Pow readPow(PlanStreamInput in) throws IOException {
        return new Pow(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writePow(PlanStreamOutput out, Pow pow) throws IOException {
        out.writeExpression(pow.base());
        out.writeExpression(pow.exponent());
    }

    static Percentile readPercentile(PlanStreamInput in) throws IOException {
        return new Percentile(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writePercentile(PlanStreamOutput out, Percentile percentile) throws IOException {
        List<Expression> fields = percentile.children();
        assert fields.size() == 2 : "percentile() aggregation must have two arguments";
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static StartsWith readStartsWith(PlanStreamInput in) throws IOException {
        return new StartsWith(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeStartsWith(PlanStreamOutput out, StartsWith startsWith) throws IOException {
        List<Expression> fields = startsWith.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static EndsWith readEndsWith(PlanStreamInput in) throws IOException {
        return new EndsWith(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeEndsWith(PlanStreamOutput out, EndsWith endsWith) throws IOException {
        List<Expression> fields = endsWith.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static Substring readSubstring(PlanStreamInput in) throws IOException {
        return new Substring(Source.EMPTY, in.readExpression(), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    static void writeSubstring(PlanStreamOutput out, Substring substring) throws IOException {
        List<Expression> fields = substring.children();
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

    static Left readLeft(PlanStreamInput in) throws IOException {
        return new Left(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeLeft(PlanStreamOutput out, Left left) throws IOException {
        List<Expression> fields = left.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static Right readRight(PlanStreamInput in) throws IOException {
        return new Right(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeRight(PlanStreamOutput out, Right right) throws IOException {
        List<Expression> fields = right.children();
        assert fields.size() == 2;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
    }

    static Split readSplit(PlanStreamInput in) throws IOException {
        return new Split(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeSplit(PlanStreamOutput out, Split split) throws IOException {
        out.writeExpression(split.left());
        out.writeExpression(split.right());
    }

    static CIDRMatch readCIDRMatch(PlanStreamInput in) throws IOException {
        return new CIDRMatch(
            Source.EMPTY,
            in.readExpression(),
            in.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readExpression))
        );
    }

    static void writeCIDRMatch(PlanStreamOutput out, CIDRMatch cidrMatch) throws IOException {
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
        var left = in.readExpression();
        var right = in.readExpression();
        return ARITHMETIC_CTRS.get(name).apply(Source.EMPTY, left, right);
    }

    static void writeArithmeticOperation(PlanStreamOutput out, ArithmeticOperation arithmeticOperation) throws IOException {
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
        entry(name(MedianAbsoluteDeviation.class), MedianAbsoluteDeviation::new)
    );

    static AggregateFunction readAggFunction(PlanStreamInput in, String name) throws IOException {
        return AGG_CTRS.get(name).apply(Source.EMPTY, in.readExpression());
    }

    static void writeAggFunction(PlanStreamOutput out, AggregateFunction aggregateFunction) throws IOException {
        out.writeExpression(aggregateFunction.field());
    }

    // -- Multivalue functions
    static final Map<String, BiFunction<Source, Expression, AbstractMultivalueFunction>> MV_CTRS = Map.ofEntries(
        entry(name(MvAvg.class), MvAvg::new),
        entry(name(MvCount.class), MvCount::new),
        entry(name(MvDedupe.class), MvDedupe::new),
        entry(name(MvMax.class), MvMax::new),
        entry(name(MvMedian.class), MvMedian::new),
        entry(name(MvMin.class), MvMin::new),
        entry(name(MvSum.class), MvSum::new)
    );

    static AbstractMultivalueFunction readMvFunction(PlanStreamInput in, String name) throws IOException {
        return MV_CTRS.get(name).apply(Source.EMPTY, in.readExpression());
    }

    static void writeMvFunction(PlanStreamOutput out, AbstractMultivalueFunction fn) throws IOException {
        out.writeExpression(fn.field());
    }

    static MvConcat readMvConcat(PlanStreamInput in) throws IOException {
        return new MvConcat(Source.EMPTY, in.readExpression(), in.readExpression());
    }

    static void writeMvConcat(PlanStreamOutput out, MvConcat fn) throws IOException {
        out.writeExpression(fn.left());
        out.writeExpression(fn.right());
    }

    // -- NamedExpressions

    static Alias readAlias(PlanStreamInput in) throws IOException {
        return new Alias(
            Source.EMPTY,
            in.readString(),
            in.readOptionalString(),
            in.readNamed(Expression.class),
            in.nameIdFromLongValue(in.readLong()),
            in.readBoolean()
        );
    }

    static void writeAlias(PlanStreamOutput out, Alias alias) throws IOException {
        out.writeString(alias.name());
        out.writeOptionalString(alias.qualifier());
        out.writeExpression(alias.child());
        out.writeLong(Long.parseLong(alias.id().toString()));
        out.writeBoolean(alias.synthetic());
    }

    // -- Expressions (other)

    static Literal readLiteral(PlanStreamInput in) throws IOException {
        return new Literal(Source.EMPTY, in.readGenericValue(), in.dataTypeFromTypeName(in.readString()));
    }

    static void writeLiteral(PlanStreamOutput out, Literal literal) throws IOException {
        out.writeGenericValue(literal.value());
        out.writeString(literal.dataType().typeName());
    }

    static Order readOrder(PlanStreamInput in) throws IOException {
        return new org.elasticsearch.xpack.esql.expression.Order(
            Source.EMPTY,
            in.readNamed(Expression.class),
            in.readEnum(Order.OrderDirection.class),
            in.readEnum(Order.NullsPosition.class)
        );
    }

    static void writeOrder(PlanStreamOutput out, Order order) throws IOException {
        out.writeExpression(order.child());
        out.writeEnum(order.direction());
        out.writeEnum(order.nullsPosition());
    }

    // -- ancillary supporting classes of plan nodes, etc

    static EsQueryExec.FieldSort readFieldSort(PlanStreamInput in) throws IOException {
        return new EsQueryExec.FieldSort(
            readFieldAttribute(in),
            in.readEnum(Order.OrderDirection.class),
            in.readEnum(Order.NullsPosition.class)
        );
    }

    static void writeFieldSort(PlanStreamOutput out, EsQueryExec.FieldSort fieldSort) throws IOException {
        writeFieldAttribute(out, fieldSort.field());
        out.writeEnum(fieldSort.direction());
        out.writeEnum(fieldSort.nulls());
    }

    @SuppressWarnings("unchecked")
    static EsIndex readEsIndex(PlanStreamInput in) throws IOException {
        return new EsIndex(
            in.readString(),
            in.readImmutableMap(StreamInput::readString, readerFromPlanReader(PlanStreamInput::readEsFieldNamed)),
            (Set<String>) in.readGenericValue()
        );
    }

    static void writeEsIndex(PlanStreamOutput out, EsIndex esIndex) throws IOException {
        out.writeString(esIndex.name());
        out.writeMap(esIndex.mapping(), (o, v) -> out.writeNamed(EsField.class, v));
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
