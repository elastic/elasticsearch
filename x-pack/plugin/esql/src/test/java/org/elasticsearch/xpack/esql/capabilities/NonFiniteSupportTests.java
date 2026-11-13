/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.expression.function.WindowFilter;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Absent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Changes;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinctOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DefaultTimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Delta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DeltaOnlyHistogramMergeOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Deriv;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Earliest;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMerge;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HistogramMergeOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Idelta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Increase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Irate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Latest;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LegacyIrate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MaxOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MinOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Present;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramFraction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sample;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sparkline;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StddevOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Variance;
import org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.WeightedAvg;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Score;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.inference.CompletionFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.Embedding;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.ConfidenceInterval;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.Random;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatePeriod;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDenseVector;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDoubleRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToExponentialHistogram;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeohash;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeohex;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeotile;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIntegerBase;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIntegerSurrogate;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIp;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosDecimal;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosOctal;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosRejected;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLongBase;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLongSurrogate;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToTDigest;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToTimeDuration;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersion;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.UrlDecode;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.UrlEncode;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.UrlEncodeComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateDiff;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateUnitCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Day;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DayName;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Hour;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Month;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.MonthName;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Now;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.TRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.ToRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Year;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.FilterUnsupportedTemporality;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramFraction;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramPercentile;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.IpPrefix;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.NetworkDirection;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan2;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cbrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.CopySign;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.E;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Exp;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Hypot;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pi;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Scalb;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Signum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tanh;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tau;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAppend;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvConcat;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDedupe;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDifference;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvFirst;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersection;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLast;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPSeriesWeightedSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPercentile;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvRLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSort;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvUnion;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvZip;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.score.Decay;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialDisjoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StBuffer;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDifference;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StEnvelope;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohash;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohex;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeometryType;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeotile;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StIntersection;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StIsEmpty;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StNPoints;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StSimplify;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StSimplifyPreserveTopology;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StSymDifference;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StUnion;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StUnionUnary;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StX;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StXMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StXMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StY;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StYMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StYMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.BitLength;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ByteLength;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Chicken;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Chunk;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Contains;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Hash;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonString;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Left;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Locate;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Md5;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Repeat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Reverse;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Right;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Sha1;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Sha256;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Space;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Split;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.TopSnippets;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.util.Delay;
import org.elasticsearch.xpack.esql.expression.function.vector.CosineSimilarity;
import org.elasticsearch.xpack.esql.expression.function.vector.DotProduct;
import org.elasticsearch.xpack.esql.expression.function.vector.Hamming;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.function.vector.L1Norm;
import org.elasticsearch.xpack.esql.expression.function.vector.L2Norm;
import org.elasticsearch.xpack.esql.expression.function.vector.Magnitude;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;

/**
 * Forces every concrete ES|QL {@link Function}, operators included, to declare how it treats non-finite
 * ({@code NaN}/{@code ±Inf}) floating-point values, by being listed in exactly one of {@link #SUPPORTED},
 * {@link #NOT_YET_SUPPORTED} or {@link #NON_FINITE_AGNOSTIC}. This makes the decision a conscious one for every new
 * function, and keeps {@link #NOT_YET_SUPPORTED} an accurate registry of the functions whose non-finite semantics
 * still need to be addressed.
 * <p>
 *     When a function is reported as unclassified, either implement {@link NonFiniteSupport} and add the function to
 *     {@link #SUPPORTED}, or add it to {@link #NOT_YET_SUPPORTED} (stating what full support still lacks) or to
 *     {@link #NON_FINITE_AGNOSTIC}, whichever describes its behavior.
 * </p>
 */
public class NonFiniteSupportTests extends ESTestCase {

    /**
     * Functions implementing {@link NonFiniteSupport}, which can either preserve or reject non-finite values.
     */
    private static final Set<Class<? extends Function>> SUPPORTED = Set.of(
        Avg.class,
        Max.class,
        Min.class,
        Percentile.class,
        StdDev.class,
        Variance.class,
        ToDegrees.class,
        Acos.class,
        Acosh.class,
        Asin.class,
        Atanh.class,
        Cosh.class,
        Log.class,
        Log10.class,
        Pow.class,
        Round.class,
        Sinh.class,
        Sqrt.class,
        Add.class,
        Div.class,
        Mod.class,
        Mul.class,
        Sub.class
    );

    /**
     * Functions that do not implement {@link NonFiniteSupport}, but whose behavior can depend on non-finite values.
     * They compute on, compare, order, hash, convert, parse or format floating-point values (including the floating-point
     * payload of types such as {@code aggregate_metric_double}, histograms, {@code double_range} and {@code dense_vector},
     * and floating-point parameters), may null, drop, throw on or replace a non-finite input, or produce a floating-point
     * result that can be non-finite or that is rejected when it is.
     * <p>
     *     Each value states what full support still lacks: a strict mode (the ES|QL default, which typically rejects
     *     non-finite results to {@code null} with a warning), an IEEE-754 mode (non-finite values preserved), or a fix for a
     *     specific handling of non-finite values. "Strict X" means X with {@link NonFiniteSupport#allowNonFinite()} set to
     *     {@code false}, whose exact handling of non-finite values is specific to X.
     * </p>
     */
    private static final Map<Class<? extends Function>, String> NOT_YET_SUPPORTED;

    static {
        Map<Class<? extends Function>, String> map = new HashMap<>();
        put(
            map,
            org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.Neg.class,
            "Unused; folding negates NaN/±Inf as is"
        );
        put(map, AvgOverTime.class, "Numeric path always uses strict Avg; histogram path always uses IEEE-754 Div");
        put(map, Changes.class, "No strict mode: numeric NaN equals NaN, but a NaN histogram sum always counts as a change");
        put(map, Count.class, "Bucket path: a ±Inf bucket bound on a t-digest throws and fails the query");
        put(map, CountDistinct.class, "No strict mode: NaN and ±Inf are counted as distinct values");
        put(map, CountDistinctOverTime.class, "Delegates to CountDistinct, which counts NaN and ±Inf as distinct values");
        put(map, DefaultTimeSeriesAggregateFunction.class, "Histogram path delegates to HistogramMergeOverTime");
        put(map, Delta.class, "No strict mode: non-finite deltas are returned as is");
        put(map, DeltaOnlyHistogramMergeOverTime.class, "Merging skips NaN min/max, and a NaN t-digest sum");
        put(map, Deriv.class, "NaN slope becomes null without a warning; ±Inf samples usually yield a NaN slope");
        put(map, DimensionValues.class, "Delegates doubles to Values, whose grouped path can emit NaN twice");
        put(map, HistogramMerge.class, "Merging skips NaN min/max, and a NaN t-digest sum");
        put(
            map,
            HistogramMergeOverTime.class,
            "Merging skips NaN min/max and a NaN t-digest sum; the cumulative path subtracts sums unchecked"
        );
        put(map, Idelta.class, "No strict mode: non-finite differences are returned as is");
        put(map, Increase.class, "NaN increase becomes null without a warning; ±Inf samples can yield NaN too");
        put(map, Irate.class, "NaN in either sample is treated as a counter reset; a NaN latest sample yields NaN");
        put(map, LegacyIrate.class, "NaN in either sample is treated as a counter reset; a NaN latest sample yields NaN");
        put(map, MaxOverTime.class, "Numbers use strict Max, which lets NaN through and turns a lone -Inf into -Double.MAX_VALUE");
        put(map, Median.class, "Numbers delegate to strict Percentile, which throws on non-finite input");
        put(map, MedianAbsoluteDeviation.class, "No IEEE-754 mode: non-finite input makes the t-digest throw");
        put(map, MinOverTime.class, "Numbers use strict Min, which lets NaN through");
        put(map, PercentileOverTime.class, "Numbers use strict Percentile, which throws on non-finite input");
        put(map, PromqlHistogramFraction.class, "No strict mode: NaN fractions are returned");
        put(map, PromqlHistogramQuantile.class, "Classic histograms return NaN/±Inf; native ones turn NaN into null without a warning");
        put(map, Rate.class, "NaN rate becomes null without a warning; ±Inf samples can yield NaN too");
        put(map, Scalar.class, "Returns NaN unless there is exactly one value; strict Max turns a lone -Inf into -Double.MAX_VALUE");
        put(map, StddevOverTime.class, "Always delegates to strict StdDev, which nulls non-finite results without a warning");
        put(map, Sum.class, "No strict mode for doubles, except constants (strict Mul); dense_vector sums reject non-finite results");
        put(map, SumOverTime.class, "No strict mode: non-finite sums are returned as is");
        put(
            map,
            Top.class,
            "Ranks NaN highest by Double.compare; limit 1 uses strict Min/Max (NaN propagates, a lone -Inf gives -Double.MAX_VALUE)"
        );
        put(map, Values.class, "No strict mode; the grouped path emits NaN twice if a group's first value is NaN and it repeats");
        put(map, VarianceOverTime.class, "Always delegates to strict Variance, which nulls non-finite results without a warning");
        put(map, WeightedAvg.class, "No IEEE-754 mode: rewritten to strict Div, and to strict Mul for a non-constant weight");
        put(map, Kql.class, "Accepts a NaN/+Inf boost, which Lucene rejects when building the query");
        put(map, Match.class, "Accepts a NaN/+Inf boost, which Lucene rejects; a NaN query matches inconsistently across code paths");
        put(map, MatchOperator.class, "A NaN query matches inconsistently across code paths");
        put(map, MatchPhrase.class, "Accepts a NaN/+Inf boost, which Lucene rejects when building the query");
        put(map, QueryString.class, "Accepts a NaN/+Inf boost, which Lucene rejects when building the query");
        put(map, Bucket.class, "Numeric path always uses strict Div and Mul; histogram path can emit NaN ranges");
        put(map, Embedding.class, "No strict mode: embedding values from the inference service are not checked");
        put(map, TextEmbedding.class, "No strict mode: embedding values from the inference service are not checked");
        put(map, Clamp.class, "No strict mode: NaN propagates through ClampMin and ClampMax");
        put(map, ConfidenceInterval.class, "Treats NaN estimates as empty buckets; ±Inf is not handled");
        put(map, ClampMax.class, "No strict mode: NaN propagates through Math.min");
        put(map, ClampMin.class, "No strict mode: NaN propagates through Math.max");
        put(map, Greatest.class, "No strict mode: NaN propagates through Math.max");
        put(map, Least.class, "No strict mode: NaN propagates through Math.min");
        put(map, FromAggregateMetricDouble.class, "No strict mode: non-finite metrics and averages are returned as is");
        put(map, ToAggregateMetricDouble.class, "No strict mode: NaN and ±Inf are stored, except folding turns NaN metrics into null");
        put(map, ToBoolean.class, "No strict mode: NaN converts to true");
        put(map, ToDateNanos.class, "NaN silently converts to the epoch; ±Inf is rejected");
        put(map, ToDatetime.class, "NaN silently converts to the epoch; ±Inf is rejected");
        put(map, ToDenseVector.class, "No strict mode: the float cast keeps NaN/±Inf and overflows to ±Inf");
        put(map, ToDouble.class, "No IEEE-754 mode: \"NaN\" and \"Infinity\" strings are rejected");
        put(map, ToDoubleRange.class, "Parsing accepts ±Inf bounds but rejects NaN bounds");
        put(map, ToExponentialHistogram.class, "NaN sum/min/max become null; ±Inf is kept");
        put(map, ToInteger.class, "NaN silently converts to 0; ±Inf is rejected");
        put(map, ToIntegerSurrogate.class, "One-argument form delegates to ToInteger, which converts NaN to 0 and rejects ±Inf");
        put(map, ToLong.class, "NaN silently converts to 0; ±Inf is rejected");
        put(map, ToLongSurrogate.class, "One-argument form delegates to ToLong, which converts NaN to 0 and rejects ±Inf");
        put(map, ToRadians.class, "No strict mode: NaN/±Inf pass through");
        put(map, ToString.class, "No strict mode: NaN and ±Inf are formatted as text");
        put(map, ToTDigest.class, "NaN sum/min/max become null; the sum can overflow to ±Inf");
        put(map, ToUnsignedLong.class, "NaN and ±Inf are rejected");
        put(map, RangeContains.class, "Rewritten to RangeWithin, where a NaN point yields false");
        put(map, RangeIntersects.class, "No strict mode: a NaN point yields false");
        put(map, RangeMax.class, "No strict mode: ±Inf bounds are returned as is");
        put(map, RangeMin.class, "No strict mode: ±Inf bounds are returned as is");
        put(map, RangeWithin.class, "No strict mode: a NaN point yields false");
        put(map, ToRange.class, "No IEEE-754 mode: NaN bounds are rejected");
        put(map, ExtractHistogramComponent.class, "No strict mode: ±Inf components are returned as is");
        put(
            map,
            HistogramFraction.class,
            "No strict mode; NaN counts round to 0 with decimals, and a ±Inf t-digest bound throws and fails the query"
        );
        put(map, HistogramPercentile.class, "NaN result becomes null without a warning; ±Inf is returned as is");
        put(map, Abs.class, "No strict mode: NaN and +Inf pass through, -Inf becomes +Inf");
        put(map, Asinh.class, "No strict mode: NaN/±Inf pass through");
        put(map, Atan.class, "No strict mode: NaN passes through");
        put(map, Atan2.class, "No strict mode: NaN passes through");
        put(map, Cbrt.class, "No strict mode: NaN/±Inf pass through");
        put(map, Ceil.class, "No strict mode: NaN/±Inf pass through");
        put(map, CopySign.class, "No strict mode; non-double paths treat a NaN sign as positive");
        put(map, Cos.class, "No strict mode: NaN passes through and ±Inf yields NaN");
        put(map, Exp.class, "No strict mode: NaN/+Inf pass through and overflow returns +Inf");
        put(map, Floor.class, "No strict mode: NaN/±Inf pass through");
        put(map, Hypot.class, "No strict mode: NaN passes through; ±Inf input or overflow returns +Inf");
        put(map, RoundTo.class, "NaN and +Inf round to the largest point, -Inf to the smallest");
        put(map, Scalb.class, "No IEEE-754 mode: non-finite input and overflow are rejected");
        put(map, Signum.class, "No strict mode: NaN passes through");
        put(map, Sin.class, "No strict mode: NaN passes through and ±Inf yields NaN");
        put(map, Tan.class, "No strict mode: NaN passes through and ±Inf yields NaN");
        put(map, Tanh.class, "No strict mode: NaN passes through");
        put(map, MvAvg.class, "No strict mode: NaN/±Inf inputs and overflow to ±Inf propagate");
        put(map, MvContains.class, "NaN never matches by ==, but the sorted (>16 values) binary search may match it");
        put(map, MvDedupe.class, "Compares with ==, so duplicate NaNs are all kept");
        put(map, MvDifference.class, "Boxed Double equality makes NaN equal to NaN");
        put(map, MvGreater.class, "No strict mode: NaN never compares as greater");
        put(map, MvInRange.class, "No strict mode: NaN is never in range");
        put(map, MvIntersection.class, "Boxed Double equality makes NaN equal to NaN");
        put(map, MvIntersects.class, "NaN never matches by ==, but the sorted (>16 values) binary search may match it");
        put(map, MvLess.class, "No strict mode: NaN never compares as less");
        put(map, MvMax.class, "No strict mode: NaN propagates through Math.max");
        put(map, MvMedian.class, "Sorts NaN as the largest value; the middle-pair average can overflow or be NaN");
        put(map, MvMedianAbsoluteDeviation.class, "A non-finite median throws and fails the query; a single non-finite value returns 0");
        put(map, MvMin.class, "No strict mode: NaN propagates through Math.min");
        put(map, MvPSeriesWeightedSum.class, "No IEEE-754 mode: non-finite sums are rejected");
        put(map, MvPercentile.class, "Accepts a NaN percentile; a NaN or ±Inf neighbour makes interpolation non-finite");
        put(map, MvSort.class, "Orders by Double.compare: NaN last ascending, first descending");
        put(map, MvSum.class, "No strict mode: NaN/±Inf inputs propagate and double overflow returns ±Inf");
        put(map, MvUnion.class, "Boxed Double equality makes NaN equal to NaN");
        put(map, Decay.class, "No strict mode: NaN input yields a NaN score");
        put(map, StBuffer.class, "No strict mode: a NaN/±Inf distance is passed to JTS");
        put(
            map,
            StDistance.class,
            "No strict mode for cartesian points; the multi-value minimum drops NaN, and all-NaN yields Double.MAX_VALUE"
        );
        put(map, StSimplify.class, "Rejects only a negative tolerance; NaN and +Inf are accepted");
        put(map, StSimplifyPreserveTopology.class, "Rejects only a negative tolerance; NaN and +Inf are accepted");
        put(map, StX.class, "No IEEE-754 mode: non-finite coordinates are rejected");
        put(map, StXMax.class, "No IEEE-754 mode: non-finite coordinates are rejected");
        put(map, StXMin.class, "No IEEE-754 mode: non-finite coordinates are rejected");
        put(map, StY.class, "No IEEE-754 mode: non-finite coordinates are rejected");
        put(map, StYMax.class, "No IEEE-754 mode: non-finite coordinates are rejected");
        put(map, StYMin.class, "No IEEE-754 mode: non-finite coordinates are rejected");
        put(map, JsonString.class, "Writes NaN and ±Inf as JSON strings");
        put(map, CosineSimilarity.class, "No strict mode: a NaN result, such as for a zero vector, is returned");
        put(map, DotProduct.class, "No strict mode: float overflow returns ±Inf");
        put(map, Hamming.class, "Casts NaN/±Inf components to bytes");
        put(map, Knn.class, "Rejects a literal query vector with non-finite values or magnitude, only for non-indexed search");
        put(map, L1Norm.class, "No strict mode: float overflow returns +Inf");
        put(map, L2Norm.class, "No strict mode: float overflow returns +Inf");
        put(map, Magnitude.class, "No strict mode: float overflow returns +Inf");
        put(map, Range.class, "Pushdown sends NaN bounds unchecked; a NaN upper bound matches like +Inf");
        put(map, Neg.class, "No strict mode: NaN/±Inf are negated as is");
        put(map, Equals.class, "No strict mode; doubles compare with == but double_range with Double.compare");
        put(map, GreaterThan.class, "No strict mode: NaN/±Inf compare with IEEE-754 semantics");
        put(map, GreaterThanOrEqual.class, "No strict mode: NaN/±Inf compare with IEEE-754 semantics");
        put(map, In.class, "Double.compare makes NaN match NaN, unlike Equals; pushdown fails on NaN/±Inf values");
        put(map, LessThan.class, "No strict mode: NaN/±Inf compare with IEEE-754 semantics");
        put(map, LessThanOrEqual.class, "No strict mode: NaN/±Inf compare with IEEE-754 semantics");
        put(map, NotEquals.class, "Pushed down, a NaN operand matches nothing, while evaluation returns true");
        put(map, HashOffset.class, "Hashes raw double bits, so NaNs with different payloads differ");
        NOT_YET_SUPPORTED = Map.copyOf(map);
    }

    /**
     * Functions whose behavior never depends on whether a floating-point value is finite: they have no floating-point
     * input or output, or only pass such values through unchanged, including {@code NaN} and {@code ±Inf}. Coordinates of
     * geometry types are not considered floating-point values.
     */
    private static final Set<Class<? extends Function>> NON_FINITE_AGNOSTIC = Set.of(
        WindowFilter.class,
        Absent.class,
        AbsentOverTime.class,
        CountApproximate.class,
        CountOverTime.class,
        Earliest.class,
        First.class,
        FirstDocId.class,
        FirstOverTime.class,
        FromPartial.class,
        Last.class,
        LastOverTime.class,
        Latest.class,
        PackDimsAgg.class,
        Present.class,
        PresentOverTime.class,
        Sample.class,
        Sparkline.class,
        SpatialCentroid.class,
        SpatialExtent.class,
        ToPartial.class,
        Score.class,
        Categorize.class,
        TBucket.class,
        TStep.class,
        TimeSeriesWithout.class,
        CompletionFunction.class,
        Random.class,
        Case.class,
        FromBase64.class,
        ToBase64.class,
        ToCartesianPoint.class,
        ToCartesianShape.class,
        ToCounter.class,
        ToDatePeriod.class,
        ToDateRange.class,
        ToGauge.class,
        ToGeoPoint.class,
        ToGeoShape.class,
        ToGeohash.class,
        ToGeohex.class,
        ToGeotile.class,
        ToIntegerBase.class,
        ToIp.class,
        ToIpLeadingZerosDecimal.class,
        ToIpLeadingZerosOctal.class,
        ToIpLeadingZerosRejected.class,
        ToLongBase.class,
        ToText.class,
        ToTimeDuration.class,
        ToVersion.class,
        UrlDecode.class,
        UrlEncode.class,
        UrlEncodeComponent.class,
        DateDiff.class,
        DateExtract.class,
        DateFormat.class,
        DateParse.class,
        DateTrunc.class,
        DateUnitCount.class,
        Day.class,
        DayName.class,
        Hour.class,
        Month.class,
        MonthName.class,
        Now.class,
        TRange.class,
        Year.class,
        FilterUnsupportedTemporality.class,
        CIDRMatch.class,
        IpPrefix.class,
        NetworkDirection.class,
        E.class,
        Pi.class,
        Tau.class,
        MvAppend.class,
        MvConcat.class,
        MvCount.class,
        MvFirst.class,
        MvLast.class,
        MvLike.class,
        MvRLike.class,
        MvSingleValueOrNull.class,
        MvSlice.class,
        MvZip.class,
        Coalesce.class,
        SpatialContains.class,
        SpatialDisjoint.class,
        SpatialIntersects.class,
        SpatialWithin.class,
        StDifference.class,
        StDimension.class,
        StEnvelope.class,
        StGeohash.class,
        StGeohex.class,
        StGeometryType.class,
        StGeotile.class,
        StIntersection.class,
        StIsEmpty.class,
        StNPoints.class,
        StSymDifference.class,
        StUnion.class,
        StUnionUnary.class,
        BitLength.class,
        ByteLength.class,
        Chicken.class,
        Chunk.class,
        Concat.class,
        Contains.class,
        EndsWith.class,
        FieldExtract.class,
        Hash.class,
        JsonExtract.class,
        LTrim.class,
        Left.class,
        Length.class,
        Locate.class,
        Md5.class,
        RTrim.class,
        Repeat.class,
        Replace.class,
        Reverse.class,
        Right.class,
        Sha1.class,
        Sha256.class,
        Space.class,
        Split.class,
        StartsWith.class,
        Substring.class,
        ToLower.class,
        ToUpper.class,
        TopSnippets.class,
        Trim.class,
        RLike.class,
        RLikeList.class,
        WildcardLike.class,
        WildcardLikeList.class,
        Delay.class,
        And.class,
        Not.class,
        Or.class,
        IsNotNull.class,
        IsNull.class,
        InsensitiveEquals.class,
        RegexExpand.class
    );

    public void testEveryFunctionIsClassified() throws IOException {
        Set<Class<? extends Function>> listed = new HashSet<>();
        for (Set<Class<? extends Function>> classification : List.of(SUPPORTED, NOT_YET_SUPPORTED.keySet(), NON_FINITE_AGNOSTIC)) {
            assertThat("classes listed in more than one set", Sets.intersection(listed, classification), empty());
            listed.addAll(classification);
        }
        Set<Class<? extends Function>> functions = functionClasses();
        assertThat("unclassified Function classes", Sets.difference(functions, listed), empty());
        assertThat("listed classes that are not concrete Function classes", Sets.difference(listed, functions), empty());
    }

    public void testSupportedMatchesNonFiniteSupportImplementations() throws IOException {
        Set<Class<? extends Function>> implementations = functionClasses().stream()
            .filter(NonFiniteSupport.class::isAssignableFrom)
            .collect(Collectors.toSet());
        assertThat("implement NonFiniteSupport but are not in SUPPORTED", Sets.difference(implementations, SUPPORTED), empty());
        assertThat("in SUPPORTED but do not implement NonFiniteSupport", Sets.difference(SUPPORTED, implementations), empty());
    }

    private static void put(Map<Class<? extends Function>, String> map, Class<? extends Function> function, String missing) {
        if (map.putIfAbsent(function, missing) != null) {
            throw new IllegalStateException("[" + function.getName() + "] is listed more than once");
        }
    }

    private static Set<Class<? extends Function>> functionClasses() throws IOException {
        return EsqlNodeSubclassTests.subclassesOf(Function.class)
            .stream()
            // Unresolvable nodes never survive analysis, so they never evaluate anything.
            .filter(c -> Unresolvable.class.isAssignableFrom(c) == false)
            .collect(Collectors.toSet());
    }
}
