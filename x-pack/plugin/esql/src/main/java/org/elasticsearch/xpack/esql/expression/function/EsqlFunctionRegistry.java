/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Absent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinctOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Delta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Deriv;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Earliest;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Idelta;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Increase;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Irate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Latest;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MaxOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MinOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PercentileOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Present;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sample;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sparkline;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StddevOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Variance;
import org.elasticsearch.xpack.esql.expression.function.aggregate.VarianceOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.WeightedAvg;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Score;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.inference.Embedding;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBase64;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatePeriod;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDenseVector;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToExponentialHistogram;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DayName;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.MonthName;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Now;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.RangeWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.TRange;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersection;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLast;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPSeriesWeightedSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPercentile;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StEnvelope;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohash;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohex;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeometryType;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeotile;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StIsEmpty;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StNPoints;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StSimplify;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StSimplifyPreserveTopology;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Hash;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonExtract;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.util.Delay;
import org.elasticsearch.xpack.esql.expression.function.vector.CosineSimilarity;
import org.elasticsearch.xpack.esql.expression.function.vector.DotProduct;
import org.elasticsearch.xpack.esql.expression.function.vector.Hamming;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.function.vector.L1Norm;
import org.elasticsearch.xpack.esql.expression.function.vector.L2Norm;
import org.elasticsearch.xpack.esql.expression.function.vector.Magnitude;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;

public class EsqlFunctionRegistry {

    private static final Map<DataType, Integer> DATA_TYPE_CASTING_PRIORITY;

    static {
        List<DataType> typePriorityList = Arrays.asList(
            DATETIME,
            DATE_PERIOD,
            TIME_DURATION,
            DOUBLE,
            LONG,
            INTEGER,
            IP,
            VERSION,
            GEO_POINT,
            GEO_SHAPE,
            CARTESIAN_POINT,
            CARTESIAN_SHAPE,
            GEOHASH,
            GEOHEX,
            GEOTILE,
            BOOLEAN,
            UNSIGNED_LONG,
            DENSE_VECTOR,
            UNSUPPORTED
        );
        DATA_TYPE_CASTING_PRIORITY = new HashMap<>();
        for (int i = 0; i < typePriorityList.size(); i++) {
            DATA_TYPE_CASTING_PRIORITY.put(typePriorityList.get(i), i);
        }
    }

    // list of functions grouped by type of functions (aggregate, statistics, math etc) and ordered alphabetically inside each group
    // a single function will have one entry for itself with its name associated to its instance and, also, one entry for each alias
    // it has with the alias name associated to the FunctionDefinition instance
    private final Map<String, FunctionDefinition> defs = new LinkedHashMap<>();
    private final Map<String, String> aliases = new HashMap<>();
    private final Map<Class<? extends Function>, String> names = new HashMap<>();
    private final Map<Class<? extends Function>, List<DataType>> dataTypesForStringLiteralConversions = new LinkedHashMap<>();

    private SnapshotFunctionRegistry snapshotRegistry = null;

    @SuppressWarnings("this-escape")
    public EsqlFunctionRegistry() {
        register(functions());
        buildDataTypesForStringLiteralConversion(functions());
        nameSurrogates();
    }

    EsqlFunctionRegistry(FunctionDefinition... functions) {
        register(functions);
    }

    public FunctionDefinition resolveFunction(String functionName) {
        FunctionDefinition def = defs.get(functionName);
        if (def == null) {
            throw new QlIllegalArgumentException("Cannot find function {}; this should have been caught during analysis", functionName);
        }
        return def;
    }

    private String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    public String resolveAlias(String alias) {
        String normalized = normalize(alias);
        return aliases.getOrDefault(normalized, normalized);
    }

    public boolean functionExists(String functionName) {
        return defs.containsKey(functionName);
    }

    public boolean functionExists(Class<? extends Function> clazz) {
        return names.containsKey(clazz);
    }

    public String functionName(Class<? extends Function> clazz) {
        String name = names.get(clazz);
        Check.notNull(name, "Cannot find function by class {}", clazz);
        return name;
    }

    public Collection<FunctionDefinition> listFunctions() {
        // It is worth double checking if we need this copy. These are immutable anyway.
        return defs.values();
    }

    private static FunctionDefinition[][] functions() {
        return new FunctionDefinition[][] {
            // grouping functions
            new FunctionDefinition[] { Bucket.DEFINITION, Categorize.DEFINITION, TBucket.DEFINITION, TimeSeriesWithout.DEFINITION },
            // aggregate functions
            // since they declare two public constructors - one with filter (for nested where) and one without
            // use casting to disambiguate between the two
            new FunctionDefinition[] {
                Avg.DEFINITION,
                Clamp.DEFINITION,
                Count.DEFINITION,
                CountDistinct.DEFINITION,
                Max.DEFINITION,
                Median.DEFINITION,
                MedianAbsoluteDeviation.DEFINITION,
                Min.DEFINITION,
                Percentile.DEFINITION,
                Sample.DEFINITION,
                StdDev.DEFINITION,
                Variance.DEFINITION,
                Sum.DEFINITION,
                Top.DEFINITION,
                Values.DEFINITION,
                WeightedAvg.DEFINITION,
                Present.DEFINITION,
                Absent.DEFINITION,
                First.DEFINITION,
                Last.DEFINITION,
                Earliest.DEFINITION,
                Latest.DEFINITION, },
            // math
            new FunctionDefinition[] {
                Abs.DEFINITION,
                Acos.DEFINITION,
                Asin.DEFINITION,
                Atan.DEFINITION,
                Atan2.DEFINITION,
                Cbrt.DEFINITION,
                Ceil.DEFINITION,
                Cos.DEFINITION,
                Cosh.DEFINITION,
                Acosh.DEFINITION,
                Asinh.DEFINITION,
                Atanh.DEFINITION,
                E.DEFINITION,
                Exp.DEFINITION,
                Floor.DEFINITION,
                Greatest.DEFINITION,
                CopySign.DEFINITION,
                Hypot.DEFINITION,
                Log.DEFINITION,
                Log10.DEFINITION,
                Least.DEFINITION,
                ClampMax.DEFINITION,
                ClampMin.DEFINITION,
                Pi.DEFINITION,
                Pow.DEFINITION,
                Round.DEFINITION,
                RoundTo.DEFINITION,
                Scalb.DEFINITION,
                Signum.DEFINITION,
                Sin.DEFINITION,
                Sinh.DEFINITION,
                Sqrt.DEFINITION,
                Tan.DEFINITION,
                Tanh.DEFINITION,
                Tau.DEFINITION },
            // string
            new FunctionDefinition[] {
                BitLength.DEFINITION,
                ByteLength.DEFINITION,
                Chicken.DEFINITION,
                Concat.DEFINITION,
                Contains.DEFINITION,
                EndsWith.DEFINITION,
                Hash.DEFINITION,
                JsonExtract.DEFINITION,
                LTrim.DEFINITION,
                Left.DEFINITION,
                Length.DEFINITION,
                Locate.DEFINITION,
                Md5.DEFINITION,
                RTrim.DEFINITION,
                Repeat.DEFINITION,
                Replace.DEFINITION,
                Reverse.DEFINITION,
                Right.DEFINITION,
                Sha1.DEFINITION,
                Sha256.DEFINITION,
                Space.DEFINITION,
                StartsWith.DEFINITION,
                Substring.DEFINITION,
                ToLower.DEFINITION,
                ToUpper.DEFINITION,
                Trim.DEFINITION,
                UrlEncode.DEFINITION,
                UrlEncodeComponent.DEFINITION,
                UrlDecode.DEFINITION,
                Chunk.DEFINITION },
            // date
            new FunctionDefinition[] {
                DateDiff.DEFINITION,
                DateExtract.DEFINITION,
                DateFormat.DEFINITION,
                DateParse.DEFINITION,
                DateTrunc.DEFINITION,
                DayName.DEFINITION,
                MonthName.DEFINITION,
                Now.DEFINITION,
                TRange.DEFINITION },
            // spatial
            new FunctionDefinition[] {
                SpatialCentroid.DEFINITION,
                SpatialContains.DEFINITION,
                SpatialExtent.DEFINITION,
                SpatialDisjoint.DEFINITION,
                SpatialIntersects.DEFINITION,
                SpatialWithin.DEFINITION,
                StDistance.DEFINITION,
                StEnvelope.DEFINITION,
                StBuffer.DEFINITION,
                StSimplify.DEFINITION,
                StSimplifyPreserveTopology.DEFINITION,
                StGeohash.DEFINITION,
                StGeotile.DEFINITION,
                StGeohex.DEFINITION,
                StNPoints.DEFINITION,
                StGeometryType.DEFINITION,
                StDimension.DEFINITION,
                StIsEmpty.DEFINITION,
                StXMax.DEFINITION,
                StXMin.DEFINITION,
                StYMax.DEFINITION,
                StYMin.DEFINITION,
                StX.DEFINITION,
                StY.DEFINITION },
            // conditional
            new FunctionDefinition[] { Case.DEFINITION },
            // null
            new FunctionDefinition[] { Coalesce.DEFINITION, },
            // IP
            new FunctionDefinition[] { CIDRMatch.DEFINITION, IpPrefix.DEFINITION, NetworkDirection.DEFINITION },
            // conversion functions
            new FunctionDefinition[] {
                FromBase64.DEFINITION,
                ToAggregateMetricDouble.DEFINITION,
                ToBase64.DEFINITION,
                ToBoolean.DEFINITION,
                ToCartesianPoint.DEFINITION,
                ToCartesianShape.DEFINITION,
                ToDatePeriod.DEFINITION,
                ToDatetime.DEFINITION,
                ToDateNanos.DEFINITION,
                ToDegrees.DEFINITION,
                ToDenseVector.DEFINITION,
                ToDouble.DEFINITION,
                ToExponentialHistogram.DEFINITION,
                ToGeohash.DEFINITION,
                ToGeotile.DEFINITION,
                ToGeohex.DEFINITION,
                ToGeoPoint.DEFINITION,
                ToGeoShape.DEFINITION,
                ToIp.DEFINITION,
                ToIntegerSurrogate.DEFINITION,
                ToLongSurrogate.DEFINITION,
                ToRadians.DEFINITION,
                ToString.DEFINITION,
                ToTDigest.DEFINITION,
                ToTimeDuration.DEFINITION,
                ToUnsignedLong.DEFINITION,
                ToVersion.DEFINITION, },
            // multivalue functions
            new FunctionDefinition[] {
                MvAppend.DEFINITION,
                MvAvg.DEFINITION,
                MvConcat.DEFINITION,
                MvContains.DEFINITION,
                MvCount.DEFINITION,
                MvDedupe.DEFINITION,
                MvDifference.DEFINITION,
                MvFirst.DEFINITION,
                MvIntersection.DEFINITION,
                MvLast.DEFINITION,
                MvMax.DEFINITION,
                MvMedian.DEFINITION,
                MvMedianAbsoluteDeviation.DEFINITION,
                MvMin.DEFINITION,
                MvIntersects.DEFINITION,
                MvPercentile.DEFINITION,
                MvPSeriesWeightedSum.DEFINITION,
                MvSort.DEFINITION,
                MvSlice.DEFINITION,
                MvUnion.DEFINITION,
                MvZip.DEFINITION,
                MvSum.DEFINITION,
                Split.DEFINITION },
            // search functions
            new FunctionDefinition[] {
                Decay.DEFINITION,
                Kql.DEFINITION,
                Knn.DEFINITION,
                Match.DEFINITION,
                QueryString.DEFINITION,
                MatchPhrase.DEFINITION,
                Score.DEFINITION,
                TopSnippets.DEFINITION },
            // time-series functions
            new FunctionDefinition[] {
                Rate.DEFINITION,
                Irate.DEFINITION,
                Idelta.DEFINITION,
                Delta.DEFINITION,
                Increase.DEFINITION,
                Deriv.DEFINITION,
                MaxOverTime.DEFINITION,
                MinOverTime.DEFINITION,
                SumOverTime.DEFINITION,
                StddevOverTime.DEFINITION,
                VarianceOverTime.DEFINITION,
                CountOverTime.DEFINITION,
                CountDistinctOverTime.DEFINITION,
                PresentOverTime.DEFINITION,
                AbsentOverTime.DEFINITION,
                AvgOverTime.DEFINITION,
                LastOverTime.DEFINITION,
                FirstOverTime.DEFINITION,
                PercentileOverTime.DEFINITION,
                // dense vector functions
                TextEmbedding.DEFINITION,
                Embedding.DEFINITION,
                CosineSimilarity.DEFINITION,
                DotProduct.DEFINITION,
                L1Norm.DEFINITION,
                L2Norm.DEFINITION,
                Hamming.DEFINITION } };
    }

    private static FunctionDefinition[][] snapshotFunctions() {
        return new FunctionDefinition[][] {
            new FunctionDefinition[] {
                // The delay() function is for debug/snapshot environments only and should never be enabled in a non-snapshot build.
                // This is an experimental function and can be removed without notice.
                Delay.DEFINITION,
                // dense vector functions
                Magnitude.DEFINITION,
                // date_range functions
                RangeMax.DEFINITION,
                RangeMin.DEFINITION,
                RangeWithin.DEFINITION,
                ToDateRange.DEFINITION,
                Sparkline.DEFINITION } };
    }

    public EsqlFunctionRegistry snapshotRegistry() {
        if (Build.current().isSnapshot() == false) {
            return this;
        }
        var snapshotRegistry = this.snapshotRegistry;
        if (snapshotRegistry == null) {
            snapshotRegistry = new SnapshotFunctionRegistry();
            this.snapshotRegistry = snapshotRegistry;
        }
        return snapshotRegistry;
    }

    public static boolean isSnapshotOnly(String functionName) {
        for (FunctionDefinition[] defs : snapshotFunctions()) {
            for (FunctionDefinition def : defs) {
                if (def.name().equalsIgnoreCase(functionName)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String normalizeName(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    public static class ArgSignature {

        public record Hint(String entityType, Map<String, String> constraints) {}

        protected final String name;
        protected final String[] type;
        protected final String description;
        protected final boolean optional;
        protected final boolean variadic;
        protected final DataType targetDataType;
        protected final Hint hint;
        protected final String appliesTo;

        public ArgSignature(
            String name,
            String[] type,
            String description,
            boolean optional,
            boolean variadic,
            Hint hint,
            DataType targetDataType,
            String appliesTo
        ) {
            this.name = name;
            this.type = type;
            this.description = description;
            this.optional = optional;
            this.variadic = variadic;
            this.targetDataType = targetDataType;
            this.hint = hint;
            this.appliesTo = appliesTo;
        }

        public ArgSignature(
            String name,
            String[] type,
            String description,
            boolean optional,
            boolean variadic,
            Hint hint,
            DataType targetDataType
        ) {
            this(name, type, description, optional, variadic, hint, targetDataType, "");
        }

        public ArgSignature(String name, String[] type, String description, boolean optional, Hint hint, boolean variadic) {
            this(name, type, description, optional, variadic, hint, UNSUPPORTED, "");
        }

        public ArgSignature(String name, String[] type, String description, boolean optional, boolean variadic, DataType targetDataType) {
            this(name, type, description, optional, variadic, null, targetDataType, "");
        }

        public ArgSignature(String name, String[] type, String description, boolean optional, boolean variadic) {
            this(name, type, description, optional, variadic, null, UNSUPPORTED, "");
        }

        public String name() {
            return name;
        }

        public String[] type() {
            return type;
        }

        public String description() {
            return description;
        }

        public boolean optional() {
            return optional;
        }

        public boolean variadic() {
            return variadic;
        }

        public DataType targetDataType() {
            return targetDataType;
        }

        public Map<String, MapEntryArgSignature> mapParams() {
            return Map.of();
        }

        public String appliesTo() {
            return appliesTo;
        }

        public boolean mapArg() {
            return false;
        }

        @Override
        public String toString() {
            return "ArgSignature{"
                + "name='"
                + name
                + "', type="
                + Arrays.toString(type)
                + ", description='"
                + description
                + "', optional="
                + optional
                + ", targetDataType="
                + targetDataType
                + "}}";
        }
    }

    public static class MapArgSignature extends ArgSignature {
        private final Map<String, MapEntryArgSignature> mapParams;

        public MapArgSignature(
            String name,
            String description,
            boolean optional,
            Map<String, MapEntryArgSignature> mapParams,
            String appliesTo
        ) {
            super(name, new String[] { "map" }, description, optional, false, null, UNSUPPORTED, appliesTo);
            this.mapParams = mapParams;
        }

        public MapArgSignature(String name, String description, boolean optional, Map<String, MapEntryArgSignature> mapParams) {
            this(name, description, optional, mapParams, "");
        }

        @Override
        public Map<String, MapEntryArgSignature> mapParams() {
            return mapParams;
        }

        @Override
        public boolean mapArg() {
            return true;
        }

        @Override
        public String toString() {
            return "MapArgSignature{"
                + "name='map', type='map', description='"
                + description
                + "', optional="
                + optional
                + ", targetDataType=unsupported, mapParams={"
                + mapParams.values().stream().map(mapArg -> "{" + mapArg + "}").collect(Collectors.joining(", "))
                + "}}";
        }
    }

    public record MapEntryArgSignature(String name, String valueHint, String type, String description, String appliesTo) {
        @Override
        public String toString() {
            return "name='" + name + "', values=" + valueHint + ", description='" + description + "', type=" + type;
        }
    }

    public record FunctionDescription(
        String name,
        List<ArgSignature> args,
        String[] returnType,
        String description,
        boolean variadic,
        FunctionType type
    ) {
        /**
         * The name of every argument.
         */
        public List<String> argNames() {
            return args.stream().map(ArgSignature::name).toList();
        }

        /**
         * The signature of every argument.
         */
        public List<ArgSignature> args() {
            return args;
        }

        /**
         * The description of every argument.
         */
        public List<String> argDescriptions() {
            return args.stream().map(ArgSignature::description).toList();
        }
    }

    /**
     * Build a list target data types, which is used by ImplicitCasting to convert string literals to a target data type.
     */
    private static DataType getTargetType(String[] names) {
        List<DataType> types = new ArrayList<>();
        for (String name : names) {
            DataType type = DataType.fromTypeName(name);
            if (type != null && type != UNSUPPORTED) { // A type should not be null or UNSUPPORTED, just a sanity check here
                // If the function takes strings as input, there is no need to cast a string literal to it.
                // Return UNSUPPORTED means that ImplicitCasting doesn't support this argument, and it will be skipped by ImplicitCasting.
                if (isString(type)) {
                    return UNSUPPORTED;
                }
                types.add(type);
            }
        }

        return types.stream()
            .filter(DATA_TYPE_CASTING_PRIORITY::containsKey)
            .min(Comparator.comparing(DATA_TYPE_CASTING_PRIORITY::get))
            .orElse(UNSUPPORTED);
    }

    public static FunctionDescription description(FunctionDefinition def) {
        Constructor<?> constructor = constructorFor(def.clazz());
        if (constructor == null) {
            return new FunctionDescription(def.name(), List.of(), null, null, false, FunctionType.SCALAR);
        }
        FunctionInfo functionInfo = functionInfo(def);
        String functionDescription = functionInfo == null ? "" : functionInfo.description().replace('\n', ' ');
        String[] returnType = functionInfo == null ? new String[] { "?" } : removeUnderConstruction(functionInfo.returnType());
        var params = constructor.getParameters(); // no multiple c'tors supported

        List<EsqlFunctionRegistry.ArgSignature> args = new ArrayList<>(params.length);
        boolean variadic = false;
        int countOfParamsToDescribe = params.length;
        if (TemporalityAware.class.isAssignableFrom(def.clazz())) {
            countOfParamsToDescribe -= 2; // skip the implicit @timestamp and temporality parameter (last or last before Configuration)
        } else if (TimestampAware.class.isAssignableFrom(def.clazz())) {
            countOfParamsToDescribe--; // skip the implicit @timestamp parameter (last or last before Configuration)
        }
        if (ConfigurationFunction.class.isAssignableFrom(def.clazz())) {
            // this isn't enforced by the contract, but the convention is: func(..., Expression timestamp, Configuration config)
            assert Configuration.class.isAssignableFrom(params[params.length - 1].getType())
                : "The configuration parameter must be the last argument of an EsqlConfigurationFunction definition";
            countOfParamsToDescribe--; // skip the Configuration parameter
        }
        for (int i = 1; i < countOfParamsToDescribe; i++) { // skipping 1st argument, the source
            boolean isList = List.class.isAssignableFrom(params[i].getType());
            variadic |= isList;
            MapParam mapParamInfo = params[i].getAnnotation(MapParam.class); // refactor this
            if (mapParamInfo != null) {
                args.add(mapParam(mapParamInfo));
            } else {
                Param paramInfo = params[i].getAnnotation(Param.class);
                args.add(paramInfo != null ? param(paramInfo, isList) : paramWithoutAnnotation(params[i].getName()));
            }
        }
        return new FunctionDescription(def.name(), args, returnType, functionDescription, variadic, functionInfo.type());
    }

    public static ArgSignature param(Param param, boolean variadic) {
        String[] type = removeUnderConstruction(param.type());
        String desc = param.description().replace('\n', ' ');
        DataType targetDataType = getTargetType(type);
        ArgSignature.Hint hint = null;
        if (param.hint() != null && param.hint().entityType() != Param.Hint.ENTITY_TYPE.NONE) {
            Map<String, String> constraints = Arrays.stream(param.hint().constraints())
                .collect(Collectors.toMap(Param.Hint.Constraint::name, Param.Hint.Constraint::value));
            hint = new ArgSignature.Hint(param.hint().entityType().name().toLowerCase(Locale.ROOT), constraints);
        }

        return new EsqlFunctionRegistry.ArgSignature(
            param.name(),
            type,
            desc,
            param.optional(),
            variadic,
            hint,
            targetDataType,
            param.applies_to()
        );
    }

    public static ArgSignature mapParam(MapParam mapParam) {
        String desc = mapParam.description().replace('\n', ' ');
        // This method is used when generating the docs, so we use a LinkedHashMap to preserve the order in which the params are defined.
        Map<String, MapEntryArgSignature> params = new LinkedHashMap<>();
        for (MapParam.MapParamEntry param : mapParam.params()) {
            String valueHint = param.valueHint().length <= 1
                ? Arrays.toString(param.valueHint())
                : "[" + String.join(", ", param.valueHint()) + "]";
            String type = param.type().length <= 1 ? Arrays.toString(param.type()) : "[" + String.join(", ", param.type()) + "]";
            MapEntryArgSignature mapArg = new MapEntryArgSignature(param.name(), valueHint, type, param.description(), param.applies_to());
            params.put(param.name(), mapArg);
        }
        return new EsqlFunctionRegistry.MapArgSignature(mapParam.name(), desc, mapParam.optional(), params, mapParam.applies_to());
    }

    public static ArgSignature paramWithoutAnnotation(String name) {
        return new EsqlFunctionRegistry.ArgSignature(name, new String[] { "?" }, "", false, false, UNSUPPORTED);
    }

    /**
     * Remove types that are being actively built.
     */
    private static String[] removeUnderConstruction(String[] types) {
        for (DataType underConstruction : DataType.UNDER_CONSTRUCTION) {
            if (underConstruction.supportedVersion().supportedLocally() == false) {
                types = Arrays.stream(types).filter(t -> underConstruction.typeName().equals(t) == false).toArray(String[]::new);
            }
        }
        return types;
    }

    public static FunctionInfo functionInfo(FunctionDefinition def) {
        Constructor<?> constructor = constructorFor(def.clazz());
        if (constructor == null) {
            return null;
        }
        return constructor.getAnnotation(FunctionInfo.class);
    }

    private static Constructor<?> constructorFor(Class<? extends Function> clazz) {
        Constructor<?>[] constructors = clazz.getConstructors();
        if (constructors.length == 0) {
            return null;
        }
        // when dealing with multiple, pick the constructor exposing the FunctionInfo annotation
        if (constructors.length > 1) {
            for (Constructor<?> constructor : constructors) {
                if (constructor.getAnnotation(FunctionInfo.class) != null) {
                    return constructor;
                }
            }
        }
        return constructors[0];
    }

    public List<DataType> getDataTypeForStringLiteralConversion(Class<? extends Function> clazz) {
        return dataTypesForStringLiteralConversions.get(clazz);
    }

    private static class SnapshotFunctionRegistry extends EsqlFunctionRegistry {
        SnapshotFunctionRegistry() {
            if (Build.current().isSnapshot() == false) {
                throw new IllegalStateException("build snapshot function registry for non-snapshot build");
            }
            register(snapshotFunctions());
            buildDataTypesForStringLiteralConversion(snapshotFunctions());
        }

    }

    void register(FunctionDefinition[]... groupFunctions) {
        for (FunctionDefinition[] group : groupFunctions) {
            register(group);
        }
    }

    void register(FunctionDefinition... functions) {
        // temporary map to hold [function_name/alias_name : function instance]
        Map<String, FunctionDefinition> batchMap = new HashMap<>();
        for (FunctionDefinition f : functions) {
            batchMap.put(f.name(), f);
            for (String alias : f.aliases()) {
                Object old = batchMap.put(alias, f);
                if (old != null || defs.containsKey(alias)) {
                    throw new QlIllegalArgumentException(
                        "alias ["
                            + alias
                            + "] is used by "
                            + "["
                            + (old != null ? old : defs.get(alias).name())
                            + "] and ["
                            + f.name()
                            + "]"
                    );
                }
                aliases.put(alias, f.name());
            }
            Check.isTrue(
                names.containsKey(f.clazz()) == false,
                "function type [{}} is registered twice with names [{}] and [{}]",
                f.clazz(),
                names.get(f.clazz()),
                f.name()
            );
            names.put(f.clazz(), f.name());
        }
        // sort the temporary map by key name and add it to the global map of functions
        defs.putAll(
            batchMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(
                    Collectors.<
                        Map.Entry<String, FunctionDefinition>,
                        String,
                        FunctionDefinition,
                        LinkedHashMap<String, FunctionDefinition>>toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue,
                            LinkedHashMap::new
                        )
                )
        );
    }

    protected void buildDataTypesForStringLiteralConversion(FunctionDefinition[]... groupFunctions) {
        for (FunctionDefinition[] group : groupFunctions) {
            for (FunctionDefinition def : group) {
                FunctionDescription signature = description(def);
                dataTypesForStringLiteralConversions.put(
                    def.clazz(),
                    signature.args().stream().map(EsqlFunctionRegistry.ArgSignature::targetDataType).collect(Collectors.toList())
                );
            }
        }
    }

    /**
     * Add {@link #names} entries for functions that are not registered, but we rewrite to using {@link SurrogateExpression}.
     */
    private void nameSurrogates() {
        names.put(ToIpLeadingZerosRejected.class, "TO_IP");
        names.put(ToIpLeadingZerosDecimal.class, "TO_IP");
        names.put(ToIpLeadingZerosOctal.class, "TO_IP");

        names.put(ToLongBase.class, "TO_LONG");
        names.put(ToLong.class, "TO_LONG");

        names.put(ToIntegerBase.class, "TO_INTEGER");
        names.put(ToInteger.class, "TO_INTEGER");
    }

    /**
     * Add capabilities for registered functions to the set of capabilities.
     */
    public void addCapabilities(EsqlCapabilities.Builder capabilities) {
        Set<String> filterAliases = new HashSet<>();
        EsqlFunctionRegistry snapshots = snapshotRegistry();
        if (snapshots != null) {
            snapshots.addCapabilities(filterAliases, capabilities, true);
        } else {
            addCapabilities(filterAliases, capabilities, true);
            if (capabilities.all()) {
                new SnapshotFunctionRegistry().addCapabilities(filterAliases, capabilities, false);
            }
        }
    }

    protected final void addCapabilities(Set<String> filterAliases, EsqlCapabilities.Builder capabilities, boolean enabled) {
        for (FunctionDefinition def : defs.values()) {
            if (false == filterAliases.add(def.name())) {
                continue;
            }
            String name = "fn_" + def.name();
            capabilities.add(name, enabled);
            for (String sub : def.capabilities()) {
                capabilities.add(name + "_" + sub, enabled);
            }
        }
    }

}
