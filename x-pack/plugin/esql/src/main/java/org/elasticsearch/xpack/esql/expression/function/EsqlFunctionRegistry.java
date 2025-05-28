/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.Build;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MaxOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MinOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sample;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialExtent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.aggregate.WeightedAvg;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MultiMatch;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Term;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIp;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosDecimal;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosOctal;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosRejected;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToTimeDuration;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDedupe;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvFirst;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvZip;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialDisjoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StEnvelope;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StX;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StXMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StXMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StY;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StYMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StYMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.BitLength;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ByteLength;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Hash;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.esql.expression.function.scalar.util.Delay;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
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
            BOOLEAN,
            UNSIGNED_LONG,
            UNSUPPORTED
        );
        DATA_TYPE_CASTING_PRIORITY = new HashMap<>();
        for (int i = 0; i < typePriorityList.size(); i++) {
            DATA_TYPE_CASTING_PRIORITY.put(typePriorityList.get(i), i);
        }
    }

    // Translation table for error messaging in the following function
    private static final String[] NUM_NAMES = { "zero", "one", "two", "three", "four", "five", };

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
            new FunctionDefinition[] {
                def(Bucket.class, Bucket::new, "bucket", "bin"),
                def(Categorize.class, Categorize::new, "categorize") },
            // aggregate functions
            // since they declare two public constructors - one with filter (for nested where) and one without
            // use casting to disambiguate between the two
            new FunctionDefinition[] {
                def(Avg.class, uni(Avg::new), "avg"),
                def(Count.class, uni(Count::new), "count"),
                def(CountDistinct.class, bi(CountDistinct::new), "count_distinct"),
                def(Max.class, uni(Max::new), "max"),
                def(Median.class, uni(Median::new), "median"),
                def(MedianAbsoluteDeviation.class, uni(MedianAbsoluteDeviation::new), "median_absolute_deviation"),
                def(Min.class, uni(Min::new), "min"),
                def(Percentile.class, bi(Percentile::new), "percentile"),
                def(Sample.class, bi(Sample::new), "sample"),
                def(StdDev.class, uni(StdDev::new), "std_dev"),
                def(Sum.class, uni(Sum::new), "sum"),
                def(Top.class, tri(Top::new), "top"),
                def(Values.class, uni(Values::new), "values"),
                def(WeightedAvg.class, bi(WeightedAvg::new), "weighted_avg") },
            // math
            new FunctionDefinition[] {
                def(Abs.class, Abs::new, "abs"),
                def(Acos.class, Acos::new, "acos"),
                def(Asin.class, Asin::new, "asin"),
                def(Atan.class, Atan::new, "atan"),
                def(Atan2.class, Atan2::new, "atan2"),
                def(Cbrt.class, Cbrt::new, "cbrt"),
                def(Ceil.class, Ceil::new, "ceil"),
                def(Cos.class, Cos::new, "cos"),
                def(Cosh.class, Cosh::new, "cosh"),
                def(E.class, E::new, "e"),
                def(Exp.class, Exp::new, "exp"),
                def(Floor.class, Floor::new, "floor"),
                def(Greatest.class, Greatest::new, "greatest"),
                def(Hypot.class, Hypot::new, "hypot"),
                def(Log.class, Log::new, "log"),
                def(Log10.class, Log10::new, "log10"),
                def(Least.class, Least::new, "least"),
                def(Pi.class, Pi::new, "pi"),
                def(Pow.class, Pow::new, "pow"),
                def(Round.class, Round::new, "round"),
                def(RoundTo.class, RoundTo::new, "round_to"),
                def(Scalb.class, Scalb::new, "scalb"),
                def(Signum.class, Signum::new, "signum"),
                def(Sin.class, Sin::new, "sin"),
                def(Sinh.class, Sinh::new, "sinh"),
                def(Sqrt.class, Sqrt::new, "sqrt"),
                def(Tan.class, Tan::new, "tan"),
                def(Tanh.class, Tanh::new, "tanh"),
                def(Tau.class, Tau::new, "tau") },
            // string
            new FunctionDefinition[] {
                def(BitLength.class, BitLength::new, "bit_length"),
                def(ByteLength.class, ByteLength::new, "byte_length"),
                def(Concat.class, Concat::new, "concat"),
                def(EndsWith.class, EndsWith::new, "ends_with"),
                def(Hash.class, Hash::new, "hash"),
                def(LTrim.class, LTrim::new, "ltrim"),
                def(Left.class, Left::new, "left"),
                def(Length.class, Length::new, "length"),
                def(Locate.class, Locate::new, "locate"),
                def(Md5.class, Md5::new, "md5"),
                def(RTrim.class, RTrim::new, "rtrim"),
                def(Repeat.class, Repeat::new, "repeat"),
                def(Replace.class, Replace::new, "replace"),
                def(Reverse.class, Reverse::new, "reverse"),
                def(Right.class, Right::new, "right"),
                def(Sha1.class, Sha1::new, "sha1"),
                def(Sha256.class, Sha256::new, "sha256"),
                def(Space.class, Space::new, "space"),
                def(StartsWith.class, StartsWith::new, "starts_with"),
                def(Substring.class, Substring::new, "substring"),
                def(ToLower.class, ToLower::new, "to_lower"),
                def(ToUpper.class, ToUpper::new, "to_upper"),
                def(Trim.class, Trim::new, "trim") },
            // date
            new FunctionDefinition[] {
                def(DateDiff.class, DateDiff::new, "date_diff"),
                def(DateExtract.class, DateExtract::new, "date_extract"),
                def(DateFormat.class, DateFormat::new, "date_format"),
                def(DateParse.class, DateParse::new, "date_parse"),
                def(DateTrunc.class, DateTrunc::new, "date_trunc"),
                def(Now.class, Now::new, "now") },
            // spatial
            new FunctionDefinition[] {
                def(SpatialCentroid.class, SpatialCentroid::new, "st_centroid_agg"),
                def(SpatialContains.class, SpatialContains::new, "st_contains"),
                def(SpatialExtent.class, SpatialExtent::new, "st_extent_agg"),
                def(SpatialDisjoint.class, SpatialDisjoint::new, "st_disjoint"),
                def(SpatialIntersects.class, SpatialIntersects::new, "st_intersects"),
                def(SpatialWithin.class, SpatialWithin::new, "st_within"),
                def(StDistance.class, StDistance::new, "st_distance"),
                def(StEnvelope.class, StEnvelope::new, "st_envelope"),
                def(StXMax.class, StXMax::new, "st_xmax"),
                def(StXMin.class, StXMin::new, "st_xmin"),
                def(StYMax.class, StYMax::new, "st_ymax"),
                def(StYMin.class, StYMin::new, "st_ymin"),
                def(StX.class, StX::new, "st_x"),
                def(StY.class, StY::new, "st_y") },
            // conditional
            new FunctionDefinition[] { def(Case.class, Case::new, "case") },
            // null
            new FunctionDefinition[] { def(Coalesce.class, Coalesce::new, "coalesce"), },
            // IP
            new FunctionDefinition[] { def(CIDRMatch.class, CIDRMatch::new, "cidr_match") },
            new FunctionDefinition[] { def(IpPrefix.class, IpPrefix::new, "ip_prefix") },
            // conversion functions
            new FunctionDefinition[] {
                def(FromBase64.class, FromBase64::new, "from_base64"),
                def(ToAggregateMetricDouble.class, ToAggregateMetricDouble::new, "to_aggregate_metric_double", "to_aggregatemetricdouble"),
                def(ToBase64.class, ToBase64::new, "to_base64"),
                def(ToBoolean.class, ToBoolean::new, "to_boolean", "to_bool"),
                def(ToCartesianPoint.class, ToCartesianPoint::new, "to_cartesianpoint"),
                def(ToCartesianShape.class, ToCartesianShape::new, "to_cartesianshape"),
                def(ToDatePeriod.class, ToDatePeriod::new, "to_dateperiod"),
                def(ToDatetime.class, ToDatetime::new, "to_datetime", "to_dt"),
                def(ToDateNanos.class, ToDateNanos::new, "to_date_nanos", "to_datenanos"),
                def(ToDegrees.class, ToDegrees::new, "to_degrees"),
                def(ToDouble.class, ToDouble::new, "to_double", "to_dbl"),
                def(ToGeoPoint.class, ToGeoPoint::new, "to_geopoint"),
                def(ToGeoShape.class, ToGeoShape::new, "to_geoshape"),
                def(ToIp.class, ToIp::new, "to_ip"),
                def(ToInteger.class, ToInteger::new, "to_integer", "to_int"),
                def(ToLong.class, ToLong::new, "to_long"),
                def(ToRadians.class, ToRadians::new, "to_radians"),
                def(ToString.class, ToString::new, "to_string", "to_str"),
                def(ToTimeDuration.class, ToTimeDuration::new, "to_timeduration"),
                def(ToUnsignedLong.class, ToUnsignedLong::new, "to_unsigned_long", "to_ulong", "to_ul"),
                def(ToVersion.class, ToVersion::new, "to_version", "to_ver"), },
            // multivalue functions
            new FunctionDefinition[] {
                def(MvAppend.class, MvAppend::new, "mv_append"),
                def(MvAvg.class, MvAvg::new, "mv_avg"),
                def(MvConcat.class, MvConcat::new, "mv_concat"),
                def(MvCount.class, MvCount::new, "mv_count"),
                def(MvDedupe.class, MvDedupe::new, "mv_dedupe"),
                def(MvFirst.class, MvFirst::new, "mv_first"),
                def(MvLast.class, MvLast::new, "mv_last"),
                def(MvMax.class, MvMax::new, "mv_max"),
                def(MvMedian.class, MvMedian::new, "mv_median"),
                def(MvMedianAbsoluteDeviation.class, MvMedianAbsoluteDeviation::new, "mv_median_absolute_deviation"),
                def(MvMin.class, MvMin::new, "mv_min"),
                def(MvPercentile.class, MvPercentile::new, "mv_percentile"),
                def(MvPSeriesWeightedSum.class, MvPSeriesWeightedSum::new, "mv_pseries_weighted_sum"),
                def(MvSort.class, MvSort::new, "mv_sort"),
                def(MvSlice.class, MvSlice::new, "mv_slice"),
                def(MvZip.class, MvZip::new, "mv_zip"),
                def(MvSum.class, MvSum::new, "mv_sum"),
                def(Split.class, Split::new, "split") },
            // fulltext functions
            new FunctionDefinition[] {
                def(Kql.class, uni(Kql::new), "kql"),
                def(Match.class, tri(Match::new), "match"),
                def(MultiMatch.class, MultiMatch::new, "multi_match"),
                def(QueryString.class, bi(QueryString::new), "qstr") } };

    }

    private static FunctionDefinition[][] snapshotFunctions() {
        return new FunctionDefinition[][] {
            new FunctionDefinition[] {
                // The delay() function is for debug/snapshot environments only and should never be enabled in a non-snapshot build.
                // This is an experimental function and can be removed without notice.
                def(Delay.class, Delay::new, "delay"),
                def(Rate.class, Rate::withUnresolvedTimestamp, "rate"),
                def(MaxOverTime.class, uni(MaxOverTime::new), "max_over_time"),
                def(MinOverTime.class, uni(MinOverTime::new), "min_over_time"),
                def(SumOverTime.class, uni(SumOverTime::new), "sum_over_time"),
                def(AvgOverTime.class, uni(AvgOverTime::new), "avg_over_time"),
                def(LastOverTime.class, LastOverTime::withUnresolvedTimestamp, "last_over_time"),
                def(FirstOverTime.class, FirstOverTime::withUnresolvedTimestamp, "first_over_time"),
                def(Term.class, bi(Term::new), "term") } };
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
        protected final String name;
        protected final String[] type;
        protected final String description;
        protected final boolean optional;
        protected final boolean variadic;
        protected final DataType targetDataType;

        public ArgSignature(String name, String[] type, String description, boolean optional, boolean variadic, DataType targetDataType) {
            this.name = name;
            this.type = type;
            this.description = description;
            this.optional = optional;
            this.variadic = variadic;
            this.targetDataType = targetDataType;
        }

        public ArgSignature(String name, String[] type, String description, boolean optional, boolean variadic) {
            this(name, type, description, optional, variadic, UNSUPPORTED);
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

        public DataType targetDataType() {
            return targetDataType;
        }

        public Map<String, MapEntryArgSignature> mapParams() {
            return Map.of();
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

        public MapArgSignature(String name, String description, boolean optional, Map<String, MapEntryArgSignature> mapParams) {
            super(name, new String[] { "map" }, description, optional, false);
            this.mapParams = mapParams;
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

    public record MapEntryArgSignature(String name, String valueHint, String type, String description) {
        @Override
        public String toString() {
            return "name='" + name + "', values=" + valueHint + ", description='" + description + "'";
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
            .min((dt1, dt2) -> DATA_TYPE_CASTING_PRIORITY.get(dt1).compareTo(DATA_TYPE_CASTING_PRIORITY.get(dt2)))
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
        for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
            if (Configuration.class.isAssignableFrom(params[i].getType()) == false) {
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
        }
        return new FunctionDescription(def.name(), args, returnType, functionDescription, variadic, functionInfo.type());
    }

    public static ArgSignature param(Param param, boolean variadic) {
        String[] type = removeUnderConstruction(param.type());
        String desc = param.description().replace('\n', ' ');
        DataType targetDataType = getTargetType(type);
        return new EsqlFunctionRegistry.ArgSignature(param.name(), type, desc, param.optional(), variadic, targetDataType);
    }

    public static ArgSignature mapParam(MapParam mapParam) {
        String desc = mapParam.description().replace('\n', ' ');
        Map<String, MapEntryArgSignature> params = new HashMap<>(mapParam.params().length);
        for (MapParam.MapParamEntry param : mapParam.params()) {
            String valueHint = param.valueHint().length <= 1
                ? Arrays.toString(param.valueHint())
                : "[" + String.join(", ", param.valueHint()) + "]";
            String type = param.type().length <= 1 ? Arrays.toString(param.type()) : "[" + String.join(", ", param.type()) + "]";
            MapEntryArgSignature mapArg = new MapEntryArgSignature(param.name(), valueHint, type, param.description());
            params.put(param.name(), mapArg);
        }
        return new EsqlFunctionRegistry.MapArgSignature(mapParam.name(), desc, mapParam.optional(), params);
    }

    public static ArgSignature paramWithoutAnnotation(String name) {
        return new EsqlFunctionRegistry.ArgSignature(name, new String[] { "?" }, "", false, false, UNSUPPORTED);
    }

    /**
     * Remove types that are being actively built.
     */
    private static String[] removeUnderConstruction(String[] types) {
        for (Map.Entry<DataType, FeatureFlag> underConstruction : DataType.UNDER_CONSTRUCTION.entrySet()) {
            if (underConstruction.getValue().isEnabled() == false) {
                types = Arrays.stream(types).filter(t -> underConstruction.getKey().typeName().equals(t) == false).toArray(String[]::new);
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
    }

    protected interface FunctionBuilder {
        Function build(Source source, List<Expression> children, Configuration cfg);
    }

    /**
     * Main method to register a function.
     *
     * @param names Must always have at least one entry which is the method's primary name
     */
    @SuppressWarnings("overloads")
    protected static FunctionDefinition def(Class<? extends Function> function, FunctionBuilder builder, String... names) {
        Check.isTrue(names.length > 0, "At least one name must be provided for the function");
        String primaryName = names[0];
        List<String> aliases = Arrays.asList(names).subList(1, names.length);
        FunctionDefinition.Builder realBuilder = (uf, cfg, extras) -> {
            if (CollectionUtils.isEmpty(extras) == false) {
                throw new ParsingException(
                    uf.source(),
                    "Unused parameters {} detected when building [{}]",
                    Arrays.toString(extras),
                    primaryName
                );
            }
            try {
                return builder.build(uf.source(), uf.children(), cfg);
            } catch (QlIllegalArgumentException e) {
                throw new ParsingException(e, uf.source(), "error building [{}]: {}", primaryName, e.getMessage());
            }
        };
        return new FunctionDefinition(primaryName, unmodifiableList(aliases), function, realBuilder);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function.
     */
    public static <T extends Function> FunctionDefinition def(
        Class<T> function,
        java.util.function.Function<Source, T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (false == children.isEmpty()) {
                throw new QlIllegalArgumentException("expects no arguments");
            }
            return ctorRef.apply(source);
        };
        return def(function, builder, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(
        Class<T> function,
        BiFunction<Source, Expression, T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (children.size() != 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            return ctorRef.apply(source, children.get(0));
        };
        return def(function, builder, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for multi-arg/n-ary function.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    protected <T extends Function> FunctionDefinition def(Class<T> function, NaryBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> { return ctorRef.build(source, children); };
        return def(function, builder, names);
    }

    protected interface NaryBuilder<T> {
        T build(Source source, List<Expression> children);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    public static <T extends Function> FunctionDefinition def(Class<T> function, BinaryBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean isBinaryOptionalParamFunction = OptionalArgument.class.isAssignableFrom(function);
            if (isBinaryOptionalParamFunction && (children.size() > 2 || children.size() < 1)) {
                throw new QlIllegalArgumentException("expects one or two arguments");
            } else if (isBinaryOptionalParamFunction == false && children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }

            return ctorRef.build(source, children.get(0), children.size() == 2 ? children.get(1) : null);
        };
        return def(function, builder, names);
    }

    public interface BinaryBuilder<T> {
        T build(Source source, Expression left, Expression right);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a ternary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, TernaryBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean hasMinimumTwo = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumTwo && (children.size() > 3 || children.size() < 2)) {
                throw new QlIllegalArgumentException("expects two or three arguments");
            } else if (hasMinimumTwo == false && children.size() != 3) {
                throw new QlIllegalArgumentException("expects exactly three arguments");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.size() == 3 ? children.get(2) : null);
        };
        return def(function, builder, names);
    }

    protected interface TernaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a quaternary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, QuaternaryBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (OptionalArgument.class.isAssignableFrom(function)) {
                if (children.size() > 4 || children.size() < 3) {
                    throw new QlIllegalArgumentException("expects three or four arguments");
                }
            } else if (TwoOptionalArguments.class.isAssignableFrom(function)) {
                if (children.size() > 4 || children.size() < 2) {
                    throw new QlIllegalArgumentException("expects minimum two, maximum four arguments");
                }
            } else if (children.size() != 4) {
                throw new QlIllegalArgumentException("expects exactly four arguments");
            }
            return ctorRef.build(
                source,
                children.get(0),
                children.get(1),
                children.size() > 2 ? children.get(2) : null,
                children.size() > 3 ? children.get(3) : null
            );
        };
        return def(function, builder, names);
    }

    protected interface QuaternaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Expression four);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a quinary function.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        QuinaryBuilder<T> ctorRef,
        int numOptionalParams,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            final int NUM_TOTAL_PARAMS = 5;
            boolean hasOptionalParams = OptionalArgument.class.isAssignableFrom(function);
            if (hasOptionalParams && (children.size() > NUM_TOTAL_PARAMS || children.size() < NUM_TOTAL_PARAMS - numOptionalParams)) {
                throw new QlIllegalArgumentException(
                    "expects between "
                        + NUM_NAMES[NUM_TOTAL_PARAMS - numOptionalParams]
                        + " and "
                        + NUM_NAMES[NUM_TOTAL_PARAMS]
                        + " arguments"
                );
            } else if (hasOptionalParams == false && children.size() != NUM_TOTAL_PARAMS) {
                throw new QlIllegalArgumentException("expects exactly " + NUM_NAMES[NUM_TOTAL_PARAMS] + " arguments");
            }
            return ctorRef.build(
                source,
                children.size() > 0 ? children.get(0) : null,
                children.size() > 1 ? children.get(1) : null,
                children.size() > 2 ? children.get(2) : null,
                children.size() > 3 ? children.get(3) : null,
                children.size() > 4 ? children.get(4) : null
            );
        };
        return def(function, builder, names);
    }

    protected interface QuinaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Expression four, Expression five);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for functions with a mandatory argument followed by a varidic list.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, UnaryVariadicBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean hasMinimumOne = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumOne && children.size() < 1) {
                throw new QlIllegalArgumentException("expects at least one argument");
            } else if (hasMinimumOne == false && children.size() < 2) {
                throw new QlIllegalArgumentException("expects at least two arguments");
            }
            return ctorRef.build(source, children.get(0), children.subList(1, children.size()));
        };
        return def(function, builder, names);
    }

    protected interface UnaryVariadicBuilder<T> {
        T build(Source source, Expression exp, List<Expression> variadic);
    }

    protected interface BinaryVariadicWithOptionsBuilder<T> {
        T build(Source source, Expression exp, List<Expression> variadic, Expression options);
    };

    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        BinaryVariadicWithOptionsBuilder<T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean hasMinimumOne = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumOne && children.size() < 1) {
                throw new QlIllegalArgumentException("expects at least one argument");
            } else if (hasMinimumOne == false && children.size() < 2) {
                throw new QlIllegalArgumentException("expects at least two arguments");
            }
            Expression options = children.getLast();
            if (options instanceof MapExpression) {
                return ctorRef.build(source, children.get(0), children.subList(1, children.size() - 1), options);
            }

            return ctorRef.build(source, children.get(0), children.subList(1, children.size()), null);
        };
        return def(function, builder, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that is configuration aware.
     */
    @SuppressWarnings("overloads")
    protected static <T extends Function> FunctionDefinition def(Class<T> function, ConfigurationAwareBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (false == children.isEmpty()) {
                throw new QlIllegalArgumentException("expects no arguments");
            }
            return ctorRef.build(source, cfg);
        };
        return def(function, builder, names);
    }

    protected interface ConfigurationAwareBuilder<T> {
        T build(Source source, Configuration configuration);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a one-argument function that is configuration aware.
     */
    @SuppressWarnings("overloads")
    public static <T extends Function> FunctionDefinition def(
        Class<T> function,
        UnaryConfigurationAwareBuilder<T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            if (children.size() > 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            Expression ex = children.size() == 1 ? children.get(0) : null;
            return ctorRef.build(source, ex, cfg);
        };
        return def(function, builder, names);
    }

    public interface UnaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression exp, Configuration configuration);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that is configuration aware.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(
        Class<T> function,
        BinaryConfigurationAwareBuilder<T> ctorRef,
        String... names
    ) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean isBinaryOptionalParamFunction = OptionalArgument.class.isAssignableFrom(function);
            if (isBinaryOptionalParamFunction && (children.size() > 2 || children.size() < 1)) {
                throw new QlIllegalArgumentException("expects one or two arguments");
            } else if (isBinaryOptionalParamFunction == false && children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }
            return ctorRef.build(source, children.get(0), children.size() == 2 ? children.get(1) : null, cfg);
        };
        return def(function, builder, names);
    }

    protected interface BinaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression left, Expression right, Configuration configuration);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a ternary function that is configuration aware.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected <T extends Function> FunctionDefinition def(Class<T> function, TernaryConfigurationAwareBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, cfg) -> {
            boolean hasMinimumTwo = OptionalArgument.class.isAssignableFrom(function);
            if (hasMinimumTwo && (children.size() > 3 || children.size() < 2)) {
                throw new QlIllegalArgumentException("expects two or three arguments");
            } else if (hasMinimumTwo == false && children.size() != 3) {
                throw new QlIllegalArgumentException("expects exactly three arguments");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.size() == 3 ? children.get(2) : null, cfg);
        };
        return def(function, builder, names);
    }

    protected interface TernaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Configuration configuration);
    }

    //
    // Utility functions to help disambiguate the method handle passed in.
    // They work by providing additional method information to help the compiler know which method to pick.
    //
    private static <T extends Function> BiFunction<Source, Expression, T> uni(BiFunction<Source, Expression, T> function) {
        return function;
    }

    private static <T extends Function> BinaryBuilder<T> bi(BinaryBuilder<T> function) {
        return function;
    }

    private static <T extends Function> TernaryBuilder<T> tri(TernaryBuilder<T> function) {
        return function;
    }

}
