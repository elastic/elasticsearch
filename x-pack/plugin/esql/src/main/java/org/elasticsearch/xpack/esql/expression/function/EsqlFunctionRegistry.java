/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

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
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Acos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan2;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialWithin;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StX;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StY;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Left;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Locate;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Right;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Split;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.esql.plan.logical.meta.MetaFunctions;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;

public final class EsqlFunctionRegistry extends FunctionRegistry {

    private static final Map<Class<? extends Function>, List<DataType>> dataTypesForStringLiteralConversion = new LinkedHashMap<>();

    private static final Map<DataType, Integer> dataTypeCastingPriority;

    static {
        List<DataType> typePriorityList = Arrays.asList(
            DATETIME,
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
        dataTypeCastingPriority = new HashMap<>();
        for (int i = 0; i < typePriorityList.size(); i++) {
            dataTypeCastingPriority.put(typePriorityList.get(i), i);
        }
    }

    public EsqlFunctionRegistry() {
        register(functions());
        buildDataTypesForStringLiteralConversion(functions());
    }

    EsqlFunctionRegistry(FunctionDefinition... functions) {
        register(functions);
    }

    private FunctionDefinition[][] functions() {
        return new FunctionDefinition[][] {
            // grouping functions
            new FunctionDefinition[] { def(Bucket.class, Bucket::new, "bucket", "bin"), },
            // aggregate functions
            new FunctionDefinition[] {
                def(Avg.class, Avg::new, "avg"),
                def(Count.class, Count::new, "count"),
                def(CountDistinct.class, CountDistinct::new, "count_distinct"),
                def(Max.class, Max::new, "max"),
                def(Median.class, Median::new, "median"),
                def(MedianAbsoluteDeviation.class, MedianAbsoluteDeviation::new, "median_absolute_deviation"),
                def(Min.class, Min::new, "min"),
                def(Percentile.class, Percentile::new, "percentile"),
                def(Sum.class, Sum::new, "sum"),
                def(Values.class, Values::new, "values") },
            // math
            new FunctionDefinition[] {
                def(Abs.class, Abs::new, "abs"),
                def(Acos.class, Acos::new, "acos"),
                def(Asin.class, Asin::new, "asin"),
                def(Atan.class, Atan::new, "atan"),
                def(Atan2.class, Atan2::new, "atan2"),
                def(Ceil.class, Ceil::new, "ceil"),
                def(Cos.class, Cos::new, "cos"),
                def(Cosh.class, Cosh::new, "cosh"),
                def(E.class, E::new, "e"),
                def(Floor.class, Floor::new, "floor"),
                def(Greatest.class, Greatest::new, "greatest"),
                def(Log.class, Log::new, "log"),
                def(Log10.class, Log10::new, "log10"),
                def(Least.class, Least::new, "least"),
                def(Pi.class, Pi::new, "pi"),
                def(Pow.class, Pow::new, "pow"),
                def(Round.class, Round::new, "round"),
                def(Signum.class, Signum::new, "signum"),
                def(Sin.class, Sin::new, "sin"),
                def(Sinh.class, Sinh::new, "sinh"),
                def(Sqrt.class, Sqrt::new, "sqrt"),
                def(Tan.class, Tan::new, "tan"),
                def(Tanh.class, Tanh::new, "tanh"),
                def(Tau.class, Tau::new, "tau") },
            // string
            new FunctionDefinition[] {
                def(Length.class, Length::new, "length"),
                def(Substring.class, Substring::new, "substring"),
                def(Concat.class, Concat::new, "concat"),
                def(LTrim.class, LTrim::new, "ltrim"),
                def(RTrim.class, RTrim::new, "rtrim"),
                def(Trim.class, Trim::new, "trim"),
                def(Left.class, Left::new, "left"),
                def(Replace.class, Replace::new, "replace"),
                def(Right.class, Right::new, "right"),
                def(StartsWith.class, StartsWith::new, "starts_with"),
                def(EndsWith.class, EndsWith::new, "ends_with"),
                def(ToLower.class, ToLower::new, "to_lower"),
                def(ToUpper.class, ToUpper::new, "to_upper"),
                def(Locate.class, Locate::new, "locate") },
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
                def(SpatialDisjoint.class, SpatialDisjoint::new, "st_disjoint"),
                def(SpatialIntersects.class, SpatialIntersects::new, "st_intersects"),
                def(SpatialWithin.class, SpatialWithin::new, "st_within"),
                def(StX.class, StX::new, "st_x"),
                def(StY.class, StY::new, "st_y") },
            // conditional
            new FunctionDefinition[] { def(Case.class, Case::new, "case") },
            // null
            new FunctionDefinition[] { def(Coalesce.class, Coalesce::new, "coalesce"), },
            // IP
            new FunctionDefinition[] { def(CIDRMatch.class, CIDRMatch::new, "cidr_match") },
            // conversion functions
            new FunctionDefinition[] {
                def(FromBase64.class, FromBase64::new, "from_base64"),
                def(ToBase64.class, ToBase64::new, "to_base64"),
                def(ToBoolean.class, ToBoolean::new, "to_boolean", "to_bool"),
                def(ToCartesianPoint.class, ToCartesianPoint::new, "to_cartesianpoint"),
                def(ToCartesianShape.class, ToCartesianShape::new, "to_cartesianshape"),
                def(ToDatetime.class, ToDatetime::new, "to_datetime", "to_dt"),
                def(ToDegrees.class, ToDegrees::new, "to_degrees"),
                def(ToDouble.class, ToDouble::new, "to_double", "to_dbl"),
                def(ToGeoPoint.class, ToGeoPoint::new, "to_geopoint"),
                def(ToGeoShape.class, ToGeoShape::new, "to_geoshape"),
                def(ToIP.class, ToIP::new, "to_ip"),
                def(ToInteger.class, ToInteger::new, "to_integer", "to_int"),
                def(ToLong.class, ToLong::new, "to_long"),
                def(ToRadians.class, ToRadians::new, "to_radians"),
                def(ToString.class, ToString::new, "to_string", "to_str"),
                def(ToUnsignedLong.class, ToUnsignedLong::new, "to_unsigned_long", "to_ulong", "to_ul"),
                def(ToVersion.class, ToVersion::new, "to_version", "to_ver"), },
            // multivalue functions
            new FunctionDefinition[] {
                def(MvAvg.class, MvAvg::new, "mv_avg"),
                def(MvConcat.class, MvConcat::new, "mv_concat"),
                def(MvCount.class, MvCount::new, "mv_count"),
                def(MvDedupe.class, MvDedupe::new, "mv_dedupe"),
                def(MvFirst.class, MvFirst::new, "mv_first"),
                def(MvLast.class, MvLast::new, "mv_last"),
                def(MvMax.class, MvMax::new, "mv_max"),
                def(MvMedian.class, MvMedian::new, "mv_median"),
                def(MvMin.class, MvMin::new, "mv_min"),
                def(MvSort.class, MvSort::new, "mv_sort"),
                def(MvSlice.class, MvSlice::new, "mv_slice"),
                def(MvZip.class, MvZip::new, "mv_zip"),
                def(MvSum.class, MvSum::new, "mv_sum"),
                def(Split.class, Split::new, "split") } };
    }

    @Override
    protected String normalize(String name) {
        return normalizeName(name);
    }

    public static String normalizeName(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    public record ArgSignature(String name, String[] type, String description, boolean optional, DataType targetDataType) {
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
                + '}';
        }
    }

    public record FunctionDescription(
        String name,
        List<ArgSignature> args,
        String[] returnType,
        String description,
        boolean variadic,
        boolean isAggregation
    ) {
        public String fullSignature() {
            StringBuilder builder = new StringBuilder();
            builder.append(MetaFunctions.withPipes(returnType));
            builder.append(" ");
            builder.append(name);
            builder.append("(");
            for (int i = 0; i < args.size(); i++) {
                ArgSignature arg = args.get(i);
                if (i > 0) {
                    builder.append(", ");
                }
                if (arg.optional()) {
                    builder.append("?");
                }
                builder.append(arg.name());
                if (i == args.size() - 1 && variadic) {
                    builder.append("...");
                }
                builder.append(":");
                builder.append(MetaFunctions.withPipes(arg.type()));
            }
            builder.append(")");
            return builder.toString();
        }

        /**
         * The name of every argument.
         */
        public List<String> argNames() {
            return args.stream().map(ArgSignature::name).toList();
        }

        /**
         * The description of every argument.
         */
        public List<String> argDescriptions() {
            return args.stream().map(ArgSignature::description).toList();
        }
    }

    public static DataType getTargetType(String[] names) {
        List<DataType> types = new ArrayList<>();
        for (String name : names) {
            types.add(DataTypes.fromEs(name));
        }
        if (types.contains(KEYWORD) || types.contains(TEXT)) {
            return UNSUPPORTED;
        }

        return types.stream()
            .min((dt1, dt2) -> dataTypeCastingPriority.get(dt1).compareTo(dataTypeCastingPriority.get(dt2)))
            .orElse(UNSUPPORTED);
    }

    public static FunctionDescription description(FunctionDefinition def) {
        var constructors = def.clazz().getConstructors();
        if (constructors.length == 0) {
            return new FunctionDescription(def.name(), List.of(), null, null, false, false);
        }
        Constructor<?> constructor = constructors[0];
        FunctionInfo functionInfo = functionInfo(def);
        String functionDescription = functionInfo == null ? "" : functionInfo.description().replace('\n', ' ');
        String[] returnType = functionInfo == null ? new String[] { "?" } : functionInfo.returnType();
        var params = constructor.getParameters(); // no multiple c'tors supported

        List<EsqlFunctionRegistry.ArgSignature> args = new ArrayList<>(params.length);
        boolean variadic = false;
        boolean isAggregation = functionInfo == null ? false : functionInfo.isAggregation();
        for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
            if (Configuration.class.isAssignableFrom(params[i].getType()) == false) {
                Param paramInfo = params[i].getAnnotation(Param.class);
                String name = paramInfo == null ? params[i].getName() : paramInfo.name();
                variadic |= List.class.isAssignableFrom(params[i].getType());
                String[] type = paramInfo == null ? new String[] { "?" } : paramInfo.type();
                String desc = paramInfo == null ? "" : paramInfo.description().replace('\n', ' ');
                boolean optional = paramInfo == null ? false : paramInfo.optional();
                DataType targetDataType = getTargetType(type);
                args.add(new EsqlFunctionRegistry.ArgSignature(name, type, desc, optional, targetDataType));
            }
        }
        return new FunctionDescription(def.name(), args, returnType, functionDescription, variadic, isAggregation);
    }

    public static FunctionInfo functionInfo(FunctionDefinition def) {
        var constructors = def.clazz().getConstructors();
        if (constructors.length == 0) {
            return null;
        }
        Constructor<?> constructor = constructors[0];
        return constructor.getAnnotation(FunctionInfo.class);
    }

    private void buildDataTypesForStringLiteralConversion(FunctionDefinition[]... groupFunctions) {
        for (FunctionDefinition[] group : groupFunctions) {
            for (FunctionDefinition def : group) {
                FunctionDescription signature = description(def);
                dataTypesForStringLiteralConversion.put(
                    def.clazz(),
                    signature.args().stream().map(EsqlFunctionRegistry.ArgSignature::targetDataType).collect(Collectors.toList())
                );
            }
        }
    }

    public List<DataType> getDataTypeForStringLiteralConversion(Class<? extends Function> clazz) {
        return dataTypesForStringLiteralConversion.get(clazz);
    }
}
