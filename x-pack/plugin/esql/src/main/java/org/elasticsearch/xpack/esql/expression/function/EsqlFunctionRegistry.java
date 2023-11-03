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
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
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
import org.elasticsearch.xpack.esql.plan.logical.show.ShowFunctions;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.session.Configuration;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public final class EsqlFunctionRegistry extends FunctionRegistry {

    public EsqlFunctionRegistry() {
        register(functions());
    }

    EsqlFunctionRegistry(FunctionDefinition... functions) {
        register(functions);
    }

    private FunctionDefinition[][] functions() {
        return new FunctionDefinition[][] {
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
                def(Sum.class, Sum::new, "sum") },
            // math
            new FunctionDefinition[] {
                def(Abs.class, Abs::new, "abs"),
                def(Acos.class, Acos::new, "acos"),
                def(Asin.class, Asin::new, "asin"),
                def(Atan.class, Atan::new, "atan"),
                def(Atan2.class, Atan2::new, "atan2"),
                def(AutoBucket.class, AutoBucket::new, "auto_bucket"),
                def(Ceil.class, Ceil::new, "ceil"),
                def(Cos.class, Cos::new, "cos"),
                def(Cosh.class, Cosh::new, "cosh"),
                def(E.class, E::new, "e"),
                def(Floor.class, Floor::new, "floor"),
                def(Greatest.class, Greatest::new, "greatest"),
                def(IsFinite.class, IsFinite::new, "is_finite"),
                def(IsInfinite.class, IsInfinite::new, "is_infinite"),
                def(IsNaN.class, IsNaN::new, "is_nan"),
                def(Log10.class, Log10::new, "log10"),
                def(Least.class, Least::new, "least"),
                def(Pi.class, Pi::new, "pi"),
                def(Pow.class, Pow::new, "pow"),
                def(Round.class, Round::new, "round"),
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
                def(EndsWith.class, EndsWith::new, "ends_with") },
            // date
            new FunctionDefinition[] {
                def(DateExtract.class, DateExtract::new, "date_extract"),
                def(DateFormat.class, DateFormat::new, "date_format"),
                def(DateParse.class, DateParse::new, "date_parse"),
                def(DateTrunc.class, DateTrunc::new, "date_trunc"),
                def(Now.class, Now::new, "now") },
            // conditional
            new FunctionDefinition[] { def(Case.class, Case::new, "case") },
            // null
            new FunctionDefinition[] { def(Coalesce.class, Coalesce::new, "coalesce"), },
            // IP
            new FunctionDefinition[] { def(CIDRMatch.class, CIDRMatch::new, "cidr_match") },
            // conversion functions
            new FunctionDefinition[] {
                def(ToBoolean.class, ToBoolean::new, "to_boolean", "to_bool"),
                def(ToDatetime.class, ToDatetime::new, "to_datetime", "to_dt"),
                def(ToDegrees.class, ToDegrees::new, "to_degrees"),
                def(ToDouble.class, ToDouble::new, "to_double", "to_dbl"),
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
                def(MvMax.class, MvMax::new, "mv_max"),
                def(MvMedian.class, MvMedian::new, "mv_median"),
                def(MvMin.class, MvMin::new, "mv_min"),
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

    public record ArgSignature(String name, String[] type, String description, boolean optional) {}

    public record FunctionDescription(String name, List<ArgSignature> args, String[] returnType, String description, boolean variadic) {
        public String fullSignature() {
            StringBuilder builder = new StringBuilder();
            builder.append(ShowFunctions.withPipes(returnType));
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
                builder.append(ShowFunctions.withPipes(arg.type()));
            }
            builder.append(")");
            return builder.toString();
        }

        public List<String> argNames() {
            return args.stream().map(ArgSignature::name).collect(Collectors.toList());
        }

    }

    public static FunctionDescription description(FunctionDefinition def) {
        var constructors = def.clazz().getConstructors();
        if (constructors.length == 0) {
            return new FunctionDescription(def.name(), List.of(), null, null, false);
        }
        Constructor<?> constructor = constructors[0];
        FunctionInfo functionInfo = constructor.getAnnotation(FunctionInfo.class);
        String functionDescription = functionInfo == null ? "" : functionInfo.description();
        String[] returnType = functionInfo == null ? new String[] { "?" } : functionInfo.returnType();
        var params = constructor.getParameters(); // no multiple c'tors supported

        List<EsqlFunctionRegistry.ArgSignature> args = new ArrayList<>(params.length);
        boolean variadic = false;
        for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
            if (Configuration.class.isAssignableFrom(params[i].getType()) == false) {
                Param paramInfo = params[i].getAnnotation(Param.class);
                String name = paramInfo == null ? params[i].getName() : paramInfo.name();
                variadic |= List.class.isAssignableFrom(params[i].getType());
                String[] type = paramInfo == null ? new String[] { "?" } : paramInfo.type();
                String desc = paramInfo == null ? "" : paramInfo.description();
                boolean optional = paramInfo == null ? false : paramInfo.optional();

                args.add(new EsqlFunctionRegistry.ArgSignature(name, type, desc, optional));
            }
        }
        return new FunctionDescription(def.name(), args, returnType, functionDescription, variadic);
    }

}
