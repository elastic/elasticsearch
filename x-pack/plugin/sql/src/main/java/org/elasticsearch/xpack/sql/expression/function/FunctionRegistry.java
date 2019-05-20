/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.aggregate.First;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Kurtosis;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRank;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Skewness;
import org.elasticsearch.xpack.sql.expression.function.aggregate.StddevPop;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.aggregate.SumOfSquares;
import org.elasticsearch.xpack.sql.expression.function.aggregate.TopHits;
import org.elasticsearch.xpack.sql.expression.function.aggregate.VarPop;
import org.elasticsearch.xpack.sql.expression.function.grouping.Histogram;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.function.scalar.Database;
import org.elasticsearch.xpack.sql.expression.function.scalar.User;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.CurrentDate;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.CurrentDateTime;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.CurrentTime;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayName;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfMonth;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfWeek;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.HourOfDay;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.IsoDayOfWeek;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.IsoWeekOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MinuteOfDay;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MinuteOfHour;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthName;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Quarter;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.SecondOfMinute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.WeekOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Year;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StAswkt;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StDistance;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StGeometryType;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StWkttosql;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StX;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StY;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StZ;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ACos;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ASin;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ATan;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ATan2;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Cbrt;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Ceil;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Cosh;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Cot;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Degrees;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.E;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Exp;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Expm1;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Log10;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Pi;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Power;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Radians;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Random;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Sign;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Sin;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Sinh;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Sqrt;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Tan;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Truncate;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Ascii;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BitLength;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Char;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.CharLength;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Insert;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.LCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Left;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Locate;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.OctetLength;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Position;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.RTrim;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Repeat;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Right;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Space;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.UCase;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Case;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Greatest;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Iif;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfNull;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Least;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.NullIf;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.Check;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class FunctionRegistry {

    // list of functions grouped by type of functions (aggregate, statistics, math etc) and ordered alphabetically inside each group
    // a single function will have one entry for itself with its name associated to its instance and, also, one entry for each alias
    // it has with the alias name associated to the FunctionDefinition instance
    private final Map<String, FunctionDefinition> defs = new LinkedHashMap<>();
    private final Map<String, String> aliases = new HashMap<>();

    /**
     * Constructor to build with the default list of functions.
     */
    public FunctionRegistry() {
        defineDefaultFunctions();
    }

    /**
     * Constructor specifying alternate functions for testing.
     */
    FunctionRegistry(FunctionDefinition... functions) {
        addToMap(functions);
    }

    private void defineDefaultFunctions() {
        // Aggregate functions
        addToMap(def(Avg.class, Avg::new, "AVG"),
                def(Count.class, Count::new, "COUNT"),
                def(First.class, First::new, "FIRST", "FIRST_VALUE"),
                def(Last.class, Last::new, "LAST", "LAST_VALUE"),
                def(Max.class, Max::new, "MAX"),
                def(Min.class, Min::new, "MIN"),
                def(Sum.class, Sum::new, "SUM"));
        // Statistics
        addToMap(
                def(Kurtosis.class, Kurtosis::new, "KURTOSIS"),
                def(MedianAbsoluteDeviation.class, MedianAbsoluteDeviation::new, "MAD"),
                def(Percentile.class, Percentile::new, "PERCENTILE"),
                def(PercentileRank.class, PercentileRank::new, "PERCENTILE_RANK"),
                def(Skewness.class, Skewness::new, "SKEWNESS"),
                def(StddevPop.class, StddevPop::new, "STDDEV_POP"),
                def(SumOfSquares.class, SumOfSquares::new, "SUM_OF_SQUARES"),
                def(VarPop.class, VarPop::new,"VAR_POP")
                );
        // histogram
        addToMap(def(Histogram.class, Histogram::new, "HISTOGRAM"));
        // Scalar functions
        // Conditional
        addToMap(def(Case.class, Case::new, "CASE"),
                def(Coalesce.class, Coalesce::new, "COALESCE"),
                def(Iif.class, Iif::new, "IIF"),
                def(IfNull.class, IfNull::new, "IFNULL", "ISNULL", "NVL"),
                def(NullIf.class, NullIf::new, "NULLIF"),
                def(Greatest.class, Greatest::new, "GREATEST"),
                def(Least.class, Least::new, "LEAST"));
        // Date
        addToMap(def(CurrentDate.class, CurrentDate::new, "CURRENT_DATE", "CURDATE", "TODAY"),
                def(CurrentTime.class, CurrentTime::new, "CURRENT_TIME", "CURTIME"),
                def(CurrentDateTime.class, CurrentDateTime::new, "CURRENT_TIMESTAMP", "NOW"),
                def(DayName.class, DayName::new, "DAY_NAME", "DAYNAME"),
                def(DayOfMonth.class, DayOfMonth::new, "DAY_OF_MONTH", "DAYOFMONTH", "DAY", "DOM"),
                def(DayOfWeek.class, DayOfWeek::new, "DAY_OF_WEEK", "DAYOFWEEK", "DOW"),
                def(DayOfYear.class, DayOfYear::new, "DAY_OF_YEAR", "DAYOFYEAR", "DOY"),
                def(HourOfDay.class, HourOfDay::new, "HOUR_OF_DAY", "HOUR"),
                def(IsoDayOfWeek.class, IsoDayOfWeek::new, "ISO_DAY_OF_WEEK", "ISODAYOFWEEK", "ISODOW", "IDOW"),
                def(IsoWeekOfYear.class, IsoWeekOfYear::new, "ISO_WEEK_OF_YEAR", "ISOWEEKOFYEAR", "ISOWEEK", "IWOY", "IW"),
                def(MinuteOfDay.class, MinuteOfDay::new, "MINUTE_OF_DAY"),
                def(MinuteOfHour.class, MinuteOfHour::new, "MINUTE_OF_HOUR", "MINUTE"),
                def(MonthName.class, MonthName::new, "MONTH_NAME", "MONTHNAME"),
                def(MonthOfYear.class, MonthOfYear::new, "MONTH_OF_YEAR", "MONTH"),
                def(SecondOfMinute.class, SecondOfMinute::new, "SECOND_OF_MINUTE", "SECOND"),
                def(Quarter.class, Quarter::new, "QUARTER"),
                def(Year.class, Year::new, "YEAR"),
                def(WeekOfYear.class, WeekOfYear::new, "WEEK_OF_YEAR", "WEEK"));
        // Math
        addToMap(def(Abs.class, Abs::new, "ABS"),
                def(ACos.class, ACos::new, "ACOS"),
                def(ASin.class, ASin::new, "ASIN"),
                def(ATan.class, ATan::new, "ATAN"),
                def(ATan2.class, ATan2::new, "ATAN2"),
                def(Cbrt.class, Cbrt::new, "CBRT"),
                def(Ceil.class, Ceil::new, "CEIL", "CEILING"),
                def(Cos.class, Cos::new, "COS"),
                def(Cosh.class, Cosh::new, "COSH"),
                def(Cot.class, Cot::new, "COT"),
                def(Degrees.class, Degrees::new, "DEGREES"),
                def(E.class, E::new, "E"),
                def(Exp.class, Exp::new, "EXP"),
                def(Expm1.class, Expm1::new, "EXPM1"),
                def(Floor.class, Floor::new, "FLOOR"),
                def(Log.class, Log::new, "LOG"),
                def(Log10.class, Log10::new, "LOG10"),
                // SQL and ODBC require MOD as a _function_
                def(Mod.class, Mod::new, "MOD"),
                def(Pi.class, Pi::new, "PI"),
                def(Power.class, Power::new, "POWER"),
                def(Radians.class, Radians::new, "RADIANS"),
                def(Random.class, Random::new, "RANDOM", "RAND"),
                def(Round.class, Round::new, "ROUND"),
                def(Sign.class, Sign::new, "SIGN", "SIGNUM"),
                def(Sin.class, Sin::new, "SIN"),
                def(Sinh.class, Sinh::new, "SINH"),
                def(Sqrt.class, Sqrt::new, "SQRT"),
                def(Tan.class, Tan::new, "TAN"),
                def(Truncate.class, Truncate::new, "TRUNCATE"));
        // String
        addToMap(def(Ascii.class, Ascii::new, "ASCII"),
                def(BitLength.class, BitLength::new, "BIT_LENGTH"),
                def(Char.class, Char::new, "CHAR"),
                def(CharLength.class, CharLength::new, "CHAR_LENGTH", "CHARACTER_LENGTH"),
                def(Concat.class, Concat::new, "CONCAT"),
                def(Insert.class, Insert::new, "INSERT"),
                def(LCase.class, LCase::new, "LCASE"),
                def(Left.class, Left::new, "LEFT"),
                def(Length.class, Length::new, "LENGTH"),
                def(Locate.class, Locate::new, "LOCATE"),
                def(LTrim.class, LTrim::new, "LTRIM"),
                def(OctetLength.class, OctetLength::new, "OCTET_LENGTH"),
                def(Position.class, Position::new, "POSITION"),
                def(Repeat.class, Repeat::new, "REPEAT"),
                def(Replace.class, Replace::new, "REPLACE"),
                def(Right.class, Right::new, "RIGHT"),
                def(RTrim.class, RTrim::new, "RTRIM"),
                def(Space.class, Space::new, "SPACE"),
                def(Substring.class, Substring::new, "SUBSTRING"),
                def(UCase.class, UCase::new, "UCASE"));

        // DataType conversion
        addToMap(def(Cast.class, Cast::new, "CAST", "CONVERT"));
        // Scalar "meta" functions
        addToMap(def(Database.class, Database::new, "DATABASE"),
                def(User.class, User::new, "USER"));

        // Geo Functions
        addToMap(def(StAswkt.class, StAswkt::new, "ST_ASWKT", "ST_ASTEXT"),
                def(StDistance.class, StDistance::new, "ST_DISTANCE"),
                def(StWkttosql.class, StWkttosql::new, "ST_WKTTOSQL", "ST_GEOMFROMTEXT"),
                def(StGeometryType.class, StGeometryType::new, "ST_GEOMETRYTYPE"),
                def(StX.class, StX::new, "ST_X"),
                def(StY.class, StY::new, "ST_Y"),
                def(StZ.class, StZ::new, "ST_Z")
        );

        // Special
        addToMap(def(Score.class, Score::new, "SCORE"));
    }

    void addToMap(FunctionDefinition...functions) {
        // temporary map to hold [function_name/alias_name : function instance]
        Map<String, FunctionDefinition> batchMap = new HashMap<>();
        for (FunctionDefinition f : functions) {
            batchMap.put(f.name(), f);
            for (String alias : f.aliases()) {
                Object old = batchMap.put(alias, f);
                if (old != null || defs.containsKey(alias)) {
                    throw new SqlIllegalArgumentException("alias [" + alias + "] is used by "
                            + "[" + (old != null ? old : defs.get(alias).name()) + "] and [" + f.name() + "]");
                }
                aliases.put(alias, f.name());
            }
        }
        // sort the temporary map by key name and add it to the global map of functions
        defs.putAll(batchMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.<Entry<String, FunctionDefinition>, String,
                        FunctionDefinition, LinkedHashMap<String, FunctionDefinition>> toMap(Map.Entry::getKey, Map.Entry::getValue,
                (oldValue, newValue) -> oldValue, LinkedHashMap::new)));
    }

    public FunctionDefinition resolveFunction(String functionName) {
        FunctionDefinition def = defs.get(functionName);
        if (def == null) {
            throw new SqlIllegalArgumentException(
                "Cannot find function {}; this should have been caught during analysis",
                functionName);
        }
        return def;
    }

    public String resolveAlias(String alias) {
        String upperCase = alias.toUpperCase(Locale.ROOT);
        return aliases.getOrDefault(upperCase, upperCase);
    }

    public boolean functionExists(String functionName) {
        return defs.containsKey(functionName);
    }

    public Collection<FunctionDefinition> listFunctions() {
        // It is worth double checking if we need this copy. These are immutable anyway.
        return defs.entrySet().stream()
                .map(e -> new FunctionDefinition(e.getKey(), emptyList(),
                        e.getValue().clazz(), e.getValue().extractViable(), e.getValue().builder()))
                .collect(toList());
    }

    public Collection<FunctionDefinition> listFunctions(String pattern) {
        // It is worth double checking if we need this copy. These are immutable anyway.
        Pattern p = Strings.hasText(pattern) ? Pattern.compile(pattern.toUpperCase(Locale.ROOT)) : null;
        return defs.entrySet().stream()
                .filter(e -> p == null || p.matcher(e.getKey()).matches())
                .map(e -> new FunctionDefinition(e.getKey(), emptyList(),
                        e.getValue().clazz(), e.getValue().extractViable(), e.getValue().builder()))
                .collect(toList());
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that
     * is not aware of time zone and does not support {@code DISTINCT}.
     */
    static <T extends Function> FunctionDefinition def(Class<T> function,
            java.util.function.Function<Source, T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (false == children.isEmpty()) {
                throw new SqlIllegalArgumentException("expects no arguments");
            }
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.apply(source);
        };
        return def(function, builder, false, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that
     * is not aware of time zone, does not support {@code DISTINCT} and needs
     * the cluster name (DATABASE()) or the user name (USER()).
     */
    @SuppressWarnings("overloads")
    static <T extends Function> FunctionDefinition def(Class<T> function,
            ConfigurationAwareFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (false == children.isEmpty()) {
                throw new SqlIllegalArgumentException("expects no arguments");
            }
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, cfg);
        };
        return def(function, builder, false, names);
    }
    
    interface ConfigurationAwareFunctionBuilder<T> {
        T build(Source source, Configuration configuration);
    }

    /**
    * Build a {@linkplain FunctionDefinition} for a one-argument function that
    * is not aware of time zone, does not support {@code DISTINCT} and needs
    * the configuration object.
    */
    @SuppressWarnings("overloads")
    static <T extends Function> FunctionDefinition def(Class<T> function,
            UnaryConfigurationAwareFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() > 1) {
                throw new SqlIllegalArgumentException("expects exactly one argument");
            }
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            Expression ex = children.size() == 1 ? children.get(0) : null;
            return ctorRef.build(source, ex, cfg);
        };
        return def(function, builder, false, names);
    }

    interface UnaryConfigurationAwareFunctionBuilder<T> {
        T build(Source source, Expression exp, Configuration configuration);
    }


    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not
     * aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            BiFunction<Source, Expression, T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new SqlIllegalArgumentException("expects exactly one argument");
            }
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.apply(source, children.get(0));
        };
        return def(function, builder, false, names);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for multi-arg function that
     * is not aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            MultiFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children);
        };
        return def(function, builder, false, names);
    }

    interface MultiFunctionBuilder<T> {
        T build(Source source, List<Expression> children);
    }
    
    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not
     * aware of time zone but does support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            DistinctAwareUnaryFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new SqlIllegalArgumentException("expects exactly one argument");
            }
            return ctorRef.build(source, children.get(0), distinct);
        };
        return def(function, builder, false, names);
    }

    interface DistinctAwareUnaryFunctionBuilder<T> {
        T build(Source source, Expression target, boolean distinct);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that
     * operates on a datetime.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            DatetimeUnaryFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new SqlIllegalArgumentException("expects exactly one argument");
            }
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), cfg.zoneId());
        };
        return def(function, builder, true, names);
    }

    interface DatetimeUnaryFunctionBuilder<T> {
        T build(Source source, Expression target, ZoneId zi);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that
     * requires a timezone.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function, DatetimeBinaryFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 2) {
                throw new SqlIllegalArgumentException("expects exactly two arguments");
            }
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), cfg.zoneId());
        };
        return def(function, builder, false, names);
    }

    interface DatetimeBinaryFunctionBuilder<T> {
        T build(Source source, Expression lhs, Expression rhs, ZoneId zi);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that is
     * not aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            BinaryFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            boolean isBinaryOptionalParamFunction = function.isAssignableFrom(Round.class) || function.isAssignableFrom(Truncate.class)
                    || TopHits.class.isAssignableFrom(function);
            if (isBinaryOptionalParamFunction && (children.size() > 2 || children.size() < 1)) {
                throw new SqlIllegalArgumentException("expects one or two arguments");
            } else if (!isBinaryOptionalParamFunction && children.size() != 2) {
                throw new SqlIllegalArgumentException("expects exactly two arguments");
            }

            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.size() == 2 ? children.get(1) : null);
        };
        return def(function, builder, false, names);
    }

    interface BinaryFunctionBuilder<T> {
        T build(Source source, Expression lhs, Expression rhs);
    }

    /**
     * Main method to register a function/
     * @param names Must always have at least one entry which is the method's primary name
     *
     */
    @SuppressWarnings("overloads")
    private static FunctionDefinition def(Class<? extends Function> function, FunctionBuilder builder,
                                          boolean datetime, String... names) {
        Check.isTrue(names.length > 0, "At least one name must be provided for the function");
        String primaryName = names[0];
        List<String> aliases = Arrays.asList(names).subList(1, names.length);
        FunctionDefinition.Builder realBuilder = (uf, distinct, cfg) -> {
            try {
                return builder.build(uf.source(), uf.children(), distinct, cfg);
            } catch (SqlIllegalArgumentException e) {
                throw new ParsingException(uf.source(), "error building [" + primaryName + "]: " + e.getMessage(), e);
            }
        };
        return new FunctionDefinition(primaryName, unmodifiableList(aliases), function, datetime, realBuilder);
    }

    private interface FunctionBuilder {
        Function build(Source source, List<Expression> children, boolean distinct, Configuration cfg);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            ThreeParametersFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            boolean hasMinimumTwo = function.isAssignableFrom(Locate.class) || function.isAssignableFrom(Iif.class);
            if (hasMinimumTwo && (children.size() > 3 || children.size() < 2)) {
                throw new SqlIllegalArgumentException("expects two or three arguments");
            } else if (!hasMinimumTwo && children.size() != 3) {
                throw new SqlIllegalArgumentException("expects exactly three arguments");
            }
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.size() == 3 ? children.get(2) : null);
        };
        return def(function, builder, false, names);
    }

    interface ThreeParametersFunctionBuilder<T> {
        T build(Source source, Expression src, Expression exp1, Expression exp2);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            FourParametersFunctionBuilder<T> ctorRef, String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) -> {
            if (children.size() != 4) {
                throw new SqlIllegalArgumentException("expects exactly four arguments");
            }
            if (distinct) {
                throw new SqlIllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(source, children.get(0), children.get(1), children.get(2), children.get(3));
        };
        return def(function, builder, false, names);
    }

    interface FourParametersFunctionBuilder<T> {
        T build(Source source, Expression src, Expression exp1, Expression exp2, Expression exp3);
    }

    /**
     * Special method to create function definition for {@link Cast} as its
     * signature is not compatible with {@link UnresolvedFunction}
     *
     * @return Cast function definition
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    private static <T extends Function> FunctionDefinition def(Class<T> function,
                                                               CastFunctionBuilder<T> ctorRef,
                                                               String... names) {
        FunctionBuilder builder = (source, children, distinct, cfg) ->
            ctorRef.build(source, children.get(0), children.get(0).dataType());
        return def(function, builder, false, names);
    }

    private interface CastFunctionBuilder<T> {
        T build(Source source, Expression expression, DataType dataType);
    }
}
