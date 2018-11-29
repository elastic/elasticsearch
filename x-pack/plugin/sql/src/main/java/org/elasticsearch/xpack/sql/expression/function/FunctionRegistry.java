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
import org.elasticsearch.xpack.sql.expression.function.aggregate.Kurtosis;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRank;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Skewness;
import org.elasticsearch.xpack.sql.expression.function.aggregate.StddevPop;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.aggregate.SumOfSquares;
import org.elasticsearch.xpack.sql.expression.function.aggregate.VarPop;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.function.scalar.Database;
import org.elasticsearch.xpack.sql.expression.function.scalar.User;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayName;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfMonth;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfWeek;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.HourOfDay;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MinuteOfDay;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MinuteOfHour;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthName;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Quarter;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.SecondOfMinute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.WeekOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Year;
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
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Greatest;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfNull;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Least;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.NullIf;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class FunctionRegistry {

    private static final Set<String> EXCLUDE_FROM_NAME_NORMALIZATION = new HashSet<>();

    static {
        EXCLUDE_FROM_NAME_NORMALIZATION.add(IfNull.class.getSimpleName());
        EXCLUDE_FROM_NAME_NORMALIZATION.add(NullIf.class.getSimpleName());
    }


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
        addToMap(def(Avg.class, Avg::new),
                def(Count.class, Count::new),
                def(Max.class, Max::new),
                def(Min.class, Min::new),
                def(Sum.class, Sum::new));
        // Statistics
        addToMap(def(StddevPop.class, StddevPop::new),
                def(VarPop.class, VarPop::new),
                def(Percentile.class, Percentile::new),
                def(PercentileRank.class, PercentileRank::new),
                def(SumOfSquares.class, SumOfSquares::new),
                def(Skewness.class, Skewness::new),
                def(Kurtosis.class, Kurtosis::new));
        // Scalar functions
        // conditional
        addToMap(def(Coalesce.class, Coalesce::new),
                 def(IfNull.class, IfNull::new, "ISNULL", "NVL"),
                 def(NullIf.class, NullIf::new),
                 def(Greatest.class, Greatest::new),
                 def(Least.class, Least::new));
        // Date
        addToMap(def(DayName.class, DayName::new, "DAYNAME"),
                def(DayOfMonth.class, DayOfMonth::new, "DAYOFMONTH", "DAY", "DOM"),
                def(DayOfWeek.class, DayOfWeek::new, "DAYOFWEEK", "DOW"),
                def(DayOfYear.class, DayOfYear::new, "DAYOFYEAR", "DOY"),
                def(HourOfDay.class, HourOfDay::new, "HOUR"),
                def(MinuteOfDay.class, MinuteOfDay::new),
                def(MinuteOfHour.class, MinuteOfHour::new, "MINUTE"),
                def(MonthName.class, MonthName::new, "MONTHNAME"),
                def(MonthOfYear.class, MonthOfYear::new, "MONTH"),
                def(SecondOfMinute.class, SecondOfMinute::new, "SECOND"),
                def(Quarter.class, Quarter::new),
                def(Year.class, Year::new),
                def(WeekOfYear.class, WeekOfYear::new, "WEEK"));
        // Math
        addToMap(def(Abs.class, Abs::new),
                def(ACos.class, ACos::new),
                def(ASin.class, ASin::new),
                def(ATan.class, ATan::new),
                def(ATan2.class, ATan2::new),
                def(Cbrt.class, Cbrt::new),
                def(Ceil.class, Ceil::new, "CEILING"),
                def(Cos.class, Cos::new),
                def(Cosh.class, Cosh::new),
                def(Cot.class, Cot::new),
                def(Degrees.class, Degrees::new),
                def(E.class, E::new),
                def(Exp.class, Exp::new),
                def(Expm1.class, Expm1::new),
                def(Floor.class, Floor::new),
                def(Log.class, Log::new),
                def(Log10.class, Log10::new),
                // SQL and ODBC require MOD as a _function_
                def(Mod.class, Mod::new),
                def(Pi.class, Pi::new),
                def(Power.class, Power::new),
                def(Radians.class, Radians::new),
                def(Random.class, Random::new, "RAND"),
                def(Round.class, Round::new),
                def(Sign.class, Sign::new, "SIGNUM"),
                def(Sin.class, Sin::new),
                def(Sinh.class, Sinh::new),
                def(Sqrt.class, Sqrt::new),
                def(Tan.class, Tan::new),
                def(Truncate.class, Truncate::new));
        // String
        addToMap(def(Ascii.class, Ascii::new),
                def(BitLength.class, BitLength::new),
                def(Char.class, Char::new),
                def(CharLength.class, CharLength::new, "CHARACTER_LENGTH"),
                def(Concat.class, Concat::new),
                def(Insert.class, Insert::new),
                def(LCase.class, LCase::new),
                def(Left.class, Left::new),
                def(Length.class, Length::new),
                def(Locate.class, Locate::new),
                def(LTrim.class, LTrim::new),
                def(OctetLength.class, OctetLength::new),
                def(Position.class, Position::new),
                def(Repeat.class, Repeat::new),
                def(Replace.class, Replace::new),
                def(Right.class, Right::new),
                def(RTrim.class, RTrim::new),
                def(Space.class, Space::new),
                def(Substring.class, Substring::new),
                def(UCase.class, UCase::new));
        // DataType conversion
        addToMap(def(Cast.class, Cast::new, "CONVERT"));
        // Scalar "meta" functions
        addToMap(def(Database.class, Database::new),
                def(User.class, User::new));
        // Special
        addToMap(def(Score.class, Score::new));
    }

    void addToMap(FunctionDefinition...functions) {
        // temporary map to hold [function_name/alias_name : function instance]
        Map<String, FunctionDefinition> batchMap = new HashMap<>();
        for (FunctionDefinition f : functions) {
            batchMap.put(f.name(), f);
            for (String alias : f.aliases()) {
                Object old = batchMap.put(alias, f);
                if (old != null || defs.containsKey(alias)) {
                    throw new IllegalArgumentException("alias [" + alias + "] is used by "
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
                        e.getValue().clazz(), e.getValue().datetime(), e.getValue().builder()))
                .collect(toList());
    }

    public Collection<FunctionDefinition> listFunctions(String pattern) {
        // It is worth double checking if we need this copy. These are immutable anyway.
        Pattern p = Strings.hasText(pattern) ? Pattern.compile(normalize(pattern)) : null;
        return defs.entrySet().stream()
                .filter(e -> p == null || p.matcher(e.getKey()).matches())
                .map(e -> new FunctionDefinition(e.getKey(), emptyList(),
                        e.getValue().clazz(), e.getValue().datetime(), e.getValue().builder()))
                .collect(toList());
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that
     * is not aware of time zone and does not support {@code DISTINCT}.
     */
    static <T extends Function> FunctionDefinition def(Class<T> function,
            java.util.function.Function<Location, T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            if (false == children.isEmpty()) {
                throw new IllegalArgumentException("expects no arguments");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.apply(location);
        };
        return def(function, builder, false, aliases);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a no-argument function that
     * is not aware of time zone, does not support {@code DISTINCT} and needs
     * the cluster name (DATABASE()) or the user name (USER()).
     */
    @SuppressWarnings("overloads")
    static <T extends Function> FunctionDefinition def(Class<T> function,
            ConfigurationAwareFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            if (false == children.isEmpty()) {
                throw new IllegalArgumentException("expects no arguments");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(location, cfg);
        };
        return def(function, builder, false, aliases);
    }
    
    interface ConfigurationAwareFunctionBuilder<T> {
        T build(Location location, Configuration configuration);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not
     * aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            BiFunction<Location, Expression, T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new IllegalArgumentException("expects exactly one argument");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.apply(location, children.get(0));
        };
        return def(function, builder, false, aliases);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for multi-arg function that
     * is not aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            MultiFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(location, children);
        };
        return def(function, builder, false, aliases);
    }

    interface MultiFunctionBuilder<T> {
        T build(Location location, List<Expression> children);
    }
    
    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not
     * aware of time zone but does support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            DistinctAwareUnaryFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new IllegalArgumentException("expects exactly one argument");
            }
            return ctorRef.build(location, children.get(0), distinct);
        };
        return def(function, builder, false, aliases);
    }

    interface DistinctAwareUnaryFunctionBuilder<T> {
        T build(Location location, Expression target, boolean distinct);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that
     * operates on a datetime.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            DatetimeUnaryFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            if (children.size() != 1) {
                throw new IllegalArgumentException("expects exactly one argument");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(location, children.get(0), cfg.timeZone());
        };
        return def(function, builder, true, aliases);
    }

    interface DatetimeUnaryFunctionBuilder<T> {
        T build(Location location, Expression target, TimeZone tz);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that is
     * not aware of time zone and does not support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            BinaryFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            boolean isBinaryOptionalParamFunction = function.isAssignableFrom(Round.class) || function.isAssignableFrom(Truncate.class);
            if (isBinaryOptionalParamFunction && (children.size() > 2 || children.size() < 1)) {
                throw new IllegalArgumentException("expects one or two arguments");
            } else if (!isBinaryOptionalParamFunction && children.size() != 2) {
                throw new IllegalArgumentException("expects exactly two arguments");
            }

            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(location, children.get(0), children.size() == 2 ? children.get(1) : null);
        };
        return def(function, builder, false, aliases);
    }

    interface BinaryFunctionBuilder<T> {
        T build(Location location, Expression lhs, Expression rhs);
    }

    @SuppressWarnings("overloads")
    private static FunctionDefinition def(Class<? extends Function> function, FunctionBuilder builder,
            boolean datetime, String... aliases) {
        String primaryName = normalize(function.getSimpleName());
        FunctionDefinition.Builder realBuilder = (uf, distinct, cfg) -> {
            try {
                return builder.build(uf.location(), uf.children(), distinct, cfg);
            } catch (IllegalArgumentException e) {
                throw new ParsingException("error building [" + primaryName + "]: " + e.getMessage(), e,
                        uf.location().getLineNumber(), uf.location().getColumnNumber());
            }
        };
        return new FunctionDefinition(primaryName, unmodifiableList(Arrays.asList(aliases)), function, datetime, realBuilder);
    }

    private interface FunctionBuilder {
        Function build(Location location, List<Expression> children, boolean distinct, Configuration cfg);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            ThreeParametersFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            boolean isLocateFunction = function.isAssignableFrom(Locate.class);
            if (isLocateFunction && (children.size() > 3 || children.size() < 2)) {
                throw new IllegalArgumentException("expects two or three arguments");
            } else if (!isLocateFunction && children.size() != 3) {
                throw new IllegalArgumentException("expects exactly three arguments");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(location, children.get(0), children.get(1), children.size() == 3 ? children.get(2) : null);
        };
        return def(function, builder, false, aliases);
    }

    interface ThreeParametersFunctionBuilder<T> {
        T build(Location location, Expression source, Expression exp1, Expression exp2);
    }

    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    static <T extends Function> FunctionDefinition def(Class<T> function,
            FourParametersFunctionBuilder<T> ctorRef, String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) -> {
            if (children.size() != 4) {
                throw new IllegalArgumentException("expects exactly four arguments");
            }
            if (distinct) {
                throw new IllegalArgumentException("does not support DISTINCT yet it was specified");
            }
            return ctorRef.build(location, children.get(0), children.get(1), children.get(2), children.get(3));
        };
        return def(function, builder, false, aliases);
    }

    interface FourParametersFunctionBuilder<T> {
        T build(Location location, Expression source, Expression exp1, Expression exp2, Expression exp3);
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
                                                               String... aliases) {
        FunctionBuilder builder = (location, children, distinct, cfg) ->
            ctorRef.build(location, children.get(0), children.get(0).dataType());
        return def(function, builder, false, aliases);
    }

    private interface CastFunctionBuilder<T> {
        T build(Location location, Expression expression, DataType dataType);
    }

    private static String normalize(String name) {
        if (EXCLUDE_FROM_NAME_NORMALIZATION.contains(name)) {
            return name.toUpperCase(Locale.ROOT);
        }

        // translate CamelCase to CAMEL_CASE
        return StringUtils.camelCaseToUnderscore(name);
    }
}
