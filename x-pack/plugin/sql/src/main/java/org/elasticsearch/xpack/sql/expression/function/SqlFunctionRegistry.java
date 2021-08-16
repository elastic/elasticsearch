/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.Check;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
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
import org.elasticsearch.xpack.sql.expression.function.aggregate.StddevSamp;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.aggregate.SumOfSquares;
import org.elasticsearch.xpack.sql.expression.function.aggregate.VarPop;
import org.elasticsearch.xpack.sql.expression.function.aggregate.VarSamp;
import org.elasticsearch.xpack.sql.expression.function.grouping.Histogram;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.function.scalar.Database;
import org.elasticsearch.xpack.sql.expression.function.scalar.User;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.CurrentDate;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.CurrentDateTime;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.CurrentTime;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateAdd;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateDiff;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateParse;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DatePart;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFormat;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParse;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTrunc;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayName;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfMonth;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfWeek;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Format;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.HourOfDay;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.IsoDayOfWeek;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.IsoWeekOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MinuteOfDay;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MinuteOfHour;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthName;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Quarter;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.SecondOfMinute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.TimeParse;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.ToChar;
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
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Trim;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.UCase;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Case;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Greatest;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfNull;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Iif;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Least;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.NullIf;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mod;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class SqlFunctionRegistry extends FunctionRegistry {

    public SqlFunctionRegistry() {
        register(functions());
    }

    SqlFunctionRegistry(FunctionDefinition... functions) {
        register(functions);
    }

    private FunctionDefinition[][] functions() {
        return new FunctionDefinition[][]{
            // Aggregate functions
            new FunctionDefinition[]{
                def(Avg.class, Avg::new, "AVG"),
                def(Count.class, Count::new, "COUNT"),
                def(First.class, First::new, "FIRST", "FIRST_VALUE"),
                def(Last.class, Last::new, "LAST", "LAST_VALUE"),
                def(Max.class, Max::new, "MAX"),
                def(Min.class, Min::new, "MIN"),
                def(Sum.class, Sum::new, "SUM")
            },
            // Statistics
            new FunctionDefinition[]{
                def(Kurtosis.class, Kurtosis::new, "KURTOSIS"),
                def(MedianAbsoluteDeviation.class, MedianAbsoluteDeviation::new, "MAD"),
                def(Percentile.class, Percentile::new, "PERCENTILE"),
                def(PercentileRank.class, PercentileRank::new, "PERCENTILE_RANK"),
                def(Skewness.class, Skewness::new, "SKEWNESS"),
                def(StddevPop.class, StddevPop::new, "STDDEV_POP"),
                def(StddevSamp.class, StddevSamp::new, "STDDEV_SAMP"),
                def(SumOfSquares.class, SumOfSquares::new, "SUM_OF_SQUARES"),
                def(VarPop.class, VarPop::new, "VAR_POP"),
                def(VarSamp.class, VarSamp::new, "VAR_SAMP")
            },
            // histogram
            new FunctionDefinition[]{
                def(Histogram.class, Histogram::new, "HISTOGRAM")
            },
            // Scalar functions
            // Conditional
            new FunctionDefinition[]{
                def(Case.class, Case::new, "CASE"),
                def(Coalesce.class, Coalesce::new, "COALESCE"),
                def(Iif.class, Iif::new, "IIF"),
                def(IfNull.class, (BinaryBuilder<IfNull>) IfNull::new, "IFNULL", "ISNULL", "NVL"),
                def(NullIf.class, NullIf::new, "NULLIF"),
                def(Greatest.class, Greatest::new, "GREATEST"),
                def(Least.class, Least::new, "LEAST")
            },
            // Date
            new FunctionDefinition[]{
                def(CurrentDate.class, CurrentDate::new, "CURRENT_DATE", "CURDATE", "TODAY"),
                def(CurrentTime.class, CurrentTime::new, "CURRENT_TIME", "CURTIME"),
                def(CurrentDateTime.class, CurrentDateTime::new, "CURRENT_TIMESTAMP", "NOW"),
                def(DayName.class, DayName::new, "DAY_NAME", "DAYNAME"),
                def(DayOfMonth.class, DayOfMonth::new, "DAY_OF_MONTH", "DAYOFMONTH", "DAY", "DOM"),
                def(DayOfWeek.class, DayOfWeek::new, "DAY_OF_WEEK", "DAYOFWEEK", "DOW"),
                def(DayOfYear.class, DayOfYear::new, "DAY_OF_YEAR", "DAYOFYEAR", "DOY"),
                def(DateAdd.class, DateAdd::new, "DATEADD", "DATE_ADD", "TIMESTAMPADD", "TIMESTAMP_ADD"),
                def(DateDiff.class, DateDiff::new, "DATEDIFF", "DATE_DIFF", "TIMESTAMPDIFF", "TIMESTAMP_DIFF"),
                def(DateParse.class, DateParse::new, "DATE_PARSE"),
                def(DatePart.class, DatePart::new, "DATEPART", "DATE_PART"),
                def(DateTimeFormat.class, DateTimeFormat::new, "DATETIME_FORMAT"),
                def(DateTimeParse.class, DateTimeParse::new, "DATETIME_PARSE"),
                def(DateTrunc.class, DateTrunc::new, "DATETRUNC", "DATE_TRUNC"),
                def(Format.class, Format::new, "FORMAT"),
                def(ToChar.class, ToChar::new, "TO_CHAR"),
                def(HourOfDay.class, HourOfDay::new, "HOUR_OF_DAY", "HOUR"),
                def(IsoDayOfWeek.class, IsoDayOfWeek::new, "ISO_DAY_OF_WEEK", "ISODAYOFWEEK", "ISODOW", "IDOW"),
                def(IsoWeekOfYear.class, IsoWeekOfYear::new, "ISO_WEEK_OF_YEAR", "ISOWEEKOFYEAR", "ISOWEEK", "IWOY", "IW"),
                def(MinuteOfDay.class, MinuteOfDay::new, "MINUTE_OF_DAY"),
                def(MinuteOfHour.class, MinuteOfHour::new, "MINUTE_OF_HOUR", "MINUTE"),
                def(MonthName.class, MonthName::new, "MONTH_NAME", "MONTHNAME"),
                def(MonthOfYear.class, MonthOfYear::new, "MONTH_OF_YEAR", "MONTH"),
                def(SecondOfMinute.class, SecondOfMinute::new, "SECOND_OF_MINUTE", "SECOND"),
                def(TimeParse.class, TimeParse::new, "TIME_PARSE"),
                def(Quarter.class, Quarter::new, "QUARTER"),
                def(Year.class, Year::new, "YEAR"),
                def(WeekOfYear.class, WeekOfYear::new, "WEEK_OF_YEAR", "WEEK")
            },
            // Math
            new FunctionDefinition[]{
                def(Abs.class, Abs::new, "ABS"),
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
                def(Truncate.class, Truncate::new, "TRUNCATE", "TRUNC")
            },
            // String
            new FunctionDefinition[]{
                def(Ascii.class, Ascii::new, "ASCII"),
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
                def(StartsWith.class, StartsWith::new, "STARTS_WITH"),
                def(Substring.class, Substring::new, "SUBSTRING"),
                def(Trim.class, Trim::new, "TRIM"),
                def(UCase.class, UCase::new, "UCASE")
            },
            // DataType conversion
            new FunctionDefinition[]{
                def(Cast.class, Cast::new, "CAST", "CONVERT")
            },
            // Scalar "meta" functions
            new FunctionDefinition[]{
                def(Database.class, Database::new, "DATABASE"),
                def(User.class, User::new, "USER")
            },
            // Geo Functions
            new FunctionDefinition[]{
                def(StAswkt.class, StAswkt::new, "ST_ASWKT", "ST_ASTEXT"),
                def(StDistance.class, StDistance::new, "ST_DISTANCE"),
                def(StWkttosql.class, StWkttosql::new, "ST_WKTTOSQL", "ST_GEOMFROMTEXT"),
                def(StGeometryType.class, StGeometryType::new, "ST_GEOMETRYTYPE"),
                def(StX.class, StX::new, "ST_X"),
                def(StY.class, StY::new, "ST_Y"),
                def(StZ.class, StZ::new, "ST_Z")
            },
            // Special
            new FunctionDefinition[]{
                def(Score.class, Score::new, "SCORE")
            }
        };
    }

    /**
     * Builder for creating SQL-specific functions.
     * All other methods defined here end up being translated to this form.
     */
    protected interface SqlFunctionBuilder {
        Function build(Source source, List<Expression> children, Configuration cfg, Boolean distinct);
    }

    /**
     * Main method to register a function.
     */
    @SuppressWarnings("overloads")
    protected static FunctionDefinition def(Class<? extends Function> function,
                                            SqlFunctionBuilder builder,
                                            boolean datetime,
                                            String... names) {
        Check.isTrue(names.length > 0, "At least one name must be provided for the function");
        String primaryName = names[0];
        List<String> aliases = Arrays.asList(names).subList(1, names.length);
        FunctionDefinition.Builder realBuilder = (uf, cfg, extras) -> {
            try {
                return builder.build(uf.source(), uf.children(), cfg, asBool(extras));
            } catch (QlIllegalArgumentException e) {
                throw new ParsingException(uf.source(), "error building [" + primaryName + "]: " + e.getMessage(), e);
            }
        };
        return new SqlFunctionDefinition(primaryName, unmodifiableList(aliases), function, datetime, realBuilder);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that is not aware of time zone but does support {@code DISTINCT}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, UnaryDistinctAwareBuilder<T> ctorRef, String... names) {
        SqlFunctionBuilder builder = (source, children, cfg, distinct) -> {
            if (children.size() != 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            return ctorRef.build(source, children.get(0), distinct == null ? Boolean.FALSE : distinct);
        };
        return def(function, builder, false, names);
    }

    protected interface UnaryDistinctAwareBuilder<T> {
        T build(Source source, Expression target, Boolean distinct);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a unary function that requires a timezone.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, UnaryZoneIdAwareBuilder<T> ctorRef, String... names) {
        SqlFunctionBuilder builder = (source, children, cfg, distinct) -> {
            if (children.size() != 1) {
                throw new QlIllegalArgumentException("expects exactly one argument");
            }
            forbidDistinct(source, distinct);
            return ctorRef.build(source, children.get(0), cfg.zoneId());
        };
        return def(function, builder, true, names);
    }

    protected interface UnaryZoneIdAwareBuilder<T> {
        T build(Source source, Expression exp, ZoneId zi);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a binary function that requires a timezone.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, BinaryZoneIdAwareBuilder<T> ctorRef, String... names) {
        SqlFunctionBuilder builder = (source, children, cfg, distinct) -> {
            if (children.size() != 2) {
                throw new QlIllegalArgumentException("expects exactly two arguments");
            }
            forbidDistinct(source, distinct);
            return ctorRef.build(source, children.get(0), children.get(1), cfg.zoneId());
        };
        return def(function, builder, true, names);
    }

    protected interface BinaryZoneIdAwareBuilder<T> {
        T build(Source source, Expression left, Expression right, ZoneId zi);
    }

    /**
     * Build a {@linkplain FunctionDefinition} for a three-args function that requires a timezone.
     */
    @SuppressWarnings("overloads") // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function,
                                                                 TernaryZoneIdAwareBuilder<T> ctorRef,
                                                                 String... names) {
        SqlFunctionBuilder builder = (source, children, cfg, distinct) -> {
            if (children.size() != 3) {
                throw new QlIllegalArgumentException("expects three arguments");
            }
            forbidDistinct(source, distinct);
            return ctorRef.build(source, children.get(0), children.get(1), children.get(2), cfg.zoneId());
        };
        return def(function, builder, true, names);
    }

    protected interface TernaryZoneIdAwareBuilder<T> {
        T build(Source source, Expression first, Expression second, Expression third, ZoneId zi);
    }


    /**
     * Special method to create function definition for Cast as its signature is not compatible with {@link UnresolvedFunction}.
     */
    @SuppressWarnings("overloads")  // These are ambiguous if you aren't using ctor references but we always do
    protected static <T extends Function> FunctionDefinition def(Class<T> function, CastBuilder<T> ctorRef, String... names) {
        SqlFunctionBuilder builder = (source, children, cfg, distinct) -> {
            forbidDistinct(source, distinct);
            return ctorRef.build(source, children.get(0), children.get(0).dataType(), cfg.zoneId());
        };
        return def(function, builder, false, names);
    }

    protected interface CastBuilder<T> {
        T build(Source source, Expression expression, DataType dataType, ZoneId zi);
    }

    private static void forbidDistinct(Source source, Boolean distinct) {
        if (distinct != null) {
            throw new ParsingException(source, "does not support DISTINCT yet it was specified");
        }
    }
}
