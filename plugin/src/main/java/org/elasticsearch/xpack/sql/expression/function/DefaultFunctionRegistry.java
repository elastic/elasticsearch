/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.*;

import org.elasticsearch.xpack.sql.SqlException;
import org.elasticsearch.xpack.sql.expression.function.aggregate.*;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.*;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.*;

import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;
import static org.elasticsearch.xpack.sql.util.CollectionUtils.of;

public class DefaultFunctionRegistry extends AbstractFunctionRegistry {

    private static final Collection<Class<? extends Function>> FUNCTIONS = combine(agg(), scalar());

    private static final Map<String, String> ALIASES = combine(dateTimeAliases());

    @Override
    protected Collection<Class<? extends Function>> functions() {
        return FUNCTIONS;
    }

    @Override
    protected Map<String, String> aliases() {
        return ALIASES;
    }

    private static Collection<Class<? extends Function>> agg() {
        return Arrays.asList(
                Avg.class,
                Count.class,
                Max.class,
                Min.class,
                Sum.class
                );
    }
    
    private static Collection<Class<? extends ScalarFunction>> scalar() {
        return combine(dateTimeFunctions(), 
                mathFunctions());
    }

    private static Collection<Class<? extends ScalarFunction>> dateTimeFunctions() {
        return Arrays.asList(
                DayOfMonth.class,
                DayOfWeek.class,
                DayOfYear.class,
                HourOfDay.class,
                MinuteOfDay.class,
                MinuteOfHour.class,
                SecondOfMinute.class,
                MonthOfYear.class,
                Year.class
                );
    }

    private static Collection<Class<? extends ScalarFunction>> mathFunctions() {
        return Arrays.asList(
                Abs.class,
                ACos.class,
                ASin.class,
                ATan.class,
                Cbrt.class,
                Ceil.class,
                Cos.class,
                Cosh.class,
                Degrees.class,
                E.class,
                Exp.class,
                Expm1.class,
                Floor.class,
                Log.class,
                Log10.class,
                Pi.class,
                Radians.class,
                Round.class,
                Sin.class,
                Sinh.class,
                Sqrt.class,
                Tan.class
                );
    }

    @SuppressWarnings("unchecked")
    private static Collection<Class<? extends ScalarFunction>> functions(Class<? extends ScalarFunction> type) {
        String path = type.getPackage().getName().replace('.', '/');
        ClassLoader cl = type.getClassLoader();
        Enumeration<URL> classes;
        try {
            classes = cl.getResources(path);
        } catch (IOException e1) {
            throw new SqlException("Cannot determine functions in package %s", path);
        }
        
        Collection<Class<? extends ScalarFunction>> collection = new ArrayList<>();

        while(classes.hasMoreElements()) {
            String url = classes.nextElement().toString();
            if (url.endsWith(".class")) {
                Class<?> c;
                try {
                    c = Class.forName(url, false, cl);
                } catch (ClassNotFoundException cnfe) {
                    throw new SqlException(cnfe, "Cannot load class %s", url);
                }
                if (type.isAssignableFrom(c)) {
                    int mod = c.getModifiers();
                    if (Modifier.isPublic(mod) && !Modifier.isAbstract(mod)) {
                        collection.add((Class<? extends ScalarFunction>) c);
                    }
                }
            }
        }

        return collection;
    }

    private static Map<String, String> dateTimeAliases() {
        return of("DAY", "DAY_OF_MONTH", 
                  "DOM", "DAY_OF_MONTH", 
                  "DOW", "DAY_OF_WEEK",
                  "DOY", "DAY_OF_YEAR",
                  "HOUR", "HOUR_OF_DAY",
                  "MINUTE", "MINUTE_OF_HOUR",
                  "MONTH", "MONTH_OF_YEAR",
                  "SECOND", "SECOND_OF_MINUTE");
    }
}