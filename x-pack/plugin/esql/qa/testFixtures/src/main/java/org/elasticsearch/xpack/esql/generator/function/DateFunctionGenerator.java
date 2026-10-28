/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.needsQuoting;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.quote;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.randomUnmappedFieldName;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.shouldAddUnmappedField;

/**
 * Generates random date function expressions.
 */
public final class DateFunctionGenerator {

    private DateFunctionGenerator() {}

    /**
     * Generates a date function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String dateFunction(List<Column> columns, boolean allowUnmapped) {
        String dateField = fieldOrUnmapped(randomName(columns, java.util.Set.of("date", "datetime")), allowUnmapped);
        if (dateField == null) {
            return null;
        }
        String datePart = randomFrom(
            "YEAR",
            "MONTH_OF_YEAR",
            "DAY_OF_MONTH",
            "HOUR_OF_DAY",
            "MINUTE_OF_HOUR",
            "SECOND_OF_MINUTE",
            "DAY_OF_WEEK",
            "DAY_OF_YEAR",
            "ALIGNED_WEEK_OF_YEAR"
        );
        String interval = randomFrom("1 day", "1 hour", "1 week", "1 month", "1 year");
        return randomFrom(
            "date_extract(\"" + datePart + "\", " + dateField + ")",
            "date_trunc(" + interval + ", " + dateField + ")",
            "date_format(\"yyyy-MM-dd\", " + dateField + ")",
            "day_name(" + dateField + ")",
            "month_name(" + dateField + ")",
            "now()"
        );
    }

    /**
     * Generates a date_diff function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String dateDiffFunction(List<Column> columns, boolean allowUnmapped) {
        List<String> dateFields = columns.stream()
            .filter(c -> c.type().equals("date") || c.type().equals("datetime"))
            .map(c -> needsQuoting(c.name()) ? quote(c.name()) : c.name())
            .collect(Collectors.toList());
        if (allowUnmapped && shouldAddUnmappedField()) {
            dateFields.add(randomUnmappedFieldName());
        }
        if (dateFields.size() < 2) {
            return null;
        }
        String unit = randomFrom("second", "minute", "hour", "day", "week", "month", "year");
        return "date_diff(\"" + unit + "\", " + dateFields.get(0) + ", " + dateFields.get(1) + ")";
    }
}
