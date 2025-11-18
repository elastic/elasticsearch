/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.parser.EsqlParser;

import java.util.Set;

import static org.elasticsearch.xpack.esql.session.IndexResolver.ALL_FIELDS;
import static org.elasticsearch.xpack.esql.session.IndexResolver.INDEX_METADATA_FIELD;
import static org.hamcrest.Matchers.equalTo;

public class FieldNameUtilsTests extends ESTestCase {

    private static final EsqlParser parser = new EsqlParser();

    public void testBasicFromCommand() {
        assertFieldNames("from test", ALL_FIELDS);
    }

    public void testBasicFromCommandWithInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("from test | inline stats max(salary) by gender", ALL_FIELDS);
    }

    public void testBasicFromCommandWithMetadata() {
        assertFieldNames("from test metadata _index, _id, _version", ALL_FIELDS);
    }

    public void testBasicFromCommandWithMetadata_AndInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("from test metadata _index, _id, _version | inline stats max(salary)", ALL_FIELDS);
    }

    public void testBasicEvalAndDrop() {
        assertFieldNames("from test | eval x = 1 | drop x", ALL_FIELDS);
    }

    public void testSimple1() {
        assertFieldNames(
            "from employees | sort emp_no | keep emp_no, still_hired | limit 3",
            Set.of("_index", "emp_no", "emp_no.*", "still_hired", "still_hired.*")
        );
    }

    public void testSimple2() {
        assertFieldNames("""
            FROM employees
            | WHERE emp_no == "2"
            """, IndexResolver.ALL_FIELDS);
    }

    public void testDirectFilter() {
        assertFieldNames(
            "from employees | sort emp_no | where still_hired | keep emp_no | limit 3",
            Set.of("_index", "emp_no", "emp_no.*", "still_hired", "still_hired.*")
        );
    }

    public void testForkEval() {
        assertFieldNames("FROM employees | fork (eval x = 1 | keep x) (eval y = 2 | keep y) (eval z = 3 | keep z)", Set.of("*"));
    }

    public void testSort1() {
        assertFieldNames(
            "from employees | sort still_hired, emp_no | keep emp_no, still_hired | limit 3",
            Set.of("_index", "emp_no", "emp_no.*", "still_hired", "still_hired.*")
        );
    }

    public void testStatsBy() {
        assertFieldNames(
            "from employees | stats avg(salary) by still_hired | sort still_hired",
            Set.of("_index", "salary", "salary.*", "still_hired", "still_hired.*")
        );
    }

    public void testStatsByAlwaysTrue() {
        assertFieldNames(
            "from employees | where first_name is not null | eval always_true = starts_with(first_name, \"\") "
                + "| stats avg(salary) by always_true",
            Set.of("_index", "first_name", "first_name.*", "salary", "salary.*")
        );
    }

    public void testStatsByAlwaysFalse() {
        assertFieldNames(
            "from employees | where first_name is not null "
                + "| eval always_false = starts_with(first_name, \"nonestartwiththis\") "
                + "| stats avg(salary) by always_false",
            Set.of("_index", "first_name", "first_name.*", "salary", "salary.*")
        );
    }

    public void testIn1() {
        assertFieldNames(
            "from employees | keep emp_no, is_rehired, still_hired "
                + "| where is_rehired in (still_hired, true) | where is_rehired != still_hired",
            Set.of("_index", "emp_no", "emp_no.*", "is_rehired", "is_rehired.*", "still_hired", "still_hired.*")
        );
    }

    public void testConvertFromString1() {
        assertFieldNames("""
            from employees
            | keep emp_no, is_rehired, first_name
            | eval rehired_str = to_string(is_rehired)
            | eval rehired_bool = to_boolean(rehired_str)
            | eval all_false = to_boolean(first_name)
            | drop first_name
            | limit 5""", Set.of("_index", "emp_no", "emp_no.*", "is_rehired", "is_rehired.*", "first_name", "first_name.*"));
    }

    public void testConvertFromDouble1() {
        assertFieldNames(
            """
                from employees
                | eval h_2 = height - 2.0, double2bool = to_boolean(h_2)
                | where emp_no in (10036, 10037, 10038)
                | keep emp_no, height, *2bool""",
            Set.of("_index", "height", "height.*", "emp_no", "emp_no.*", "h_2", "h_2.*", "*2bool.*", "*2bool")
        );
        // TODO asking for more shouldn't hurt. Can we do better? ("h_2" shouldn't be in the list of fields)
        // Set.of("_index", "height", "height.*", "emp_no", "emp_no.*", "*2bool.*", "*2bool"));
    }

    public void testConvertFromIntAndLong() {
        assertFieldNames(
            "from employees | keep emp_no, salary_change*"
                + "| eval int2bool = to_boolean(salary_change.int), long2bool = to_boolean(salary_change.long) | limit 10",
            Set.of(
                "_index",
                "emp_no",
                "emp_no.*",
                "salary_change*",
                "salary_change.int.*",
                "salary_change.int",
                "salary_change.long.*",
                "salary_change.long"
            )
        );
    }

    public void testIntToInt() {
        assertFieldNames("""
            from employees
            | where emp_no < 10002
            | keep emp_no""", Set.of("_index", "emp_no", "emp_no.*"));
    }

    public void testLongToLong() {
        assertFieldNames(
            """
                from employees
                | where languages.long < avg_worked_seconds
                | limit 1
                | keep emp_no""",
            Set.of("_index", "emp_no", "emp_no.*", "languages.long", "languages.long.*", "avg_worked_seconds", "avg_worked_seconds.*")
        );
    }

    public void testDateToDate() {
        assertFieldNames("""
            from employees
            | where birth_date < hire_date
            | keep emp_no
            | sort emp_no
            | limit 1""", Set.of("_index", "birth_date", "birth_date.*", "emp_no", "emp_no.*", "hire_date", "hire_date.*"));
    }

    public void testTwoConditionsWithDefault() {
        assertFieldNames("""
            from employees
            | eval type = case(languages <= 1, "monolingual", languages <= 2, "bilingual", "polyglot")
            | keep emp_no, type
            | limit 10""", Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*"));
    }

    public void testSingleCondition() {
        assertFieldNames("""
            from employees
            | eval g = case(gender == "F", true)
            | keep gender, g
            | limit 10""", Set.of("_index", "gender", "gender.*"));
    }

    public void testConditionIsNull() {
        assertFieldNames("""
            from employees
            | eval g = case(gender == "F", 1, languages > 1, 2, 3)
            | keep gender, languages, g
            | limit 25""", Set.of("_index", "gender", "gender.*", "languages", "languages.*"));
    }

    public void testEvalAssign() {
        assertFieldNames(
            "from employees | sort hire_date | eval x = hire_date | keep emp_no, x | limit 5",
            Set.of("_index", "hire_date", "hire_date.*", "emp_no", "emp_no.*")
        );
    }

    public void testMinMax() {
        assertFieldNames("from employees | stats min = min(hire_date), max = max(hire_date)", Set.of("_index", "hire_date", "hire_date.*"));
    }

    public void testEvalDateTruncIntervalExpressionPeriod() {
        assertFieldNames(
            "from employees | sort hire_date | eval x = date_trunc(hire_date, 1 month) | keep emp_no, hire_date, x | limit 5",
            Set.of("_index", "hire_date", "hire_date.*", "emp_no", "emp_no.*")
        );
    }

    public void testEvalDateTruncGrouping() {
        assertFieldNames("""
            from employees
            | eval y = date_trunc(hire_date, 1 year)
            | stats count(emp_no) by y
            | sort y
            | keep y, `count(emp_no)`
            | limit 5""", Set.of("_index", "hire_date", "hire_date.*", "emp_no", "emp_no.*"));
    }

    public void testIn2() {
        assertFieldNames("""
            from employees
            | eval x = date_trunc(hire_date, 1 year)
            | where birth_date not in (x, hire_date)
            | keep x, hire_date
            | sort x desc
            | limit 4""", Set.of("_index", "hire_date", "hire_date.*", "birth_date", "birth_date.*"));
    }

    public void testBucketMonth() {
        assertFieldNames("""
            from employees
            | where hire_date >= "1985-01-01T00:00:00Z" and hire_date < "1986-01-01T00:00:00Z"
            | eval hd = bucket(hire_date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | sort hire_date
            | keep hire_date, hd""", Set.of("_index", "hire_date", "hire_date.*"));
    }

    public void testBorn_before_today() {
        assertFieldNames(
            "from employees | where birth_date < now() | sort emp_no asc | keep emp_no, birth_date| limit 1",
            Set.of("_index", "birth_date", "birth_date.*", "emp_no", "emp_no.*")
        );
    }

    public void testBucketMonthInAgg() {
        assertFieldNames("""
            FROM employees
            | WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
            | EVAL bucket = BUCKET(hire_date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | STATS AVG(salary) BY bucket
            | SORT bucket""", Set.of("_index", "salary", "salary.*", "hire_date", "hire_date.*"));
    }

    public void testEvalDateParseDynamic() {
        assertFieldNames("""
            from employees
            | where emp_no == 10039 or emp_no == 10040
            | sort emp_no
            | eval birth_date_string = date_format("yyyy-MM-dd", birth_date)
            | eval new_date = date_parse("yyyy-MM-dd", birth_date_string)
            | eval bool = new_date == birth_date
            | keep emp_no, new_date, birth_date, bool""", Set.of("_index", "emp_no", "emp_no.*", "birth_date", "birth_date.*"));
    }

    public void testDateFields() {
        assertFieldNames("""
            from employees
            | where emp_no == 10049 or emp_no == 10050
            | eval year = date_extract("year", birth_date), month = date_extract("month_of_year", birth_date)
            | keep emp_no, year, month""", Set.of("_index", "emp_no", "emp_no.*", "birth_date", "birth_date.*"));
    }

    public void testEvalDissect() {
        assertFieldNames("""
            from employees
            | eval full_name = concat(first_name, " ", last_name)
            | dissect full_name "%{a} %{b}"
            | sort emp_no asc
            | keep full_name, a, b
            | limit 3""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*", "emp_no", "emp_no.*"));
    }

    public void testDissectExpression() {
        assertFieldNames("""
            from employees
            | dissect concat(first_name, " ", last_name) "%{a} %{b}"
            | sort emp_no asc
            | keep a, b
            | limit 3""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*", "emp_no", "emp_no.*"));
    }

    public void testMultivalueInput1() {
        assertFieldNames("""
            from employees
            | where emp_no <= 10006
            | dissect job_positions "%{a} %{b} %{c}"
            | sort emp_no
            | keep emp_no, a, b, c""", Set.of("_index", "emp_no", "emp_no.*", "job_positions", "job_positions.*"));
    }

    public void testLimitZero() {
        assertFieldNames("""
            FROM employees
            | LIMIT 0""", ALL_FIELDS);
    }

    public void testLimitZero_WithInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            FROM employees
            | INLINE STATS COUNT(*), MAX(salary) BY gender
            | LIMIT 0""", ALL_FIELDS);
    }

    public void testDocsDropHeight() {
        assertFieldNames("""
            FROM employees
            | DROP height
            | LIMIT 0""", ALL_FIELDS);
    }

    public void testDocsDropHeight_WithInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            FROM employees
            | DROP height
            | INLINE STATS MAX(salary) BY gender
            | LIMIT 0""", ALL_FIELDS);
    }

    public void testDocsDropHeightWithWildcard() {
        assertFieldNames("""
            FROM employees
            | DROP height*
            | LIMIT 0""", ALL_FIELDS);
    }

    public void testDocsDropHeightWithWildcard_AndInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            FROM employees
            | INLINE STATS MAX(salary) BY gender
            | DROP height*
            | LIMIT 0""", ALL_FIELDS);
    }

    public void testDocsEval() {
        assertFieldNames("""
            FROM employees
            | KEEP first_name, last_name, height
            | EVAL height_feet = height * 3.281, height_cm = height * 100
            | WHERE first_name == "Georgi"
            | LIMIT 1""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*", "height", "height.*"));
    }

    public void testDocsKeepWildcard() {
        assertFieldNames("""
            FROM employees
            | KEEP h*
            | LIMIT 0""", Set.of("_index", "h*"));
    }

    public void testDocsKeepDoubleWildcard() {
        assertFieldNames("""
            FROM employees
            | KEEP h*, *
            | LIMIT 0""", ALL_FIELDS);
    }

    public void testDocsRename() {
        assertFieldNames("""
            FROM employees
            | KEEP first_name, last_name, still_hired
            | RENAME  still_hired AS employed
            | LIMIT 0""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*", "still_hired", "still_hired.*"));
    }

    public void testDocsRenameMultipleColumns() {
        assertFieldNames("""
            FROM employees
            | KEEP first_name, last_name
            | RENAME first_name AS fn, last_name AS ln
            | LIMIT 0""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*"));
    }

    public void testDocsStats() {
        assertFieldNames("""
            FROM employees
            | STATS count = COUNT(emp_no) BY languages
            | SORT languages""", Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*"));
    }

    public void testEvalStats() {

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY y""", Set.of("_index"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY y
            | SORT y""", Set.of("_index"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY x = y
            | SORT x""", Set.of("_index"));

        assertFieldNames("""
            FROM employees
            | STATS count = COUNT(*) BY first_name
            | SORT first_name""", Set.of("_index", "first_name", "first_name.*"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY x = y
            | SORT x, first_name""", Set.of("_index", "first_name", "first_name.*"));

        assertFieldNames("""
            FROM employees
            | EVAL first_name = "a"
            | STATS count = COUNT(*) BY first_name
            | SORT first_name""", Set.of("_index"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY first_name = to_upper(y)
            | SORT first_name""", Set.of("_index"));

        assertFieldNames("""
            FROM employees
            | EVAL y = to_upper(first_name), z = "z"
            | STATS count = COUNT(*) BY first_name = to_lower(y), z
            | SORT first_name""", Set.of("_index", "first_name", "first_name.*"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY x = y, z = first_name
            | SORT x, z""", Set.of("_index", "first_name", "first_name.*"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY x = y, first_name
            | SORT x, first_name""", Set.of("_index", "first_name", "first_name.*"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(first_name) BY x = y
            | SORT x
            | DROP first_name""", Set.of("_index", "first_name", "first_name.*"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY x = y
            | MV_EXPAND x""", Set.of("_index"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY first_name, y
            | MV_EXPAND first_name""", Set.of("_index", "first_name", "first_name.*"));

        assertFieldNames("""
            FROM employees
            | MV_EXPAND first_name
            | EVAL y = "a"
            | STATS count = COUNT(*) BY first_name, y
            | SORT y""", Set.of("_index", "first_name", "first_name.*"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | MV_EXPAND y
            | STATS count = COUNT(*) BY x = y
            | SORT x""", Set.of("_index"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY x = y
            | STATS count = COUNT(count) by x
            | SORT x""", Set.of("_index"));

        assertFieldNames("""
            FROM employees
            | EVAL y = "a"
            | STATS count = COUNT(*) BY first_name, y
            | STATS count = COUNT(count) by x = y
            | SORT x""", Set.of("_index", "first_name", "first_name.*"));
    }

    public void testSortWithLimitOne_DropHeight() {
        assertFieldNames("from employees | sort languages | limit 1 | drop height*", ALL_FIELDS);
    }

    public void testSortWithLimitOne_DropHeight_WithInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("from employees | inline stats avg(salary) by languages | sort languages | limit 1 | drop height*", ALL_FIELDS);
    }

    public void testDropAllColumns() {
        assertFieldNames("from employees | keep height | drop height | eval x = 1", Set.of("_index", "height", "height.*"));
    }

    public void testDropAllColumns_WithStats() {
        assertFieldNames(
            "from employees | keep height | drop height | eval x = 1 | stats c=count(x), mi=min(x), s=sum(x)",
            Set.of("_index", "height", "height.*")
        );
    }

    public void testEnrichOn() {
        assertFieldNames(
            """
                from employees
                | sort emp_no
                | limit 1
                | eval x = to_string(languages)
                | enrich languages_policy on x
                | keep emp_no, language_name""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testEnrichOn2() {
        assertFieldNames(
            """
                from employees
                | eval x = to_string(languages)
                | enrich languages_policy on x
                | keep emp_no, language_name
                | sort emp_no
                | limit 1""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testUselessEnrich() {
        assertFieldNames("""
            from employees
            | eval x = "abc"
            | enrich languages_policy on x
            | limit 1""", ALL_FIELDS);
    }

    public void testSimpleSortLimit() {
        assertFieldNames(
            """
                from employees
                | eval x = to_string(languages)
                | enrich languages_policy on x
                | keep emp_no, language_name
                | sort emp_no
                | limit 1""",
            Set.of("_index", "languages", "languages.*", "emp_no", "emp_no.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testWith() {
        assertFieldNames(
            """
                from employees | eval x = to_string(languages) | keep emp_no, x | sort emp_no | limit 1
                | enrich languages_policy on x with language_name""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testWithAlias() {
        assertFieldNames(
            """
                from employees  | sort emp_no | limit 3 | eval x = to_string(languages) | keep emp_no, x
                | enrich languages_policy on x with lang = language_name""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testWithAliasSort() {
        assertFieldNames(
            """
                from employees | eval x = to_string(languages) | keep emp_no, x  | sort emp_no | limit 3
                | enrich languages_policy on x with lang = language_name""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testWithAliasAndPlain() {
        assertFieldNames(
            """
                from employees  | sort emp_no desc | limit 3 | eval x = to_string(languages) | keep emp_no, x
                | enrich languages_policy on x with lang = language_name, language_name""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testWithTwoAliasesSameProp() {
        assertFieldNames(
            """
                from employees  | sort emp_no | limit 1 | eval x = to_string(languages) | keep emp_no, x
                | enrich languages_policy on x with lang = language_name, lang2 = language_name""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testRedundantWith() {
        assertFieldNames(
            """
                from employees  | sort emp_no | limit 1 | eval x = to_string(languages) | keep emp_no, x
                | enrich languages_policy on x with language_name, language_name""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testNullInput() {
        assertFieldNames(
            """
                from employees
                | where emp_no == 10017
                | keep emp_no, gender
                | enrich languages_policy on gender with language_name, language_name""",
            Set.of("_index", "gender", "gender.*", "emp_no", "emp_no.*", "language_name", "language_name.*")
        );
    }

    public void testConstantNullInput() {
        assertFieldNames(
            """
                from employees
                | where emp_no == 10020
                | eval x = to_string(languages)
                | keep emp_no, x
                | enrich languages_policy on x with language_name, language_name""",
            Set.of("_index", "languages", "languages.*", "emp_no", "emp_no.*", "language_name", "language_name.*", "x", "x.*")
        );
    }

    public void testEnrichEval() {
        assertFieldNames(
            """
                from employees
                | eval x = to_string(languages)
                | enrich languages_policy on x with lang = language_name
                | eval language = concat(x, "-", lang)
                | keep emp_no, x, lang, language
                | sort emp_no desc | limit 3""",
            Set.of(
                "_index",
                "emp_no",
                "x",
                "lang",
                "language",
                "language_name",
                "languages",
                "x.*",
                "language_name.*",
                "languages.*",
                "emp_no.*",
                "lang.*",
                "language.*"
            )
        );
    }

    public void testSimple() {
        assertFieldNames(
            """
                from employees
                | eval x = 1, y = to_string(languages)
                | enrich languages_policy on y
                | where x > 1
                | keep emp_no, language_name
                | limit 1""",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "y", "x.*", "y.*")
        );
    }

    public void testEvalNullSort() {
        assertFieldNames(
            "from employees | eval x = null | sort x asc, emp_no desc | keep emp_no, x, last_name | limit 2",
            Set.of("_index", "last_name", "last_name.*", "emp_no", "emp_no.*")
        );
    }

    public void testFilterEvalFilter() {
        assertFieldNames("""
            from employees
            | where emp_no < 100010
            | eval name_len = length(first_name)
            | where name_len < 4
            | keep first_name
            | sort first_name""", Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*"));
    }

    public void testEvalWithIsNullIsNotNull() {
        assertFieldNames(
            """
                from employees
                | eval true_bool = null is null, false_bool = null is not null, negated_true = not(null is null)
                | sort emp_no
                | limit 1
                | keep *true*, *false*, first_name, last_name""",
            Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*", "last_name", "last_name.*", "*true*", "*false*")
        );
    }

    public void testInDouble() {
        assertFieldNames(
            "from employees | keep emp_no, height, height.float, height.half_float, height.scaled_float | where height in (2.03)",
            Set.of(
                "_index",
                "emp_no",
                "emp_no.*",
                "height",
                "height.*",
                "height.float",
                "height.float.*",
                "height.half_float",
                "height.half_float.*",
                "height.scaled_float",
                "height.scaled_float.*"
            )
        );
    }

    public void testConvertFromDatetime() {
        assertFieldNames(
            "from employees | sort emp_no | eval hire_double = to_double(hire_date) | keep emp_no, hire_date, hire_double | limit 3",
            Set.of("_index", "emp_no", "emp_no.*", "hire_date", "hire_date.*")
        );
    }

    public void testBucket() {
        assertFieldNames("""
            FROM employees
            | WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
            | EVAL bh = bucket(height, 20, 1.41, 2.10)
            | SORT hire_date
            | KEEP hire_date, height, bh""", Set.of("_index", "hire_date", "hire_date.*", "height", "height.*"));
    }

    public void testEvalGrok() {
        assertFieldNames("""
            from employees
            | eval full_name = concat(first_name, " ", last_name)
            | grok full_name "%{WORD:a} %{WORD:b}"
            | sort emp_no asc
            | keep full_name, a, b
            | limit 3""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*", "emp_no", "emp_no.*"));
    }

    public void testGrokExpression() {
        assertFieldNames("""
            from employees
            | grok concat(first_name, " ", last_name) "%{WORD:a} %{WORD:b}"
            | sort emp_no asc
            | keep a, b
            | limit 3""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*", "emp_no", "emp_no.*"));
    }

    public void testEvalGrokSort() {
        assertFieldNames("""
            from employees
            | eval full_name = concat(first_name, " ", last_name)
            | grok full_name "%{WORD:a} %{WORD:b}"
            | sort a asc
            | keep full_name, a, b
            | limit 3""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*"));
    }

    public void testGrokStats() {
        assertFieldNames("""
            from employees
            | eval x = concat(gender, " foobar")
            | grok x "%{WORD:a} %{WORD:b}"
            | stats n = max(emp_no) by a
            | keep a, n
            | sort a asc""", Set.of("_index", "gender", "gender.*", "emp_no", "emp_no.*"));
    }

    public void testNullOnePattern() {
        assertFieldNames("""
            from employees
            | where emp_no == 10030
            | grok first_name "%{WORD:a}"
            | keep first_name, a""", Set.of("_index", "first_name", "first_name.*", "emp_no", "emp_no.*"));
    }

    public void testMultivalueInput() {
        assertFieldNames("""
            from employees
            | where emp_no <= 10006
            | grok job_positions "%{WORD:a} %{WORD:b} %{WORD:c}"
            | sort emp_no
            | keep emp_no, a, b, c, job_positions""", Set.of("_index", "job_positions", "job_positions.*", "emp_no", "emp_no.*"));
    }

    public void testSelectAll() {
        assertFieldNames("FROM apps metadata _id", ALL_FIELDS);
    }

    public void testFilterById() {
        assertFieldNames("FROM apps metadata _id | WHERE _id == \"4\"", ALL_FIELDS);
    }

    public void testFilterById_WithInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("FROM apps metadata _id | INLINE STATS max(rate) | WHERE _id == \"4\"", ALL_FIELDS);
    }

    public void testKeepId() {
        assertFieldNames("FROM apps metadata _id | WHERE id == 3 | KEEP _id", Set.of("_index", "id", "id.*"));
    }

    public void testIdRangeAndSort() {
        assertFieldNames("""
            FROM apps metadata _id
            | WHERE _id >= "2" AND _id <= "7"
            | SORT _id
            | keep id, name, _id""", Set.of("_index", "id", "id.*", "name", "name.*"));
    }

    public void testOrderById() {
        assertFieldNames("FROM apps metadata _id | KEEP _id, name | SORT _id", Set.of("_index", "name", "name.*"));
    }

    public void testOrderByIdDesc() {
        assertFieldNames("FROM apps metadata _id | KEEP _id, name | SORT _id DESC", Set.of("_index", "name", "name.*"));
    }

    public void testConcatId() {
        assertFieldNames("FROM apps metadata _id | eval c = concat(_id, name) | SORT _id | KEEP c", Set.of("_index", "name", "name.*"));
    }

    public void testStatsOnId() {
        assertFieldNames("FROM apps metadata _id | stats c = count(_id), d = count_distinct(_id)", INDEX_METADATA_FIELD);
    }

    public void testStatsOnIdByGroup() {
        assertFieldNames(
            "FROM apps metadata _id | stats c = count(_id) by name | sort c desc, name | limit 5",
            Set.of("_index", "name", "name.*")
        );
    }

    public void testSimpleProject() {
        assertFieldNames(
            "from hosts | keep card, host, ip0, ip1",
            Set.of("_index", "card", "card.*", "host", "host.*", "ip0", "ip0.*", "ip1", "ip1.*")
        );
    }

    public void testEquals() {
        assertFieldNames(
            "from hosts | sort host, card | where ip0 == ip1 | keep card, host",
            Set.of("_index", "card", "card.*", "host", "host.*", "ip0", "ip0.*", "ip1", "ip1.*")
        );
    }

    public void testConditional() {
        assertFieldNames(
            "from hosts | eval eq=case(ip0==ip1, ip0, ip1) | keep eq, ip0, ip1",
            Set.of("_index", "ip1", "ip1.*", "ip0", "ip0.*")
        );
    }

    public void testWhereWithAverageBySubField() {
        assertFieldNames(
            "from employees | where languages + 1 == 6 | stats avg(avg_worked_seconds) by languages.long",
            Set.of("_index", "languages", "languages.*", "avg_worked_seconds", "avg_worked_seconds.*", "languages.long", "languages.long.*")
        );
    }

    public void testAverageOfEvalValue() {
        assertFieldNames(
            "from employees | eval ratio = salary / height | stats avg(ratio)",
            Set.of("_index", "salary", "salary.*", "height", "height.*")
        );
    }

    public void testTopNProjectEvalProject() {
        assertFieldNames(
            "from employees | sort salary | limit 1 | keep languages, salary | eval x = languages + 1 | keep x",
            Set.of("_index", "salary", "salary.*", "languages", "languages.*")
        );
    }

    public void testMvSum() {
        assertFieldNames("""
            from employees
            | where emp_no > 10008
            | eval salary_change = mv_sum(salary_change.int)
            | sort emp_no
            | keep emp_no, salary_change.int, salary_change
            | limit 7""", Set.of("_index", "emp_no", "emp_no.*", "salary_change.int", "salary_change.int.*"));
    }

    public void testMetaIndexAliasedInAggs() {
        assertFieldNames(
            "from employees metadata _index | eval _i = _index | stats max = max(emp_no) by _i",
            Set.of("_index", "emp_no", "emp_no.*")
        );
    }

    public void testCoalesceFolding() {
        assertFieldNames("""
            FROM employees
            | EVAL foo=COALESCE(true, false, null)
            | SORT emp_no ASC
            | KEEP emp_no, first_name, foo
            | limit 3""", Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*"));
    }

    public void testRenameEvalProject() {
        assertFieldNames(
            "from employees | rename languages as x | keep x | eval z = 2 * x | keep x, z | limit 3",
            Set.of("_index", "languages", "languages.*")
        );
    }

    public void testRenameProjectEval() {
        assertFieldNames("""
            from employees
            | eval y = languages
            | rename languages as x
            | keep x, y
            | eval x2 = x + 1
            | eval y2 = y + 2
            | limit 3""", Set.of("_index", "languages", "languages.*"));
    }

    public void testRenameWithFilterPushedToES() {
        assertFieldNames(
            "from employees | rename emp_no as x | keep languages, first_name, last_name, x | where x > 10030 and x < 10040 | limit 5",
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "first_name", "first_name.*", "last_name", "last_name.*")
        );
    }

    public void testRenameOverride() {
        assertFieldNames(
            "from employees | rename emp_no as languages | keep languages, last_name | limit 3",
            Set.of("_index", "emp_no", "emp_no.*", "last_name", "last_name.*")
        );
    }

    public void testProjectRenameDate() {
        assertFieldNames(
            "from employees | sort hire_date | rename hire_date as x | keep emp_no, x | limit 5",
            Set.of("_index", "hire_date", "hire_date.*", "emp_no", "emp_no.*")
        );
    }

    public void testRenameDrop() {
        assertFieldNames("""
            from employees
            | sort hire_date
            | rename hire_date as x, emp_no as y
            | drop first_name, last_name, gender, birth_date, salary, languages*, height*, still_hired, avg_worked_seconds,
            job_positions, is_rehired, salary_change*
            | limit 5""", ALL_FIELDS);
    }

    public void testMaxOfLong() {
        assertFieldNames("from employees | stats l = max(languages.long)", Set.of("_index", "languages.long", "languages.long.*"));
    }

    public void testGroupByAlias() {
        assertFieldNames(
            "from employees | rename languages as l | keep l, height | stats m = min(height) by l | sort l",
            Set.of("_index", "languages", "languages.*", "height", "height.*")
        );
    }

    public void testByStringAndLong() {
        assertFieldNames("""
            from employees
            | eval trunk_worked_seconds = avg_worked_seconds / 100000000 * 100000000
            | stats c = count(gender) by gender, trunk_worked_seconds
            | sort c desc""", Set.of("_index", "avg_worked_seconds", "avg_worked_seconds.*", "gender", "gender.*"));
    }

    public void testByStringAndLongWithAlias() {
        assertFieldNames("""
            from employees
            | eval trunk_worked_seconds = avg_worked_seconds / 100000000 * 100000000
            | rename gender as g, trunk_worked_seconds as tws
            | keep g, tws
            | stats c = count(g) by g, tws
            | sort c desc""", Set.of("_index", "avg_worked_seconds", "avg_worked_seconds.*", "gender", "gender.*"));
    }

    public void testByStringAndString() {
        assertFieldNames("""
            from employees
            | eval hire_year_str = date_format("yyyy", hire_date)
            | stats c = count(gender) by gender, hire_year_str
            | sort c desc, gender, hire_year_str
            | where c >= 5""", Set.of("_index", "hire_date", "hire_date.*", "gender", "gender.*"));
    }

    public void testByLongAndLong() {
        assertFieldNames("""
            from employees
            | eval trunk_worked_seconds = avg_worked_seconds / 100000000 * 100000000
            | stats c = count(languages.long) by languages.long, trunk_worked_seconds
            | sort c desc""", Set.of("_index", "avg_worked_seconds", "avg_worked_seconds.*", "languages.long", "languages.long.*"));
    }

    public void testByDateAndKeywordAndIntWithAlias() {
        assertFieldNames(
            """
                from employees
                | eval d = date_trunc(hire_date, 1 year)
                | rename gender as g, languages as l, emp_no as e
                | keep d, g, l, e
                | stats c = count(e) by d, g, l
                | sort c desc, d, l desc
                | limit 10""",
            Set.of("_index", "hire_date", "hire_date.*", "gender", "gender.*", "languages", "languages.*", "emp_no", "emp_no.*")
        );
    }

    public void testCountDistinctOfKeywords() {
        assertFieldNames(
            """
                from employees
                | eval hire_year_str = date_format("yyyy", hire_date)
                | stats g = count_distinct(gender), h = count_distinct(hire_year_str)""",
            Set.of("_index", "hire_date", "hire_date.*", "gender", "gender.*")
        );
    }

    public void testCountDistinctOfIpPrecision() {
        assertFieldNames("""
            FROM hosts
            | STATS COUNT_DISTINCT(ip0, 80000), COUNT_DISTINCT(ip1, 5)""", Set.of("_index", "ip0", "ip0.*", "ip1", "ip1.*"));
    }

    public void testPercentileOfLong() {
        assertFieldNames(
            """
                from employees
                | stats p0 = percentile(salary_change.long, 0), p50 = percentile(salary_change.long, 50)""",
            Set.of("_index", "salary_change.long", "salary_change.long.*")
        );
    }

    public void testMedianOfInteger() {
        assertFieldNames("""
            FROM employees
            | STATS MEDIAN(salary), PERCENTILE(salary, 50)""", Set.of("_index", "salary", "salary.*"));
    }

    public void testMedianAbsoluteDeviation() {
        assertFieldNames("""
            FROM employees
            | STATS MEDIAN(salary), MEDIAN_ABSOLUTE_DEVIATION(salary)""", Set.of("_index", "salary", "salary.*"));
    }

    public void testIn3VLWithComputedNull() {
        assertFieldNames(
            """
                from employees
                | where mv_count(job_positions) <= 1
                | where emp_no >= 10024
                | limit 3
                | keep emp_no, job_positions
                | eval nil = concat("", null)
                | eval is_in = job_positions in ("Accountant", "Internship", nil)""",
            Set.of("_index", "job_positions", "job_positions.*", "emp_no", "emp_no.*")
        );
    }

    public void testCase() {
        assertFieldNames("""
            FROM apps
            | EVAL version_text = TO_STR(version)
            | WHERE version IS NULL OR version_text LIKE "1*"
            | EVAL v = TO_VER(CONCAT("123", TO_STR(version)))
            | EVAL m = CASE(version > TO_VER("1.1"), 1, 0)
            | EVAL g = CASE(version > TO_VER("1.3.0"), version, TO_VER("1.3.0"))
            | EVAL i = CASE(version IS NULL, TO_VER("0.1"), version)
            | EVAL c = CASE(
            version > TO_VER("1.1"), "high",
            version IS NULL, "none",
            "low")
            | SORT version DESC NULLS LAST, id DESC
            | KEEP v, version, version_text, id, m, g, i, c""", Set.of("_index", "version", "version.*", "id", "id.*"));
    }

    public void testLikePrefix() {
        assertFieldNames("""
            from employees
            | where first_name like "Eberhar*"
            | keep emp_no, first_name""", Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*"));
    }

    public void testRLikePrefix() {
        assertFieldNames("""
            from employees
            | where first_name rlike "Aleja.*"
            | keep emp_no""", Set.of("_index", "first_name", "first_name.*", "emp_no", "emp_no.*"));
    }

    public void testByUnmentionedLongAndLong() {
        assertFieldNames(
            """
                from employees
                | eval trunk_worked_seconds = avg_worked_seconds / 100000000 * 100000000
                | stats c = count(gender) by languages.long, trunk_worked_seconds
                | sort c desc""",
            Set.of("_index", "avg_worked_seconds", "avg_worked_seconds.*", "languages.long", "languages.long.*", "gender", "gender.*")
        );
    }

    public void testRenameNopProject() {
        assertFieldNames("""
            from employees
            | rename emp_no as emp_no
            | keep emp_no, last_name
            | limit 3""", Set.of("_index", "emp_no", "emp_no.*", "last_name", "last_name.*"));
    }

    public void testRename() {
        assertFieldNames("""
            from test
            | rename emp_no as e
            | keep first_name, e
            """, Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*"));
    }

    public void testChainedRename() {
        assertFieldNames("""
            from test
            | rename emp_no as r1, r1 as r2, r2 as r3
            | keep first_name, r3
            """, Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*", "r1", "r1.*", "r2", "r2.*"));// TODO
                                                                                                                   // asking for
                                                                                                                   // more
                                                                                                                   // shouldn't
        // hurt. Can we do better?
        // Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*"));
    }

    public void testChainedRenameReuse() {
        assertFieldNames("""
            from test
            | rename emp_no as r1, r1 as r2, r2 as r3, first_name as r1
            | keep r1, r3
            """, Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*", "r1", "r1.*", "r2", "r2.*"));// TODO
                                                                                                                   // asking for
                                                                                                                   // more
                                                                                                                   // shouldn't
        // hurt. Can we do better?
        // Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*"));
    }

    public void testRenameBackAndForth() {
        assertFieldNames("""
            from test
            | rename emp_no as r1, r1 as emp_no
            | keep emp_no
            """, Set.of("_index", "emp_no", "emp_no.*", "r1", "r1.*"));// TODO asking for more shouldn't hurt. Can we do better?
        // Set.of("_index", "emp_no", "emp_no.*"));
    }

    public void testRenameReuseAlias() {
        assertFieldNames("""
            from test
            | rename emp_no as e, first_name as e
            """, ALL_FIELDS);
    }

    public void testIfDuplicateNamesGroupingHasPriority() {
        assertFieldNames(
            "from employees | stats languages = avg(height), languages = min(height) by languages | sort languages",
            Set.of("_index", "height", "height.*", "languages", "languages.*")
        );
    }

    public void testCoalesce() {
        assertFieldNames("""
            FROM employees
            | EVAL first_name = COALESCE(first_name, "X")
            | SORT first_name DESC, emp_no ASC
            | KEEP emp_no, first_name
            | limit 10""", Set.of("_index", "first_name", "first_name.*", "emp_no", "emp_no.*"));
    }

    public void testCoalesceBackwards() {
        assertFieldNames("""
            FROM employees
            | EVAL first_name = COALESCE("X", first_name)
            | SORT first_name DESC, emp_no ASC
            | KEEP emp_no, first_name
            | limit 10""", Set.of("_index", "first_name", "first_name.*", "emp_no", "emp_no.*"));
    }

    public void testGroupByVersionCast() {
        assertFieldNames("""
            FROM apps
            | EVAL g = TO_VER(CONCAT("1.", TO_STR(version)))
            | STATS id = MAX(id) BY g
            | SORT id
            | DROP g""", Set.of("_index", "version", "version.*", "id", "id.*"));
    }

    public void testCoalesceEndsInNull() {
        assertFieldNames("""
            FROM employees
            | EVAL first_name = COALESCE(first_name, last_name, null)
            | SORT first_name DESC, emp_no ASC
            | KEEP emp_no, first_name
            | limit 3""", Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*", "emp_no", "emp_no.*"));
    }

    public void testMvAvg() {
        assertFieldNames(
            """
                from employees
                | where emp_no > 10008
                | eval salary_change = mv_avg(salary_change)
                | sort emp_no
                | keep emp_no, salary_change.int, salary_change
                | limit 7""",
            Set.of("_index", "emp_no", "emp_no.*", "salary_change", "salary_change.*", "salary_change.int", "salary_change.int.*")
        );
    }

    public void testEvalOverride() {
        assertFieldNames("""
            from employees
            | eval languages = languages + 1
            | eval languages = languages + 1
            | limit 5
            | keep l*""", Set.of("_index", "languages", "languages.*", "l*"));// subtlety here. Keeping only "languages*" can
                                                                              // remove any other "l*"
        // named fields
    }

    public void testBasicWildcardKeep() {
        assertFieldNames("from test | keep *", ALL_FIELDS);
    }

    public void testBasicWildcardKeep2() {
        assertFieldNames("""
            from test
            | keep un*
            """, Set.of("_index", "un*"));
    }

    public void testWildcardKeep() {
        assertFieldNames("""
            from test
            | keep first_name, *, last_name
            """, ALL_FIELDS);
    }

    public void testProjectThenDropName() {
        assertFieldNames("""
            from test
            | keep *name
            | drop first_name
            """, Set.of("_index", "*name", "*name.*", "first_name", "first_name.*"));
    }

    public void testProjectAfterDropName() {
        assertFieldNames("""
            from test
            | drop first_name
            | keep *name
            """, Set.of("_index", "*name.*", "*name", "first_name", "first_name.*"));
    }

    public void testProjectWithMixedQuoting() {
        assertFieldNames("""
            from test
            | drop first_name
            | keep *`name`
            """, Set.of("_index", "*name.*", "*name", "first_name", "first_name.*"));
    }

    public void testProjectKeepAndDropName() {
        assertFieldNames("""
            from test
            | drop first_name
            | keep last_name
            """, Set.of("_index", "last_name", "last_name.*", "first_name", "first_name.*"));
    }

    public void testProjectDropPattern() {
        assertFieldNames("""
            from test
            | keep *
            | drop *_name
            """, ALL_FIELDS);
    }

    public void testProjectDropPattern_WithInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            from test
            | inline stats max(foo) by bar
            | keep *
            | drop *_name
            """, ALL_FIELDS);
    }

    public void testProjectDropNoStarPattern() {
        assertFieldNames("""
            from test
            | drop *_name
            """, ALL_FIELDS);
    }

    public void testProjectOrderPatternWithRest() {
        assertFieldNames("""
            from test
            | keep *name, *, emp_no
            """, ALL_FIELDS);
    }

    public void testProjectQuotedPatterWithRest() {
        assertFieldNames("""
            from test
            | eval `*alpha`= first_name
            | drop `*alpha`
            | keep *name, *, emp_no
            """, ALL_FIELDS);
    }

    public void testProjectDropPatternAndKeepOthers() {
        assertFieldNames("""
            from test
            | drop l*
            | keep first_name, salary
            """, Set.of("_index", "l*", "first_name", "first_name.*", "salary", "salary.*"));
    }

    public void testProjectDropWithQuotedAndUnquotedPatternAndKeepOthers() {
        assertFieldNames("""
            from test
            | drop `l`*
            | keep first_name, salary
            """, Set.of("_index", "l*", "first_name", "first_name.*", "salary", "salary.*"));
    }

    public void testAliasesThatGetDropped() {
        assertFieldNames("""
            from test
            | eval x = languages + 1
            | where first_name like "%A"
            | eval first_name = concat(first_name, "xyz")
            | drop first_name
            """, ALL_FIELDS);
    }

    public void testWhereClauseNoProjection() {
        assertFieldNames("""
            from test
            | where first_name is not null
            """, ALL_FIELDS);
    }

    public void testCountAllGrouped() {
        assertFieldNames("""
            from test
            | stats c = count(*) by languages
            | rename languages as l
            | sort l DESC
            """, Set.of("_index", "languages", "languages.*"));
    }

    public void testCountAllAndOtherStatGrouped() {
        assertFieldNames("""
            from test
            | stats c = count(*), min = min(emp_no) by languages
            | sort languages
            """, Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*"));
    }

    public void testCountAllAndOtherStatGrouped_WithInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            from test
            | inline stats c = count(*), min = min(emp_no) by languages
            | stats c = count(*), min = min(emp_no) by languages
            | sort languages
            """, Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*"));
    }

    public void testCountAllWithImplicitNameOtherStatGrouped() {
        assertFieldNames("""
            from test
            | stats count(*), min = min(emp_no) by languages
            | drop `count(*)`
            | sort languages
            """, Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*"));
    }

    public void testDropWithQuotedAndUnquotedName() {
        assertFieldNames("""
            from test
            | stats count(*), min = min(emp_no) by languages
            | drop count`(*)`
            | sort languages
            """, Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*"));
    }

    public void testCountAllWithEval() {
        assertFieldNames("""
            from test
            | rename languages as l
            | stats min = min(salary) by l
            | eval x = min + 1
            | stats ca = count(*), cx = count(x) by l
            | sort l
            """, Set.of("_index", "languages", "languages.*", "salary", "salary.*"));
    }

    public void testCountAllWithEval_AndInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            from test
            | rename languages as l
            | inline stats max(salary) by l
            | stats min = min(salary) by l
            | eval x = min + 1
            | stats ca = count(*), cx = count(x) by l
            | sort l
            """, Set.of("_index", "languages", "languages.*", "salary", "salary.*"));
    }

    public void testKeepAfterEval_AndInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            from test
            | rename languages as l
            | inline stats max(salary) by l
            | stats min = min(salary) by l
            | eval x = min + 1
            | keep x, l
            | sort l
            """, Set.of("_index", "languages", "languages.*", "salary", "salary.*"));
    }

    public void testKeepBeforeEval_AndInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            from test
            | rename languages as l
            | keep l, salary, emp_no
            | inline stats max(salary) by l
            | eval x = `max(salary)` + 1
            | stats min = min(salary) by l
            | sort l
            """, Set.of("_index", "languages", "languages.*", "salary", "salary.*", "emp_no", "emp_no.*"));
    }

    public void testStatsBeforeEval_AndInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            from test
            | rename languages as l
            | stats min = min(salary) by l
            | eval salary = min + 1
            | inline stats max(salary) by l
            | sort l
            """, Set.of("_index", "languages", "languages.*", "salary", "salary.*"));
    }

    public void testStatsBeforeInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            from test
            | stats min = min(salary) by languages
            | inline stats max(min) by languages
            """, Set.of("_index", "languages", "languages.*", "salary", "salary.*"));
    }

    public void testKeepBeforeInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            from test
            | keep languages, salary
            | inline stats max(salary) by languages
            """, Set.of("_index", "languages", "languages.*", "salary", "salary.*"));
    }

    public void testCountStar() {
        assertFieldNames("""
            from test
            | stats count=count(*)
            | sort count desc
            | limit 0
            """, INDEX_METADATA_FIELD);
    }

    public void testEnrichOnDefaultFieldWithKeep() {
        assertFieldNames("""
            from employees
            | enrich languages_policy
            | keep emp_no""", true, Set.of("*"), Set.of());
    }

    public void testDissectOverwriteName() {
        assertFieldNames("""
            from employees
            | dissect first_name "%{first_name} %{more}"
            | keep emp_no, first_name, more""", Set.of("_index", "emp_no", "emp_no.*", "first_name", "first_name.*"));
    }

    /**
     * Fix alias removal in regex extraction with JOIN
     * @see <a href="https://github.com/elastic/elasticsearch/issues/127467">ES|QL: pruning of JOINs leads to missing fields</a>
      */
    public void testAvoidGrokAttributesRemoval() {
        assertFieldNames("""
            from message_types
            | eval type = 1
            | lookup join message_types_lookup on message
            | drop  message
            | grok type "%{WORD:b}"
            | stats x = max(b)
            | keep x""", Set.of("_index", "x", "b", "type", "message", "x.*", "message.*", "type.*", "b.*"));
    }

    public void testAvoidGrokAttributesRemoval2() {
        assertFieldNames("""
            from sample_data
            | dissect message "%{type}"
            | drop type
            | lookup join message_types_lookup on message
            | stats count = count(*) by type
            | keep count
            | sort count""", Set.of("_index", "type", "message", "count", "message.*", "type.*", "count.*"));
    }

    public void testAvoidGrokAttributesRemoval3() {
        assertFieldNames(
            """
                from sample_data
                | grok message "%{WORD:type}"
                | drop type
                | lookup join message_types_lookup on message
                | stats max = max(event_duration) by type
                | keep max
                | sort max""",
            Set.of("_index", "type", "event_duration", "message", "max", "event_duration.*", "message.*", "type.*", "max.*")
        );
    }

    /**
     * @see <a href="https://github.com/elastic/elasticsearch/issues/127468">ES|QL: Grok only supports KEYWORD or TEXT values, found expression [type] type [INTEGER]</a>
     */
    public void testAvoidGrokAttributesRemoval4() {
        assertFieldNames("""
            from message_types
            | eval type = 1
            | lookup join message_types_lookup on message
            | drop  message
            | grok type "%{WORD:b}"
            | stats x = max(b)
            | keep x""", Set.of("_index", "x", "b", "type", "message", "x.*", "message.*", "type.*", "b.*"));
    }

    /**
     * @see <a href="https://github.com/elastic/elasticsearch/issues/127468">ES|QL: Grok only supports KEYWORD or TEXT values, found expression [type] type [INTEGER]</a>
     */
    public void testAvoidGrokAttributesRemoval5() {
        assertFieldNames(
            """
                FROM sample_data, employees
                | EVAL client_ip = client_ip::keyword
                | RENAME languages AS language_code
                | LOOKUP JOIN clientips_lookup ON client_ip
                | EVAL type = 1::keyword
                | EVAL type = 2
                | LOOKUP JOIN message_types_lookup ON message
                | LOOKUP JOIN languages_lookup ON language_code
                | DISSECT type "%{type_as_text}"
                | KEEP message
                | WHERE message IS NOT NULL
                | SORT message DESC
                | LIMIT 1""",
            Set.of(
                "_index",
                "message",
                "type",
                "languages",
                "client_ip",
                "language_code",
                "language_code.*",
                "client_ip.*",
                "message.*",
                "type.*",
                "languages.*"
            )

        );
    }

    public void testEnrichOnDefaultField() {
        assertFieldNames("""
            from employees
            | enrich languages_policy""", true, ALL_FIELDS, Set.of());
    }

    public void testMetrics() {
        var query = "TS k8s | STATS bytes=sum(rate(network.total_bytes_in)), sum(rate(network.total_cost)) BY cluster";
        assertFieldNames(
            query,
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "network.total_bytes_in",
                "network.total_bytes_in.*",
                "network.total_cost",
                "network.total_cost.*",
                "cluster",
                "cluster.*"
            )

        );
    }

    public void testLookupJoin() {
        assertFieldNames(
            "FROM employees | KEEP languages | RENAME languages AS language_code | LOOKUP JOIN languages_lookup ON language_code",
            Set.of("_index", "languages", "languages.*", "language_code", "language_code.*"),
            Set.of("languages_lookup") // Since we have KEEP before the LOOKUP JOIN we need to wildcard the lookup index
        );
    }

    public void testLookupJoinKeep() {
        assertFieldNames(
            """
                FROM employees
                | KEEP languages
                | RENAME languages AS language_code
                | LOOKUP JOIN languages_lookup ON language_code
                | KEEP languages, language_code, language_name""",
            Set.of("_index", "languages", "languages.*", "language_code", "language_code.*", "language_name", "language_name.*"),
            Set.of()  // Since we have KEEP after the LOOKUP, we can use the global field names instead of wildcarding the lookup index
        );
    }

    public void testLookupJoinKeepWildcard() {
        assertFieldNames(
            """
                FROM employees
                | KEEP languages
                | RENAME languages AS language_code
                | LOOKUP JOIN languages_lookup ON language_code
                | KEEP language*""",
            Set.of("_index", "language*", "languages", "languages.*", "language_code", "language_code.*"),
            Set.of()  // Since we have KEEP after the LOOKUP, we can use the global field names instead of wildcarding the lookup index
        );
    }

    public void testMultiLookupJoin() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | LOOKUP JOIN message_types_lookup ON message""",
            Set.of("*"), // With no KEEP we should keep all fields
            Set.of() // since global field names are wildcarded, we don't need to wildcard any indices
        );
    }

    public void testMultiLookupJoinKeepBefore() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | KEEP @timestamp, client_ip, event_duration, message
                | LOOKUP JOIN clientips_lookup ON client_ip
                | LOOKUP JOIN message_types_lookup ON message""",
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "client_ip",
                "client_ip.*",
                "event_duration",
                "event_duration.*",
                "message",
                "message.*"
            ),
            Set.of("clientips_lookup", "message_types_lookup") // Since the KEEP is before both JOINS we need to wildcard both indices
        );
    }

    public void testMultiLookupJoinKeepBetween() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | KEEP @timestamp, client_ip, event_duration, message, env
                | LOOKUP JOIN message_types_lookup ON message""",
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "client_ip",
                "client_ip.*",
                "event_duration",
                "event_duration.*",
                "message",
                "message.*",
                "env",
                "env.*"
            ),
            Set.of("message_types_lookup")  // Since the KEEP is before the second JOIN, we need to wildcard the second index
        );
    }

    public void testMultiLookupJoinKeepAfter() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | LOOKUP JOIN message_types_lookup ON message
                | KEEP @timestamp, client_ip, event_duration, message, env, type""",
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "client_ip",
                "client_ip.*",
                "event_duration",
                "event_duration.*",
                "message",
                "message.*",
                "env",
                "env.*",
                "type",
                "type.*"
            ),
            Set.of()  // Since the KEEP is after both JOINs, we can use the global field names
        );
    }

    public void testMultiLookupJoinKeepAfterWildcard() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | LOOKUP JOIN message_types_lookup ON message
                | KEEP *env*, *type*""",
            Set.of("_index", "*env*", "*type*", "client_ip", "client_ip.*", "message", "message.*"),
            Set.of()  // Since the KEEP is after both JOINs, we can use the global field names
        );
    }

    public void testMultiLookupJoinSameIndex() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | EVAL client_ip = message
                | LOOKUP JOIN clientips_lookup ON client_ip""",
            Set.of("*"), // With no KEEP we should keep all fields
            Set.of() // since global field names are wildcarded, we don't need to wildcard any indices
        );
    }

    public void testMultiLookupJoinSameIndexKeepBefore() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | KEEP @timestamp, client_ip, event_duration, message
                | LOOKUP JOIN clientips_lookup ON client_ip
                | EVAL client_ip = message
                | LOOKUP JOIN clientips_lookup ON client_ip""",
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "client_ip",
                "client_ip.*",
                "event_duration",
                "event_duration.*",
                "message",
                "message.*"
            ),
            Set.of("clientips_lookup") // Since there is no KEEP after the last JOIN, we need to wildcard the index
        );
    }

    public void testMultiLookupJoinSameIndexKeepBetween() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | KEEP @timestamp, client_ip, event_duration, message, env
                | EVAL client_ip = message
                | LOOKUP JOIN clientips_lookup ON client_ip""",
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "client_ip",
                "client_ip.*",
                "event_duration",
                "event_duration.*",
                "message",
                "message.*",
                "env",
                "env.*"
            ),
            Set.of("clientips_lookup") // Since there is no KEEP after the last JOIN, we need to wildcard the index
        );
    }

    public void testMultiLookupJoinSameIndexKeepAfter() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | EVAL client_ip = message
                | LOOKUP JOIN clientips_lookup ON client_ip
                | KEEP @timestamp, client_ip, event_duration, message, env""",
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "client_ip",
                "client_ip.*",
                "event_duration",
                "event_duration.*",
                "message",
                "message.*",
                "env",
                "env.*"
            ),
            Set.of()  // Since the KEEP is after both JOINs, we can use the global field names
        );
    }

    public void testInsist_fieldIsMappedToNonKeywordSingleIndex() {
        assumeTrue("UNMAPPED_FIELDS available as snapshot only", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());
        assertFieldNames(
            "FROM partial_mapping_sample_data | INSIST_🐔 client_ip | KEEP @timestamp, client_ip",
            Set.of("_index", "@timestamp", "@timestamp.*", "client_ip", "client_ip.*"),
            Set.of()
        );
    }

    public void testInsist_fieldIsMappedToKeywordSingleIndex() {
        assumeTrue("UNMAPPED_FIELDS available as snapshot only", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());
        assertFieldNames(
            "FROM partial_mapping_sample_data | INSIST_🐔 message | KEEP @timestamp, message",
            Set.of("_index", "@timestamp", "@timestamp.*", "message", "message.*"),
            Set.of()
        );
    }

    public void testInsist_fieldDoesNotExistSingleIndex() {
        assumeTrue("UNMAPPED_FIELDS available as snapshot only", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());
        assertFieldNames(
            "FROM partial_mapping_sample_data | INSIST_🐔 foo | KEEP @timestamp, foo",
            Set.of("_index", "@timestamp", "@timestamp.*", "foo", "foo.*"),
            Set.of()
        );
    }

    public void testInsist_fieldIsUnmappedSingleIndex() {
        assumeTrue("UNMAPPED_FIELDS available as snapshot only", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());
        assertFieldNames(
            "FROM partial_mapping_sample_data | INSIST_🐔 unmapped_message | KEEP @timestamp, unmapped_message",
            Set.of("_index", "@timestamp", "@timestamp.*", "unmapped_message", "unmapped_message.*"),
            Set.of()
        );
    }

    public void testInsist_multiFieldTestSingleIndex() {
        assumeTrue("UNMAPPED_FIELDS available as snapshot only", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());
        assertFieldNames(
            "FROM partial_mapping_sample_data | INSIST_🐔 message, unmapped_message, client_ip, foo | KEEP @timestamp, unmapped_message",
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "message",
                "message.*",
                "unmapped_message",
                "unmapped_message.*",
                "client_ip",
                "client_ip.*",
                "foo",
                "foo.*"
            ),
            Set.of()
        );
    }

    public void testInsist_fieldIsMappedToDifferentTypesMultiIndex() {
        assumeTrue("UNMAPPED_FIELDS available as snapshot only", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());
        assertFieldNames(
            "FROM sample_data_ts_long, sample_data METADATA _index | INSIST_🐔 @timestamp | KEEP _index, @timestamp",
            Set.of("_index", "@timestamp", "@timestamp.*"),
            Set.of()
        );
    }

    public void testInsist_multiFieldMappedMultiIndex() {
        assumeTrue("UNMAPPED_FIELDS available as snapshot only", EsqlCapabilities.Cap.UNMAPPED_FIELDS.isEnabled());
        assertFieldNames(
            """
                FROM sample_data_ts_long, sample_data METADATA _index
                | INSIST_🐔 @timestamp, unmapped_message
                | INSIST_🐔 message, foo
                | KEEP _index, @timestamp, message, foo""",
            Set.of(
                "_index",
                "@timestamp",
                "@timestamp.*",
                "message",
                "message.*",
                "unmapped_message",
                "unmapped_message.*",
                "foo",
                "foo.*"
            ),
            Set.of()
        );
    }

    public void testJoinMaskingKeep() {
        assertFieldNames(
            """
                from languag*
                | eval type = null
                | rename language_name as message
                | lookup join message_types_lookup on message
                | rename type as message
                | lookup join message_types_lookup on message
                | keep `language.name`""",
            Set.of(
                "_index",
                "language.name",
                "type",
                "language_name",
                "message",
                "language_name.*",
                "message.*",
                "type.*",
                "language.name.*"
            )
        );
    }

    public void testJoinMaskingKeep2() {
        assertFieldNames("""
            from languag*
            | eval type = "foo"
            | rename type as message
            | lookup join message_types_lookup on message
            | rename type as message
            | lookup join message_types_lookup on message
            | keep `language.name`""", Set.of("_index", "language.name", "type", "message", "message.*", "type.*", "language.name.*"));
    }

    public void testEnrichMaskingEvalOn() {
        assertFieldNames(
            """
                from employees
                | eval language_name = null
                | enrich languages_policy on languages
                | rename language_name as languages
                | eval languages = length(languages)
                | enrich languages_policy on languages
                | keep emp_no, language_name""",
            Set.of("_index", "emp_no", "language_name", "languages", "language_name.*", "languages.*", "emp_no.*")
        );
    }

    public void testEnrichAndJoinMaskingEvalWh() {
        assertFieldNames(
            """
                from employees
                | eval language_name = null
                | enrich languages_policy on languages
                | rename language_name as languages
                | eval languages = length(languages)
                | enrich languages_policy on languages
                | lookup join message_types_lookup on language_name
                | keep emp_no, language_name""",
            Set.of("_index", "emp_no", "language_name", "languages", "language_name.*", "languages.*", "emp_no.*")
        );
    }

    public void testDropAgainWithWildcardAfterEval() {
        assertFieldNames("""
            from employees
            | eval full_name = 12
            | drop full_name
            | drop *name
            | keep emp_no
            """, Set.of("_index", "emp_no", "emp_no.*", "*name", "*name.*"));
    }

    public void testDropWildcardFieldsAfterRename() {
        assertFieldNames(
            """
                from employees
                | rename first_name AS first_names, last_name AS last_names
                | eval first_names = 1
                | drop first_names
                | drop *_names
                | keep gender""",
            Set.of("_index", "first_name", "first_name.*", "last_name", "last_name.*", "*_names", "*_names.*", "gender", "gender.*")
        );
    }

    public void testDropWildcardFieldsAfterLookupJoins() {
        assertFieldNames("""
            FROM sample_data
            | EVAL client_ip = client_ip::keyword
            | LOOKUP JOIN clientips_lookup ON client_ip
            | LOOKUP JOIN message_types_lookup ON message
            | SORT @timestamp
            | DROP *e""", Set.of("*"), Set.of());
    }

    public void testDropWildcardFieldsAfterLookupJoins2() {
        assertFieldNames("""
            FROM sample_data
            | EVAL client_ip = client_ip::keyword
            | LOOKUP JOIN clientips_lookup ON client_ip
            | DROP *e, client_ip
            | LOOKUP JOIN message_types_lookup ON message
            | SORT @timestamp
            | DROP *e""", Set.of("*"), Set.of());
    }

    public void testDropWildcardFieldsAfterLookupJoinsAndKeep() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | LOOKUP JOIN message_types_lookup ON message
                | KEEP @timestamp, message, *e*
                | SORT @timestamp
                | DROP *e""",
            Set.of("_index", "client_ip", "client_ip.*", "message", "message.*", "@timestamp", "@timestamp.*", "*e*", "*e", "*e.*"),
            Set.of()
        );
    }

    public void testDropWildcardFieldsAfterLookupJoinKeepLookupJoin() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | LOOKUP JOIN clientips_lookup ON client_ip
                | KEEP @timestamp, *e*, client_ip
                | LOOKUP JOIN message_types_lookup ON message
                | SORT @timestamp
                | DROP *e""",
            Set.of("_index", "client_ip", "client_ip.*", "message", "message.*", "@timestamp", "@timestamp.*", "*e*", "*e", "*e.*"),
            Set.of("message_types_lookup")
        );
    }

    public void testDropWildcardFieldsAfterKeepAndLookupJoins() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | KEEP @timestamp, *e*, client_ip
                | LOOKUP JOIN clientips_lookup ON client_ip
                | LOOKUP JOIN message_types_lookup ON message
                | SORT @timestamp
                | DROP *e""",
            Set.of("_index", "client_ip", "client_ip.*", "message", "message.*", "@timestamp", "@timestamp.*", "*e*", "*e", "*e.*"),
            Set.of("clientips_lookup", "message_types_lookup")
        );
    }

    public void testDropWildcardFieldsAfterKeepAndLookupJoins2() {
        assertFieldNames(
            """
                FROM sample_data
                | EVAL client_ip = client_ip::keyword
                | KEEP @timestamp, *e*, client_ip
                | LOOKUP JOIN clientips_lookup ON client_ip
                | DROP *e
                | LOOKUP JOIN message_types_lookup ON message
                | SORT @timestamp
                | DROP *e, client_ip""",
            Set.of("_index", "client_ip", "client_ip.*", "message", "message.*", "@timestamp", "@timestamp.*", "*e*", "*e", "*e.*"),
            Set.of("clientips_lookup", "message_types_lookup")
        );
    }

    public void testForkFieldsWithKeepAfterFork() {
        assertFieldNames("""
            FROM test
            | WHERE a > 2000
            | EVAL b = a + 100
            | FORK (WHERE c > 1 AND a < 10000 | EVAL d = a + 500)
                   (WHERE d > 1000 AND e == "aaa" | EVAL c = a + 200)
            | WHERE x > y
            | KEEP a, b, c, d, x
            """, Set.of("_index", "a", "x", "y", "c", "d", "e", "e.*", "d.*", "y.*", "x.*", "a.*", "c.*"));
    }

    public void testForkFieldsWithKeepBeforeFork() {
        assertFieldNames("""
            FROM test
            | KEEP a, b, c, d, x, y
            | WHERE a > 2000
            | EVAL b = a + 100
            | FORK (WHERE c > 1 AND a < 10000 | EVAL d = a + 500)
                   (WHERE d > 1000 AND e == "aaa" | EVAL c = a + 200)
            | WHERE x > y
            """, Set.of("_index", "x", "y", "a", "d", "e", "b", "c", "e.*", "d.*", "y.*", "x.*", "a.*", "c.*", "b.*"));
    }

    public void testForkFieldsWithNoProjection() {
        assertFieldNames("""
            FROM test
            | WHERE a > 2000
            | EVAL b = a + 100
            | FORK (WHERE c > 1 AND a < 10000 | EVAL d = a + 500)
                   (WHERE d > 1000 AND e == "aaa" | EVAL c = a + 200)
            | WHERE x > y
            """, ALL_FIELDS);
    }

    public void testForkFieldsWithStatsInOneBranch() {
        assertFieldNames("""
            FROM test
            | WHERE a > 2000
            | EVAL b = a + 100
            | FORK (WHERE c > 1 AND a < 10000 | EVAL d = a + 500)
                   (STATS x = count(*), y=min(z))
            | WHERE x > y
            """, Set.of("_index", "x", "y", "a", "c", "z", "y.*", "x.*", "z.*", "a.*", "c.*"));
    }

    public void testForkFieldsWithEnrichAndLookupJoins() {
        assertFieldNames(
            """
                FROM test
                | KEEP a, b, abc, def, z, xyz
                | ENRICH enrich_policy ON abc
                | EVAL b = a + 100
                | LOOKUP JOIN my_lookup_index ON def
                | FORK (WHERE c > 1 AND a < 10000 | EVAL d = a + 500)
                       (STATS x = count(*), y=min(z))
                | LOOKUP JOIN my_lookup_index ON xyz
                | WHERE x > y OR _fork == "fork1"
                """,
            Set.of(
                "_index",
                "x",
                "y",
                "a",
                "c",
                "abc",
                "b",
                "def",
                "z",
                "xyz",
                "def.*",
                "y.*",
                "x.*",
                "xyz.*",
                "z.*",
                "abc.*",
                "a.*",
                "c.*",
                "b.*"
            ),
            Set.of("my_lookup_index")
        );
    }

    public void testForkWithStatsInAllBranches() {
        assertFieldNames("""
            FROM test
            | WHERE a > 2000
            | EVAL b = a + 100
            | FORK (WHERE c > 1 AND a < 10000 | STATS m = count(*))
                   (EVAL z = a * b | STATS m = max(z))
                   (STATS x = count(*), y=min(z))
            | WHERE x > y
            """, Set.of("_index", "x", "y", "c", "a", "z", "y.*", "x.*", "z.*", "a.*", "c.*"));
    }

    public void testForkWithStatsInAllBranches1() {
        assertFieldNames("""
            FROM employees
            | FORK
                   ( STATS x = min(last_name))
                   ( EVAL last_name = first_name  | STATS y = max(last_name))
            """, Set.of("_index", "first_name", "last_name", "first_name.*", "last_name.*"));
    }

    public void testForkWithStatsInAllBranches2() {
        assertFieldNames("""
            FROM employees
            | FORK
                   ( EVAL last_name = first_name  | STATS y = VALUES(last_name))
                   ( STATS x = VALUES(last_name))
            """, Set.of("_index", "first_name", "last_name", "first_name.*", "last_name.*"));
    }

    public void testForkWithStatsAndWhere() {
        assertFieldNames(
            " FROM employees | FORK ( WHERE true | stats min(salary) by gender) ( WHERE true | LIMIT 3 )",
            IndexResolver.ALL_FIELDS
        );
    }

    public void testNullString() {
        assertFieldNames("""
             FROM sample_data
            | EVAL x = null::string
            | STATS COUNT() BY category=CATEGORIZE(x)
            | SORT category""", Set.of("_index"));
    }

    public void testNullStringWithFork() {
        assertFieldNames("""
             FROM sample_data
            | EVAL x = null::string
            | STATS COUNT() BY category=CATEGORIZE(x)
            | SORT category
            | FORK (WHERE true) (WHERE true) | WHERE _fork == "fork1" | DROP _fork""", IndexResolver.ALL_FIELDS);
    }

    public void testSingleFork() {
        assertFieldNames("""
             FROM employees
            | FORK
               ( STATS x = count(*))
               ( WHERE emp_no == "2" )
            | SORT _fork""", IndexResolver.ALL_FIELDS);
    }

    public void testForkRefs1() {
        assertFieldNames("""
            FROM employees
            | KEEP first_name, last_name
            | FORK
               ( EVAL x = first_name)
               ( EVAL x = last_name)
            """, Set.of("_index", "first_name", "last_name", "last_name.*", "first_name.*"));
    }

    public void testForkRefs2() {
        assertFieldNames("""
            FROM employees
            | FORK
               ( KEEP first_name | EVAL x = first_name)
               ( KEEP last_name | EVAL x = last_name)
            """, Set.of("_index", "first_name", "last_name", "last_name.*", "first_name.*"));
    }

    public void testForkRefs3() {
        assertFieldNames("""
            FROM employees
            | FORK
               ( KEEP first_name | EVAL last_name = first_name)
               ( KEEP first_name | EVAL x = first_name)
            """, Set.of("_index", "first_name", "first_name.*"));
    }

    public void testForkRef4() {
        assertFieldNames(
            """
                from employees
                | sort emp_no
                | limit 1
                | FORK
                   (eval x = to_string(languages) | enrich languages_policy on x | keep language_name)
                   (eval y = to_string(emp_no) | enrich languages_policy on y | keep emp_no)
                """,
            Set.of("_index", "emp_no", "emp_no.*", "languages", "languages.*", "language_name", "language_name.*", "x", "x.*", "y", "y.*")
        );
    }

    public void testRerankerAfterFuse() {
        assertFieldNames("""
            FROM books METADATA _id, _index, _score
            | FORK ( WHERE title:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            ( WHERE author:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            | FUSE
            | RERANK "Tolkien" ON title WITH { "inference_id" : "test_reranker" }
            | EVAL _score=ROUND(_score, 2)
            | SORT _score DESC, book_no ASC
            | LIMIT 2
            | KEEP book_no, title, author, _score""", Set.of("_index", "book_no", "title", "author", "title.*", "author.*", "book_no.*"));
    }

    public void testSimpleFuse() {
        assertFieldNames("""
            FROM employees METADATA _id, _index, _score
            | FORK ( WHERE emp_no:10001 )
            ( WHERE emp_no:10002 )
            | FUSE
            | EVAL _score = round(_score, 4)
            | KEEP _score, _fork, emp_no
            | SORT _score, _fork, emp_no""", Set.of("_index", "emp_no", "emp_no.*"));
    }

    public void testFuseWithMatchAndScore() {
        assertFieldNames("""
            FROM books METADATA _id, _index, _score
            | FORK ( WHERE title:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            ( WHERE author:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | EVAL _fork = mv_sort(_fork)
            | EVAL _score = round(_score, 5)
            | KEEP _score, _fork, _id""", Set.of("_index", "title", "author", "title.*", "author.*"));
    }

    public void testFuseWithDisjunctionAndPostFilter() {
        assertFieldNames("""
            FROM books METADATA _id, _index, _score
            | FORK ( WHERE title:"Tolkien" OR author:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            ( WHERE author:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            | FUSE
            | SORT _score DESC, _id, _index
            | EVAL _fork = mv_sort(_fork)
            | EVAL _score = round(_score, 5)
            | KEEP _score, _fork, _id
            | WHERE _score > 0.014""", Set.of("_index", "title", "author", "title.*", "author.*"));
    }

    public void testFuseWithStats() {
        assertFieldNames("""
            FROM books METADATA _id, _index, _score
            | FORK ( WHERE title:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            ( WHERE author:"Tolkien" | SORT _score, _id DESC | LIMIT 3 )
            ( WHERE author:"Ursula K. Le Guin" AND title:"short stories" | SORT _score, _id DESC | LIMIT 3)
            | FUSE
            | STATS count_fork=COUNT(*) BY _fork
            | SORT _fork""", Set.of("_index", "title", "author", "title.*", "author.*"));
    }

    public void testFuseWithMultipleForkBranches() {
        assertFieldNames("""
            FROM books METADATA _id, _index, _score
            | FORK (WHERE author:"Keith Faulkner" AND qstr("author:Rory or author:Beverlie") | SORT _score, _id DESC | LIMIT 3)
            (WHERE author:"Ursula K. Le Guin" | SORT _score, _id DESC | LIMIT 3)
            (WHERE title:"Tolkien" AND author:"Tolkien" AND year > 2000 AND mv_count(author) == 1 | SORT _score, _id DESC | LIMIT 3)
            (WHERE match(author, "Keith Faulkner") AND match(author, "Rory Tyger") | SORT _score, _id DESC | LIMIT 3)
            | FUSE
            | SORT _score DESC, _id, _index
            | EVAL _fork = mv_sort(_fork)
            | EVAL _score = round(_score, 4)
            | EVAL title = trim(substring(title, 1, 20))
            | KEEP _score, author, title, _fork""", Set.of("_index", "author", "title", "year", "title.*", "author.*", "year.*"));
    }

    public void testFuseWithSemanticSearch() {
        assertFieldNames("""
            FROM semantic_text METADATA _id, _score, _index
            | FORK ( WHERE semantic_text_field:"something" | SORT _score DESC | LIMIT 2)
            ( WHERE semantic_text_field:"something else" | SORT _score DESC | LIMIT 2)
            | FUSE
            | SORT _score DESC, _id, _index
            | EVAL _score = round(_score, 4)
            | EVAL _fork = mv_sort(_fork)
            | KEEP _fork, _score, _id, semantic_text_field""", Set.of("_index", "semantic_text_field", "semantic_text_field.*"));
    }

    public void testSimpleFork() {
        assertFieldNames("""
            FROM employees
            | FORK ( WHERE emp_no == 10001 )
            ( WHERE emp_no == 10002 )
            | KEEP emp_no, _fork
            | SORT emp_no""", Set.of("_index", "emp_no", "emp_no.*"));
    }

    public void testSimpleForkWithStats() {
        assertFieldNames("""
            FROM books METADATA _score
            | WHERE author:"Faulkner"
            | EVAL score = round(_score, 2)
            | FORK (SORT score DESC, author | LIMIT 5 | KEEP author, score)
            (STATS total = COUNT(*))
            | SORT _fork, score DESC, author""", Set.of("_index", "score", "author", "score.*", "author.*"));
    }

    public void testForkWithWhereSortAndLimit() {
        assertFieldNames("""
            FROM employees
            | FORK ( WHERE hire_date < "1985-03-01T00:00:00Z" | SORT first_name | LIMIT 5 )
            ( WHERE hire_date < "1988-03-01T00:00:00Z" | SORT first_name | LIMIT 5 )
            | KEEP emp_no, first_name, _fork
            | SORT emp_no, _fork""", Set.of("_index", "emp_no", "first_name", "hire_date", "first_name.*", "hire_date.*", "emp_no.*"));
    }

    public void testFiveFork() {
        assertFieldNames("""
            FROM employees
            | FORK ( WHERE emp_no == 10005 )
            ( WHERE emp_no == 10004 )
            ( WHERE emp_no == 10003 )
            ( WHERE emp_no == 10002 )
            ( WHERE emp_no == 10001 )
            | KEEP  _fork, emp_no
            | SORT _fork""", Set.of("_index", "emp_no", "emp_no.*"));
    }

    public void testForkWithWhereSortDescAndLimit() {
        assertFieldNames(
            """
                FROM employees
                | FORK ( WHERE hire_date < "1985-03-01T00:00:00Z" | SORT first_name DESC | LIMIT 2 )
                ( WHERE hire_date < "1988-03-01T00:00:00Z" | SORT first_name DESC NULLS LAST | LIMIT 2 )
                | KEEP _fork, emp_no, first_name
                | SORT _fork, first_name DESC""",
            Set.of("_index", "first_name", "emp_no", "hire_date", "first_name.*", "hire_date.*", "emp_no.*")
        );
    }

    public void testForkWithCommonPrefilter() {
        assertFieldNames("""
            FROM employees
            | WHERE emp_no > 10050
            | FORK ( SORT emp_no ASC | LIMIT 2 )
            ( SORT emp_no DESC NULLS LAST | LIMIT 2 )
            | KEEP _fork, emp_no
            | SORT _fork, emp_no""", Set.of("_index", "emp_no", "emp_no.*"));
    }

    public void testForkWithSemanticSearchAndScore() {
        assertFieldNames("""
            FROM semantic_text METADATA _id, _score
            | FORK ( WHERE semantic_text_field:"something" | SORT _score DESC | LIMIT 2)
            ( WHERE semantic_text_field:"something else" | SORT _score DESC | LIMIT 2)
            | EVAL _score = round(_score, 4)
            | SORT _fork, _score, _id
            | KEEP _fork, _score, _id, semantic_text_field""", Set.of("_index", "semantic_text_field", "semantic_text_field.*"));
    }

    public void testForkWithEvals() {
        assertFieldNames("""
            FROM employees
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081 | EVAL x = "abc" | EVAL y = 1)
            (WHERE emp_no == 10081 OR emp_no == 10087 | EVAL x = "def" | EVAL z = 2)
            | KEEP _fork, emp_no, x, y, z
            | SORT _fork, emp_no""", Set.of("_index", "emp_no", "x", "y", "z", "y.*", "x.*", "z.*", "emp_no.*"));
    }

    public void testForkWithStats() {
        assertFieldNames("""
            FROM employees
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            (STATS x = COUNT(*), y = MAX(emp_no), z = MIN(emp_no))
            (STATS x = COUNT(*), y = MIN(emp_no))
            | KEEP _fork, emp_no, x, y, z
            | SORT _fork, emp_no""", Set.of("_index", "emp_no", "x", "y", "z", "y.*", "x.*", "z.*", "emp_no.*"));
    }

    public void testForkWithDissect() {
        assertFieldNames(
            """
                FROM employees
                | WHERE emp_no == 10048 OR emp_no == 10081
                | FORK (EVAL a = CONCAT(first_name, " ", emp_no::keyword, " ", last_name)
                | DISSECT a "%{x} %{y} %{z}" )
                (EVAL b = CONCAT(last_name, " ", emp_no::keyword, " ", first_name)
                | DISSECT b "%{x} %{y} %{w}" )
                | KEEP _fork, emp_no, x, y, z, w
                | SORT _fork, emp_no""",
            Set.of(
                "_index",
                "emp_no",
                "x",
                "y",
                "z",
                "w",
                "first_name",
                "last_name",
                "w.*",
                "y.*",
                "last_name.*",
                "x.*",
                "z.*",
                "first_name.*",
                "emp_no.*"
            )
        );
    }

    public void testForkWithMixOfCommands() {
        assertFieldNames(
            """
                FROM employees
                | WHERE emp_no == 10048 OR emp_no == 10081
                | FORK ( EVAL a = CONCAT(first_name, " ", emp_no::keyword, " ", last_name)
                | DISSECT a "%{x} %{y} %{z}"
                | EVAL y = y::keyword )
                ( STATS x = COUNT(*)::keyword, y = MAX(emp_no)::keyword, z = MIN(emp_no)::keyword )
                ( SORT emp_no ASC | LIMIT 2 | EVAL x = last_name )
                ( EVAL x = "abc" | EVAL y = "aaa" )
                | KEEP _fork, emp_no, x, y, z, a
                | SORT _fork, emp_no""",
            Set.of(
                "_index",
                "emp_no",
                "x",
                "y",
                "z",
                "a",
                "first_name",
                "last_name",
                "y.*",
                "last_name.*",
                "x.*",
                "z.*",
                "first_name.*",
                "a.*",
                "emp_no.*"
            )
        );
    }

    public void testForkWithFiltersOnConstantValues() {
        assertFieldNames("""
            FROM employees
            | EVAL z = 1
            | WHERE z == 1
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081 | WHERE z - 1 == 0)
            (WHERE emp_no == 10081 OR emp_no == 10087 | EVAL a = "x" )
            (STATS x = COUNT(*), y = MAX(emp_no), z = MIN(emp_no) | EVAL a = "y" )
            (STATS x = COUNT(*), y = MIN(emp_no))
            | WHERE _fork == "fork2" OR a == "y"
            | KEEP _fork, emp_no, x, y, z
            | SORT _fork, emp_no""", Set.of("_index", "emp_no", "a", "a.*", "emp_no.*"));
    }

    public void testForkWithUnsupportedAttributes() {
        assertFieldNames("""
            FROM heights
            | FORK (SORT description DESC | LIMIT 1 | EVAL x = length(description) )
            (SORT description ASC | LIMIT 1)
            | SORT _fork""", ALL_FIELDS);
    }

    public void testForkAfterLookupJoin() {
        assertFieldNames(
            """
                FROM employees
                | EVAL language_code = languages
                | LOOKUP JOIN languages_lookup ON language_code
                | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
                (WHERE emp_no == 10081 OR emp_no == 10087)
                (WHERE emp_no == 10081 | EVAL language_name = "Klingon")
                | KEEP _fork, emp_no, language_code, language_name
                | SORT _fork, emp_no""",
            Set.of(
                "_index",
                "emp_no",
                "language_code",
                "language_name",
                "languages",
                "_fork",
                "_fork.*",
                "language_code.*",
                "language_name.*",
                "languages.*",
                "emp_no.*"
            )
        );
    }

    public void testForkBeforeLookupJoin() {
        assertFieldNames(
            """
                FROM employees
                | EVAL language_code = languages
                | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
                (WHERE emp_no == 10081 OR emp_no == 10087)
                (WHERE emp_no == 10081 | EVAL language_name = "Klingon")
                | LOOKUP JOIN languages_lookup ON language_code
                | KEEP _fork, emp_no, language_code, language_name
                | SORT _fork, emp_no""",
            Set.of(
                "_index",
                "emp_no",
                "language_code",
                "language_name",
                "languages",
                "_fork",
                "_fork.*",
                "language_code.*",
                "language_name.*",
                "languages.*",
                "emp_no.*"
            )
        );
    }

    public void testForkBranchWithLookupJoin() {
        assertFieldNames(
            """
                FROM employees
                | EVAL language_code = languages
                | FORK (WHERE emp_no == 10048 OR emp_no == 10081 | LOOKUP JOIN languages_lookup ON language_code)
                (WHERE emp_no == 10081 OR emp_no == 10087 | LOOKUP JOIN languages_lookup ON language_code)
                (WHERE emp_no == 10081 | EVAL language_name = "Klingon" | LOOKUP JOIN languages_lookup ON language_code)
                | KEEP _fork, emp_no, language_code, language_name
                | SORT _fork, emp_no""",
            Set.of(
                "_index",
                "emp_no",
                "language_code",
                "language_name",
                "languages",
                "_fork",
                "_fork.*",
                "language_code.*",
                "language_name.*",
                "languages.*",
                "emp_no.*"
            )
        );
    }

    public void testForkBeforeStats() {
        assertFieldNames(
            """
                FROM employees
                | WHERE emp_no == 10048 OR emp_no == 10081
                | FORK ( EVAL a = CONCAT(first_name, " ", emp_no::keyword, " ", last_name)
                | DISSECT a "%{x} %{y} %{z}"
                | EVAL y = y::keyword )
                ( STATS x = COUNT(*)::keyword, y = MAX(emp_no)::keyword, z = MIN(emp_no)::keyword )
                ( SORT emp_no ASC | LIMIT 2 | EVAL x = last_name )
                ( EVAL x = "abc" | EVAL y = "aaa" )
                | STATS c = count(*), m = max(_fork)""",
            Set.of("_index", "first_name", "emp_no", "last_name", "last_name.*", "first_name.*", "emp_no.*")
        );
    }

    public void testForkBeforeStatsWithWhere() {
        assertFieldNames("""
            FROM employees
            | WHERE emp_no == 10048 OR emp_no == 10081
            | FORK ( EVAL a = CONCAT(first_name, " ", emp_no::keyword, " ", last_name)
            | DISSECT a "%{x} %{y} %{z}"
            | EVAL y = y::keyword )
            ( STATS x = COUNT(*)::keyword, y = MAX(emp_no)::keyword, z = MIN(emp_no)::keyword )
            ( SORT emp_no ASC | LIMIT 2 | EVAL x = last_name )
            ( EVAL x = "abc" | EVAL y = "aaa" )
            | STATS a = count(*) WHERE _fork == "fork1",
            b = max(_fork)""", Set.of("_index", "first_name", "emp_no", "last_name", "last_name.*", "first_name.*", "emp_no.*"));
    }

    public void testForkBeforeStatsByWithWhere() {
        assertFieldNames("""
            FROM employees
            | WHERE emp_no == 10048 OR emp_no == 10081
            | FORK ( EVAL a = CONCAT(first_name, " ", emp_no::keyword, " ", last_name)
            | DISSECT a "%{x} %{y} %{z}"
            | EVAL y = y::keyword )
            ( STATS x = COUNT(*)::keyword, y = MAX(emp_no)::keyword, z = MIN(emp_no)::keyword )
            ( SORT emp_no ASC | LIMIT 2 | EVAL x = last_name )
            ( EVAL x = "abc" | EVAL y = "aaa" )
            | STATS a = count(*)  WHERE emp_no > 10000,
            b = max(x) WHERE _fork == "fork1" BY _fork
            | SORT _fork""", Set.of("_index", "emp_no", "x", "first_name", "last_name", "last_name.*", "x.*", "first_name.*", "emp_no.*"));
    }

    public void testForkAfterDrop() {
        assertFieldNames("""
            FROM languages
            | DROP language_code
            | FORK ( WHERE language_name == "English" | EVAL x = 1 )
            ( WHERE language_name != "English" )
            | SORT _fork, language_name""", ALL_FIELDS);
    }

    public void testForkBranchWithDrop() {
        assertFieldNames(
            """
                FROM languages
                | FORK ( EVAL x = 1 | DROP language_code | WHERE language_name == "English" | DROP x )
                ( WHERE language_name != "English" )
                | SORT _fork, language_name
                | KEEP language_name, language_code, _fork""",
            Set.of("_index", "language_name", "language_code", "language_code.*", "language_name.*")
        );
    }

    public void testForkBeforeDrop() {
        assertFieldNames("""
            FROM languages
            | FORK (WHERE language_code == 1 OR language_code == 2)
            (WHERE language_code == 1)
            | DROP language_code
            | SORT _fork, language_name""", ALL_FIELDS);
    }

    public void testForkBranchWithKeep() {
        assertFieldNames("""
            FROM languages
            | FORK ( WHERE language_name == "English" | KEEP language_name, language_code )
            ( WHERE language_name != "English" )
            | SORT _fork, language_name""", Set.of("_index", "language_name", "language_code", "language_code.*", "language_name.*"));
    }

    public void testForkBeforeRename() {
        assertFieldNames("""
            FROM languages
            | FORK (WHERE language_code == 1 OR language_code == 2)
            (WHERE language_code == 1)
            | RENAME language_code AS code
            | SORT _fork, language_name""", ALL_FIELDS);
    }

    public void testForkBranchWithRenameAs() {
        assertFieldNames(
            """
                FROM languages
                | FORK (RENAME language_code AS code | WHERE code == 1 OR code == 2)
                (WHERE language_code == 1 | RENAME language_code AS x)
                | SORT _fork, language_name
                | KEEP code, language_name, x, _fork""",
            Set.of("_index", "language_name", "language_code", "language_code.*", "language_name.*")
        );
    }

    public void testForkBranchWithRenameEquals() {
        assertFieldNames(
            """
                FROM languages
                | FORK (RENAME code = language_code | WHERE code == 1 OR code == 2)
                (WHERE language_code == 1 | RENAME x = language_code)
                | SORT _fork, language_name
                | KEEP code, language_name, x, _fork""",
            Set.of("_index", "language_name", "language_code", "language_code.*", "language_name.*")
        );
    }

    public void testForkAfterRename() {
        assertFieldNames("""
            FROM languages
            | RENAME language_code AS code
            | FORK (WHERE code == 1 OR code == 2)
            (WHERE code == 1)
            | SORT _fork, language_name""", ALL_FIELDS);
    }

    public void testForkBeforeDissect() {
        assertFieldNames("""
            FROM employees
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | EVAL x = concat(gender, " foobar")
            | DISSECT x "%{a} %{b}"
            | SORT _fork, emp_no
            | KEEP emp_no, gender, x, a, b, _fork""", Set.of("_index", "emp_no", "gender", "gender.*", "emp_no.*"));
    }

    public void testForkBranchWithDissect() {
        assertFieldNames("""
            FROM employees
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081
            | EVAL x = concat(gender, " foobar")
            | DISSECT x "%{a} %{b}")
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | SORT _fork, emp_no
            | KEEP emp_no, gender, x, a, b, _fork""", Set.of("_index", "emp_no", "gender", "gender.*", "emp_no.*"));
    }

    public void testForkAfterDissect() {
        assertFieldNames("""
            FROM employees
            | EVAL x = concat(gender, " foobar")
            | DISSECT x "%{a} %{b}"
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | SORT _fork, emp_no
            | KEEP emp_no, gender, x, a, b, _fork""", Set.of("_index", "emp_no", "gender", "gender.*", "emp_no.*"));
    }

    public void testForkAfterEnrich() {
        assertFieldNames(
            """
                FROM addresses
                | KEEP city.country.continent.planet.name, city.country.name, city.name
                | EVAL city.name = REPLACE(city.name, "San Francisco", "South San Francisco")
                | ENRICH city_names ON city.name WITH city.country.continent.planet.name = airport
                | FORK (WHERE city.name != "Amsterdam")
                (WHERE city.country.name == "Japan")
                | SORT _fork, city.name""",
            Set.of(
                "_index",
                "city.name",
                "airport",
                "city.country.continent.planet.name",
                "city.country.name",
                "city.country.continent.planet.name.*",
                "city.name.*",
                "city.country.name.*",
                "airport.*"
            )
        );
    }

    public void testForkBranchWithEnrich() {
        assertFieldNames(
            """
                FROM addresses
                | KEEP city.country.continent.planet.name, city.country.name, city.name
                | EVAL city.name = REPLACE(city.name, "San Francisco", "South San Francisco")
                | FORK (ENRICH city_names ON city.name WITH city.country.continent.planet.name = airport)
                (ENRICH city_names ON city.name WITH city.country.continent.planet.name = airport)
                | SORT _fork, city.name""",
            Set.of(
                "_index",
                "city.name",
                "airport",
                "city.country.continent.planet.name",
                "city.country.name",
                "city.country.continent.planet.name.*",
                "city.name.*",
                "city.country.name.*",
                "airport.*"
            )
        );
    }

    public void testForkBeforeEnrich() {
        assertFieldNames(
            """
                FROM addresses
                | KEEP city.country.continent.planet.name, city.country.name, city.name
                | EVAL city.name = REPLACE(city.name, "San Francisco", "South San Francisco")
                | FORK (WHERE city.country.name == "Netherlands")
                (WHERE city.country.name != "Japan")
                | ENRICH city_names ON city.name WITH city.country.continent.planet.name = airport
                | SORT _fork, city.name""",
            Set.of(
                "_index",
                "city.name",
                "airport",
                "city.country.name",
                "city.country.continent.planet.name",
                "city.country.continent.planet.name.*",
                "city.name.*",
                "city.country.name.*",
                "airport.*"
            )
        );
    }

    public void testForkBeforeMvExpand() {
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, job_positions
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | MV_EXPAND job_positions
            | SORT _fork, emp_no, job_positions""", Set.of("_index", "emp_no", "job_positions", "job_positions.*", "emp_no.*"));
    }

    public void testForkBranchWithMvExpand() {
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, job_positions
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081 | MV_EXPAND job_positions)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | SORT _fork, emp_no, job_positions""", Set.of("_index", "emp_no", "job_positions", "job_positions.*", "emp_no.*"));
    }

    public void testForkAfterMvExpand() {
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, job_positions
            | MV_EXPAND job_positions
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | SORT _fork, emp_no, job_positions""", Set.of("_index", "emp_no", "job_positions", "job_positions.*", "emp_no.*"));
    }

    public void testForkBeforeInlineStatsIgnore() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, languages, gender
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | INLINE STATS max_lang = MAX(languages) BY gender
            | SORT emp_no, gender, _fork
            | LIMIT 5""", Set.of("_index", "emp_no", "gender", "languages", "gender.*", "languages.*", "emp_no.*"));
    }

    public void testForkBranchWithInlineStatsIgnore() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, languages, gender
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081
            | INLINE STATS x = MAX(languages) BY gender)
            (WHERE emp_no == 10081 OR emp_no == 10087
            | INLINE STATS x = MIN(languages))
            (WHERE emp_no == 10012 OR emp_no == 10012)
            | SORT emp_no, gender, _fork""", Set.of("_index", "emp_no", "gender", "languages", "gender.*", "languages.*", "emp_no.*"));
    }

    public void testForkAfterInlineStatsIgnore() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, languages, gender
            | INLINE STATS max_lang = MAX(languages) BY gender
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | SORT emp_no, gender, _fork""", Set.of("_index", "emp_no", "gender", "languages", "gender.*", "languages.*", "emp_no.*"));
    }

    public void testForkBeforeChangePoint() {
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, salary
            | EVAL salary=CASE(emp_no==10042, 1000000, salary)
            | FORK (WHERE emp_no > 10100)
            (WHERE emp_no <= 10100)
            | CHANGE_POINT salary ON emp_no
            | STATS COUNT() by type
            | SORT type""", Set.of("_index", "type", "emp_no", "salary", "type.*", "salary.*", "emp_no.*"));
    }

    public void testForkBranchWithChangePoint() {
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, salary
            | FORK (EVAL salary=CASE(emp_no==10042, 1000000, salary)
            | CHANGE_POINT salary ON emp_no)
            (EVAL salary=CASE(emp_no==10087, 1000000, salary)
            | CHANGE_POINT salary ON emp_no)
            | STATS COUNT() by type, _fork
            | SORT _fork, type""", Set.of("_index", "type", "emp_no", "salary", "type.*", "salary.*", "emp_no.*"));
    }

    public void testForkAfterChangePoint() {
        assertFieldNames(
            """
                FROM employees
                | KEEP emp_no, salary
                | EVAL salary = CASE(emp_no==10042, 1000000, salary)
                | CHANGE_POINT salary ON emp_no
                | FORK (STATS a = COUNT() by type)
                (STATS b = VALUES(type))
                | SORT _fork, a, type, b""",
            Set.of("_index", "a", "type", "b", "emp_no", "salary", "type.*", "a.*", "salary.*", "b.*", "emp_no.*")
        );
    }

    public void testForkBeforeCompletion() {
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, first_name, last_name
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | COMPLETION x=CONCAT(first_name, " ", last_name) WITH { "inference_id" : "test_completion" }
            | SORT _fork, emp_no""", Set.of("_index", "emp_no", "first_name", "last_name", "last_name.*", "first_name.*", "emp_no.*"));
    }

    public void testForkBranchWithCompletion() {
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, first_name, last_name
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081
            | COMPLETION x=CONCAT(first_name, " ", last_name) WITH { "inference_id" : "test_completion" })
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | SORT _fork, emp_no""", Set.of("_index", "emp_no", "first_name", "last_name", "last_name.*", "first_name.*", "emp_no.*"));
    }

    public void testForkAfterCompletion() {
        assertFieldNames("""
            FROM employees
            | KEEP emp_no, first_name, last_name
            | COMPLETION x=CONCAT(first_name, " ", last_name) WITH { "inference_id" : "test_completion" }
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | SORT _fork, emp_no""", Set.of("_index", "emp_no", "first_name", "last_name", "last_name.*", "first_name.*", "emp_no.*"));
    }

    public void testForkAfterGrok() {
        assertFieldNames("""
            FROM employees
            | EVAL x = concat(gender, " foobar")
            | GROK x "%{WORD:a} %{WORD:b}"
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | SORT _fork, emp_no
            | KEEP emp_no, gender, x, a, b, _fork""", Set.of("_index", "emp_no", "gender", "gender.*", "emp_no.*"));
    }

    public void testForkBranchWithGrok() {
        assertFieldNames(
            """
                FROM employees
                | WHERE emp_no == 10048 OR emp_no == 10081
                | FORK (EVAL a = CONCAT(first_name, " ", emp_no::keyword, " ", last_name)
                | GROK a "%{WORD:x} %{WORD:y} %{WORD:z}" )
                (EVAL b = CONCAT(last_name, " ", emp_no::keyword, " ", first_name)
                | GROK b "%{WORD:x} %{WORD:y} %{WORD:z}" )
                | KEEP _fork, emp_no, x, y, z
                | SORT _fork, emp_no""",
            Set.of(
                "_index",
                "emp_no",
                "x",
                "y",
                "z",
                "first_name",
                "last_name",
                "y.*",
                "last_name.*",
                "x.*",
                "z.*",
                "first_name.*",
                "emp_no.*"
            )
        );
    }

    public void testForkBeforeGrok() {
        assertFieldNames("""
            FROM employees
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081)
            (WHERE emp_no == 10081 OR emp_no == 10087)
            | EVAL x = concat(gender, " foobar")
            | GROK x "%{WORD:a} %{WORD:b}"
            | SORT _fork, emp_no
            | KEEP emp_no, gender, x, a, b, _fork""", Set.of("_index", "emp_no", "gender", "gender.*", "emp_no.*"));
    }

    public void testImplicitFieldNames() {
        assertFieldNames("""
            FROM sample_data
            | STATS x = 1 year + TBUCKET(1 day) BY b1d = TBUCKET(1 day)""", Set.of("_index", "@timestamp", "@timestamp.*"));
    }

    public void testKeepTimestampBeforeStats() {
        assertFieldNames("""
            FROM sample_data
                | WHERE event_duration > 0
                | KEEP @timestamp, client_ip
                | STATS count = COUNT(*), avg_dur = AVG(event_duration) BY hour = TBUCKET(1h), client_ip
                | SORT hour ASC
            """, Set.of("_index", "@timestamp", "@timestamp.*", "client_ip", "client_ip.*", "event_duration", "event_duration.*"));
    }

    public void testKeepAtWildcardBeforeStats() {
        assertFieldNames("""
            FROM sample_data
                | WHERE message LIKE "error%"
                | KEEP @*, message
                | STATS errors = COUNT() BY day = TBUCKET(1d), message
                | SORT day ASC
            """, Set.of("_index", "@timestamp", "@timestamp.*", "@*", "message", "message.*"));
    }

    public void testKeepWildcardBeforeStats() {
        assertFieldNames(
            """
                FROM sample_data
                    | WHERE client_ip IS NOT NULL
                    | KEEP *stamp*, client_ip
                    | STATS p95 = PERCENTILE(event_duration, 95) BY ten_min = TBUCKET(10min), client_ip
                    | SORT ten_min ASC
                """,
            Set.of("_index", "@timestamp", "@timestamp.*", "client_ip", "client_ip.*", "event_duration", "event_duration.*", "*stamp*")
        );
    }

    public void testStatsChainingWithTimestampCarriedForward() {
        assertFieldNames("""
            FROM sample_data
                | KEEP @timestamp, event_duration
                | STATS day_count = COUNT(), day_p95 = PERCENTILE(event_duration, 95) BY day = TBUCKET(1d), @timestamp
                | WHERE day_count > 0
                | STATS hour_count = COUNT(), hour_p95 = PERCENTILE(day_p95, 95)  BY hour = TBUCKET(1h), day
                | SORT day ASC, hour ASC
            """, Set.of("_index", "@timestamp", "@timestamp.*", "event_duration", "event_duration.*"));
    }

    public void testStatsChainingWithTimestampEval() {
        assertFieldNames("""
            FROM sample_data
                | KEEP @timestamp, event_duration, message
                | EVAL t = @timestamp
                | STATS total = COUNT(*), med = MEDIAN(event_duration) BY d = TBUCKET(1d), message, t
                | WHERE total > 5
                | STATS day_total = SUM(total), hour_med = MEDIAN(med) BY h = TBUCKET(1h), message
            """, Set.of("_index", "@timestamp", "@timestamp.*", "event_duration", "event_duration.*", "message", "message.*"));
    }

    public void testStatsChainingWithTimestampCarriedForwardAsByKey() {
        assertFieldNames("""
            FROM sample_data
                | KEEP @timestamp, client_ip, event_duration
                | STATS reqs = COUNT(), max_dur = MAX(event_duration) BY day = TBUCKET(1d), client_ip, @timestamp
                | WHERE max_dur > 1000
                | STATS spikes = COUNT() BY hour = TBUCKET(1h), client_ip, day
            """, Set.of("_index", "@timestamp", "@timestamp.*", "event_duration", "event_duration.*", "client_ip", "client_ip.*"));
    }

    public void testSubqueryInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        // TODO improve FieldNameUtils to process subqueries better, so that we don't call field-caps with "*"
        assertFieldNames("""
            FROM employees, (FROM books | WHERE author:"Faulkner" | KEEP title, author | SORT title | LIMIT 5)
            | WHERE emp_no == 10000 OR author IS NOT NULL
            | KEEP emp_no, first_name, last_name, author, title
            | SORT emp_no, author
            """, Set.of("*"));
    }

    public void testSubqueryInFromWithFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        // nested fork may trigger assertion in FieldNameUtils, defer the check of nested subqueries or subquery with fork
        // to logical plan optimizer.
        // TODO Improve FieldNameUtils to process subqueries better, , so that we don't call field-caps with "*"
        assertFieldNames("""
            FROM employees, (FROM books | FORK (WHERE author:"Faulkner") (WHERE title:"Ring") | KEEP title, author | SORT title | LIMIT 5)
            | WHERE emp_no == 10000 OR author IS NOT NULL
            | KEEP emp_no, first_name, last_name, author, title
            | SORT emp_no, author
            """, Set.of("*"));

        assertFieldNames("""
            FROM books, (FROM employees | WHERE emp_no == 10000)
            | FORK (WHERE author:"Faulkner") (WHERE title:"Ring")
            | KEEP emp_no, first_name, last_name, author, title
            | SORT emp_no, author
            """, Set.of("*"));
    }

    private void assertFieldNames(String query, Set<String> expected) {
        assertFieldNames(query, false, expected, Set.of());
    }

    private void assertFieldNames(String query, Set<String> expected, Set<String> wildCardIndices) {
        assertFieldNames(query, false, expected, wildCardIndices);
    }

    private void assertFieldNames(String query, boolean hasEnriches, Set<String> expected, Set<String> wildCardIndices) {
        var preAnalysisResult = FieldNameUtils.resolveFieldNames(parser.createStatement(query), hasEnriches);
        assertThat("Query-wide field names", preAnalysisResult.fieldNames(), equalTo(expected));
        assertThat("Lookup Indices that expect wildcard lookups", preAnalysisResult.wildcardJoinIndices(), equalTo(wildCardIndices));
    }
}
