/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveNotEquals;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveWildcardEquals;
import org.elasticsearch.xpack.eql.expression.predicate.operator.comparison.InsensitiveWildcardNotEquals;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomZone;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public final class EqlTestUtils {

    public static final Version EQL_GA_VERSION = Version.V_7_10_0;

    private EqlTestUtils() {
    }

    public static final EqlConfiguration TEST_CFG = new EqlConfiguration(new String[] {"none"},
            org.elasticsearch.xpack.ql.util.DateUtils.UTC, "nobody", "cluster", null, emptyMap(), null,
            TimeValue.timeValueSeconds(30), null, 123, "", new TaskId("test", 123), null, x -> Collections.emptySet());

    public static EqlConfiguration randomConfiguration() {
        return new EqlConfiguration(new String[]{randomAlphaOfLength(16)},
            randomZone(),
            randomAlphaOfLength(16),
            randomAlphaOfLength(16),
            null,
            emptyMap(),
            null,
            new TimeValue(randomNonNegativeLong()),
            randomIndicesOptions(),
            randomIntBetween(1, 1000),
            randomAlphaOfLength(16),
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomTask(),
            x -> Collections.emptySet());
    }

    public static EqlSearchTask randomTask() {
        return new EqlSearchTask(randomLong(), "transport", EqlSearchAction.NAME, "", null, emptyMap(), emptyMap(),
            new AsyncExecutionId("", new TaskId(randomAlphaOfLength(10), 1)), TimeValue.timeValueDays(5));
    }

    public static InsensitiveEquals seq(Expression left, Expression right) {
        return new InsensitiveWildcardEquals(EMPTY, left, right, randomZone());
    }

    public static InsensitiveNotEquals sneq(Expression left, Expression right) {
        return new InsensitiveWildcardNotEquals(EMPTY, left, right, randomZone());
    }

    public static IndicesOptions randomIndicesOptions() {
        return IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
            randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
    }

    public static SearchSortValues randomSearchSortValues(Object[] values) {
        DocValueFormat[] sortValueFormats = new DocValueFormat[values.length];
        for (int i = 0; i < values.length; i++) {
            sortValueFormats[i] = DocValueFormat.RAW;
        }
        return new SearchSortValues(values, sortValueFormats);
    }

    public static SearchSortValues randomSearchLongSortValues() {
        int size = randomIntBetween(1, 20);
        Object[] values = new Object[size];
        DocValueFormat[] sortValueFormats = new DocValueFormat[size];
        for (int i = 0; i < size; i++) {
            values[i] = randomLong();
            sortValueFormats[i] = DocValueFormat.RAW;
        }
        return new SearchSortValues(values, sortValueFormats);
    }
}
