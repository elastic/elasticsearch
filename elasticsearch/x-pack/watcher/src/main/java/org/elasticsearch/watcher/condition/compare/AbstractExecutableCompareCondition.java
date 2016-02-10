/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.compare;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.xcontent.ObjectPath;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractExecutableCompareCondition<C extends Condition, R extends Condition.Result>
        extends ExecutableCondition<C, R> {
    static final Pattern DATE_MATH_PATTERN = Pattern.compile("<\\{(.+)\\}>");
    static final Pattern PATH_PATTERN = Pattern.compile("\\{\\{(.+)\\}\\}");

    private final Clock clock;

    public AbstractExecutableCompareCondition(C condition, ESLogger logger, Clock clock) {
        super(condition, logger);
        this.clock = clock;
    }

    @Override
    public R execute(WatchExecutionContext ctx) {
        Map<String, Object> resolvedValues = new HashMap<>();
        try {
            Map<String, Object> model = Variables.createCtxModel(ctx, ctx.payload());
            return doExecute(model, resolvedValues);
        } catch (Exception e) {
            logger.error("failed to execute [{}] condition for [{}]", e, type(), ctx.id());
            if (resolvedValues.isEmpty()) {
                resolvedValues = null;
            }
            return doFailure(resolvedValues, e);
        }
    }

    protected Object resolveConfiguredValue(Map<String, Object> resolvedValues, Map<String, Object> model, Object configuredValue) {
        if (configuredValue instanceof String) {

            // checking if the given value is a date math expression
            Matcher matcher = DATE_MATH_PATTERN.matcher((String) configuredValue);
            if (matcher.matches()) {
                String dateMath = matcher.group(1);
                configuredValue = WatcherDateTimeUtils.parseDateMath(dateMath, DateTimeZone.UTC, clock);
                resolvedValues.put(dateMath, WatcherDateTimeUtils.formatDate((DateTime) configuredValue));
            } else {
                // checking if the given value is a path expression
                matcher = PATH_PATTERN.matcher((String) configuredValue);
                if (matcher.matches()) {
                    String configuredPath = matcher.group(1);
                    configuredValue = ObjectPath.eval(configuredPath, model);
                    resolvedValues.put(configuredPath, configuredValue);
                }
            }
        }
        return configuredValue;
    }

    protected abstract R doExecute(Map<String, Object> model, Map<String, Object> resolvedValues) throws Exception;

    protected abstract R doFailure(Map<String, Object> resolvedValues, Exception e);
}
