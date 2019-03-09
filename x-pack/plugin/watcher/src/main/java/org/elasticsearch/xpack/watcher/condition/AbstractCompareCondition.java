/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class AbstractCompareCondition implements ExecutableCondition {
    static final Pattern DATE_MATH_PATTERN = Pattern.compile("<\\{(.+)\\}>");
    static final Pattern PATH_PATTERN = Pattern.compile("\\{\\{(.+)\\}\\}");

    private final Clock clock;
    private final String type;

    protected AbstractCompareCondition(String type, Clock clock) {
        this.clock = clock;
        this.type = type;
    }

    @Override
    public final Result execute(WatchExecutionContext ctx) {
        Map<String, Object> resolvedValues = new HashMap<>();
        Map<String, Object> model = Variables.createCtxParamsMap(ctx, ctx.payload());
        return doExecute(model, resolvedValues);
    }

    protected Object resolveConfiguredValue(Map<String, Object> resolvedValues, Map<String, Object> model, Object configuredValue) {
        if (configuredValue instanceof String) {

            // checking if the given value is a date math expression
            Matcher matcher = DATE_MATH_PATTERN.matcher((String) configuredValue);
            if (matcher.matches()) {
                String dateMath = matcher.group(1);
                configuredValue = WatcherDateTimeUtils.parseDateMath(dateMath, ZoneOffset.UTC, clock);
                resolvedValues.put(dateMath, WatcherDateTimeUtils.formatDate((ZonedDateTime) configuredValue));
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

    protected abstract Result doExecute(Map<String, Object> model, Map<String, Object> resolvedValues);

    @Override
    public String type() {
        return type;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }
}
