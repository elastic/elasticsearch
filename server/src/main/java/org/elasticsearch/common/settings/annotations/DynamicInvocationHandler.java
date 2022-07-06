/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings.annotations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.sp.api.analysis.settings.BooleanSetting;
import org.elasticsearch.sp.api.analysis.settings.LongSetting;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.function.Function;

public class DynamicInvocationHandler implements InvocationHandler {

    private static Logger LOGGER = LogManager.getLogger(DynamicInvocationHandler.class);
    private String prefix = "";
    private Settings settings;

    public DynamicInvocationHandler(Settings settings) {
        this.settings = settings;
    }

    public DynamicInvocationHandler(String prefix, Settings settings) {
        this.prefix = prefix;
        this.settings = settings;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        LOGGER.info("Invoked method: {}", method.getName());
        // LongSetting annotation = method.getAnnotation(LongSetting.class);
        // assert method.getAnnotations().length == 1;
        Annotation annotation = method.getAnnotations()[0];
        if (annotation instanceof LongSetting) {
            LongSetting setting = (LongSetting) annotation;
            return getValue(Long::valueOf, setting.path(), setting.defaultValue(), setting.max());

        } else if (annotation instanceof BooleanSetting) {
            BooleanSetting setting = (BooleanSetting) annotation;
            return getValue(Boolean::valueOf, setting.path(), setting.defaultValue());
        } else {
            throw new IllegalArgumentException();
        }

    }

    private <T extends Comparable<T>> T getValue(Function<String, T> parser, String path, T defaultValue, T max) {
        T value = getValue(parser, path, defaultValue);
        if (value.compareTo(max) > 0) {
            throw new IllegalArgumentException("Setting value " + value + "is greater than max " + max);
        }
        return value;
    }

    private <T> T getValue(Function<String, T> parser, String path, T defaultValue) {
        String key = /*prefix + "." +*/ path;
        String value = settings.get(key, String.valueOf(defaultValue));
        return parser.apply(value);
    }

}
