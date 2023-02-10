/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis.wrappers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugin.settings.BooleanSetting;
import org.elasticsearch.plugin.settings.IntSetting;
import org.elasticsearch.plugin.settings.ListSetting;
import org.elasticsearch.plugin.settings.LongSetting;
import org.elasticsearch.plugin.settings.StringSetting;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.function.Function;

public class SettingsInvocationHandler implements InvocationHandler {

    private static Logger LOGGER = LogManager.getLogger(SettingsInvocationHandler.class);
    private Settings settings;
    private Environment environment;

    public SettingsInvocationHandler(Settings settings, Environment environment) {
        this.settings = settings;
        this.environment = environment;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> T create(Settings settings, Class<T> parameterType, Environment environment) {
        return (T) Proxy.newProxyInstance(
            parameterType.getClassLoader(),
            new Class[] { parameterType },
            new SettingsInvocationHandler(settings, environment)
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        assert method.getAnnotations().length == 1;
        Annotation annotation = method.getAnnotations()[0];

        if (annotation instanceof IntSetting setting) {
            return getValue(Integer::valueOf, setting.path(), setting.defaultValue());
        } else if (annotation instanceof LongSetting setting) {
            return getValue(Long::valueOf, setting.path(), setting.defaultValue());
        } else if (annotation instanceof BooleanSetting setting) {
            return getValue(Boolean::valueOf, setting.path(), setting.defaultValue());
        } else if (annotation instanceof StringSetting setting) {
            return getValue(String::valueOf, setting.path(), setting.defaultValue());
        } else if (annotation instanceof ListSetting setting) {
            return settings.getAsList(setting.path(), Collections.emptyList());
        } else {
            throw new IllegalArgumentException("Unrecognised annotation " + annotation);
        }
    }

    private <T> T getValue(Function<String, T> parser, String path, T defaultValue) {
        String key = path;
        if (settings.get(key) != null) {
            return parser.apply(settings.get(key));
        }
        return defaultValue;
    }

}
