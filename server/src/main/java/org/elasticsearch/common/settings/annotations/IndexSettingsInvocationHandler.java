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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.sp.api.analysis.settings.BooleanSetting;
import org.elasticsearch.sp.api.analysis.settings.LongSetting;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class IndexSettingsInvocationHandler implements InvocationHandler {

    private static Logger LOGGER = LogManager.getLogger(IndexSettingsInvocationHandler.class);
    private String prefix = "";
    private IndexSettings settings;

    public IndexSettingsInvocationHandler(IndexSettings settings) {
        this.settings = settings;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        LOGGER.info("Invoked method: {}", method.getName());
        Annotation annotation = method.getAnnotations()[0];
        if (annotation instanceof LongSetting) {
            LongSetting setting = (LongSetting) annotation;
            Setting<Long> longSetting = Setting.longSetting(
                setting.path(),
                setting.defaultValue(),// this easily can be different than in IndexSettings.
                setting.max(),// should be min..
                Setting.Property.IndexScope
            );
            return settings.getValue(longSetting);

        } else if (annotation instanceof BooleanSetting) {
            BooleanSetting setting = (BooleanSetting) annotation;
            Setting<Boolean> booleanSetting = Setting.boolSetting(setting.path(), setting.defaultValue(), Setting.Property.IndexScope);
            return settings.getValue(booleanSetting);
        } else {
            throw new IllegalArgumentException();
        }
    }

}
