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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.sp.api.analysis.settings.BooleanSetting;
import org.elasticsearch.sp.api.analysis.settings.LongSetting;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClusterSettingsInvocationHandler implements InvocationHandler {

    private static Logger LOGGER = LogManager.getLogger(DynamicInvocationHandler.class);
    private final Class<?> settingsClass;
    private final ClusterService clusterService;

    private final ConcurrentMap<String, Object> settings = new ConcurrentHashMap<>();

    public ClusterSettingsInvocationHandler(Class<?> settingsClass, ClusterService clusterService) {
        this.settingsClass = settingsClass;
        this.clusterService = clusterService;
    }

    public void init() {
        for (Method method : settingsClass.getMethods()) {
            Annotation annotation = method.getAnnotations()[0];
            if (annotation instanceof LongSetting) {
                LongSetting setting = (LongSetting) annotation;
                Setting<Long> longSetting = Setting.longSetting(
                    setting.path(),
                    setting.defaultValue(),
                    0,
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic
                );
                clusterService.getClusterSettings().addSettingsUpdateConsumer(longSetting, (value) -> settings.put(setting.path(), value));

            } else if (annotation instanceof BooleanSetting) {
                BooleanSetting setting = (BooleanSetting) annotation;

                Setting<Boolean> booleanSetting = Setting.boolSetting(
                    setting.path(),
                    setting.defaultValue(),
                    Setting.Property.NodeScope,
                    Setting.Property.Dynamic
                );
                clusterService.getClusterSettings().addSettingsUpdateConsumer(booleanSetting, value -> settings.put(setting.path(), value));
            } else {
                throw new IllegalArgumentException();
            }

        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        LOGGER.info("Invoked method: {}", method.getName());
        // LongSetting annotation = method.getAnnotation(LongSetting.class);
        // assert method.getAnnotations().length == 1;
        Annotation annotation = method.getAnnotations()[0];
        if (annotation instanceof LongSetting) {
            LongSetting setting = (LongSetting) annotation;
            String path = setting.path();
            long defValue = setting.defaultValue();
            return settings.getOrDefault(path, defValue);

        } else if (annotation instanceof BooleanSetting) {
            BooleanSetting setting = (BooleanSetting) annotation;

            String path = setting.path();
            boolean defValue = setting.defaultValue();

            return settings.getOrDefault(path, defValue);
        } else {
            throw new IllegalArgumentException();
        }

    }
}
