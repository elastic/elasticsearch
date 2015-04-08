/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public abstract class ThreadPoolSettingsBuilder<B extends ThreadPoolSettingsBuilder> {

    public static Same same(String name) {
        return new Same(name);
    }

    protected final String name;
    private final ImmutableSettings.Builder builder = ImmutableSettings.builder();

    protected ThreadPoolSettingsBuilder(String name, String type) {
        this.name = name;
        put("type", type);
    }

    public Settings build() {
        return builder.build();
    }

    protected B put(String setting, Object value) {
        builder.put("threadpool." + name + "." + setting, value);
        return (B) this;
    }

    protected B put(String setting, int value) {
        builder.put("threadpool." + name + "." + setting, value);
        return (B) this;
    }

    public static class Same extends ThreadPoolSettingsBuilder<Same> {
        public Same(String name) {
            super(name, "same");
        }
    }

    public static class Fixed extends ThreadPoolSettingsBuilder<Fixed> {

        public Fixed(String name) {
            super(name, "fixed");
        }

        public Fixed size(int size) {
            return put("size", size);
        }

        public Fixed queueSize(int queueSize) {
            return put("queue_size", queueSize);
        }
    }

}
