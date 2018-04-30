/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class RealmConfig {

    final RealmIdentifier identifier;
    final boolean enabled;
    final int order;
    private final Environment env;
    private final Settings globalSettings;
    private final ThreadContext threadContext;

    @Deprecated
    public RealmConfig(RealmIdentifier identifier, Settings settings, Settings globalSettings, Environment env,
                       ThreadContext threadContext) {
        this(identifier, globalSettings, env, threadContext);
    }

    public RealmConfig(RealmIdentifier identifier, Settings globalSettings, Environment env,
                       ThreadContext threadContext) {
        this.identifier = identifier;
        this.globalSettings = globalSettings;
        this.env = env;
        enabled = getSetting(RealmSettings.ENABLED_SETTING);
        order = getSetting(RealmSettings.ORDER_SETTING);
        this.threadContext = threadContext;
    }

    public RealmIdentifier identifier() {
        return identifier;
    }

    public String name() {
        return identifier.name;
    }

    public boolean enabled() {
        return enabled;
    }

    public int order() {
        return order;
    }

    public String type() {
        return identifier.type;
    }

    public Settings globalSettings() {
        return globalSettings;
    }

    public Logger logger(Class clazz) {
        return Loggers.getLogger(clazz, globalSettings);
    }

    public Environment env() {
        return env;
    }

    public ThreadContext threadContext() {
        return threadContext;
    }

    public <T> Setting<T> getConcreteSetting(Setting.AffixSetting<T> setting) {
        return setting.getConcreteSettingForNamespace(name());
    }

    public <T> Setting<T> getConcreteSetting(Function<String, Setting.AffixSetting<T>> settingFactory) {
        return getConcreteSetting(settingFactory.apply(type()));
    }

    public <T> T getSetting(Setting.AffixSetting<T> setting) {
        return getConcreteSetting(setting).get(globalSettings);
    }

    public <T> T getSetting(Function<String, Setting.AffixSetting<T>> settingFactory) {
        return getSetting(settingFactory.apply(type()));
    }

    public <T> T getSetting(Function<String, Setting.AffixSetting<T>> settingFactory, Supplier<T> orElse) {
        return getSetting(settingFactory.apply(type()), orElse);
    }

    public <T> T getSetting(Setting.AffixSetting<T> setting, Supplier<T> orElse) {
        final Setting<T> concrete = setting.getConcreteSettingForNamespace(name());
        if (concrete.exists(globalSettings)) {
            return concrete.get(globalSettings);
        } else {
            return orElse.get();
        }
    }

    public <T> boolean hasSetting(Function<String, Setting.AffixSetting<T>> settingFactory) {
        return getConcreteSetting(settingFactory).exists(globalSettings);
    }

    public <T> boolean hasSetting(Setting.AffixSetting<T> setting) {
        return getConcreteSetting(setting).exists(globalSettings);
    }

    public Settings getSettingsByPrefix(String prefix) {
        final String sslPrefix = "xpack.security.authc.realms." + identifier.type + "." + identifier.name + "." + prefix;
        return globalSettings().getByPrefix(sslPrefix);
    }

    public static class RealmIdentifier {
        final String type;
        final String name;

        public RealmIdentifier(String type, String name) {
            this.type = Objects.requireNonNull(type, "Realm type cannot be null");
            this.name = Objects.requireNonNull(name, "Realm name cannot be null");
        }

        public String getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (getClass() != o.getClass()) {
                return false;
            }
            final RealmIdentifier other = (RealmIdentifier) o;
            return Objects.equals(this.type, other.type) &&
                    Objects.equals(this.name, other.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, name);
        }

        @Override
        public String toString() {
            return type + '.' + name;
        }
    }
}
