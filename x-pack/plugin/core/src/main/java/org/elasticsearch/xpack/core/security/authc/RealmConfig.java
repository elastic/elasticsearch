/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

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
    private final Settings settings;
    private final ThreadContext threadContext;

    public RealmConfig(RealmIdentifier identifier, Settings settings, Environment env, ThreadContext threadContext) {
        this.identifier = identifier;
        this.settings = settings;
        this.env = env;
        this.enabled = getSetting(RealmSettings.ENABLED_SETTING);
        this.order = getSetting(RealmSettings.ORDER_SETTING);
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

    /**
     * @return The settings for the current node.
     * This will include the settings for this realm (as well as other realms, and other non-security settings).
     * @see #getConcreteSetting(Setting.AffixSetting)
     */
    public Settings settings() {
        return settings;
    }

    public Environment env() {
        return env;
    }

    public ThreadContext threadContext() {
        return threadContext;
    }

    /**
     * Return the {@link Setting.AffixSetting#getConcreteSettingForNamespace concrete setting}
     * that is produced by applying this realm's name as the namespace.
     * Realm configuration is defined using affix settings in the form {@code xpack.security.authc.realms.type.(name).key},
     * where
     * <ul>
     *     <li>{@code type} is a fixed string (known at compile time) that identifies the type of the realm being configured.</li>
     *     <li>{@code (name)} is a variable string (known only at runtime) that uniquely names the realm.</li>
     *     <li>{@code key} is a fixed string (known at compile time) that identifies a specific setting within the realm.</li>
     * </ul>
     * In order to extract an individual value from the runtime {@link Settings} object, it is necessary to convert an
     * {@link Setting.AffixSetting} object into a concrete {@link Setting} object that has a fixed key, for a specific name.
     */
    public <T> Setting<T> getConcreteSetting(Setting.AffixSetting<T> setting) {
        return setting.getConcreteSettingForNamespace(name());
    }

    /**
     * Return the {@link Setting.AffixSetting#getConcreteSettingForNamespace concrete setting} that is produced by applying this realm's
     * type as a parameter to the provided function, and the realm's name (as the namespace) to the resulting {@link Setting.AffixSetting}.
     * Because some settings (e.g. {@link RealmSettings#ORDER_SETTING "order"}) are defined for multiple "types", but the Settings
     * infrastructure treats the type as a fixed part of the setting key, it is common to define such multi-realm settings using a
     * {@link Function} of this form.
     * @see #getConcreteSetting(Setting.AffixSetting)
     */
    public <T> Setting<T> getConcreteSetting(Function<String, Setting.AffixSetting<T>> settingFactory) {
        return getConcreteSetting(settingFactory.apply(type()));
    }

    /**
     * Obtain the value of the provided {@code setting} from the node's {@link #settings global settings}.
     * The {@link Setting.AffixSetting} is made <em>concrete</em> through {@link #getConcreteSetting(Setting.AffixSetting)}, which is then
     * used to {@link Setting#get(Settings) retrieve} the setting value.
     */
    public <T> T getSetting(Setting.AffixSetting<T> setting) {
        return getConcreteSetting(setting).get(settings);
    }

    /**
     * Obtain the value of the provided {@code setting} from the node's {@link #settings global settings}.
     * {@link #getConcreteSetting(Function)} is used to obtain a <em>concrete setting</em> from the provided
     * {@link Function}/{@link Setting.AffixSetting}, and this <em>concrete setting</em> is then used to
     * {@link Setting#get(Settings) retrieve} the setting value.
     */
    public <T> T getSetting(Function<String, Setting.AffixSetting<T>> settingFactory) {
        return getSetting(settingFactory.apply(type()));
    }

    /**
     * Obtain the value of the provided {@code setting} from the node's {@link #settings global settings}.
     * {@link #getConcreteSetting(Function)} is used to obtain a <em>concrete setting</em> from the provided
     * {@link Function}/{@link Setting.AffixSetting}.
     * If this <em>concrete setting</em> {@link Setting#exists(Settings) exists} in the global settings, then its value is returned,
     * otherwise the {@code onElse} {@link Supplier} is executed and returned.
     */
    public <T> T getSetting(Function<String, Setting.AffixSetting<T>> settingFactory, Supplier<T> orElse) {
        return getSetting(settingFactory.apply(type()), orElse);
    }

    /**
     * Obtain the value of the provided {@code setting} from the node's {@link #settings global settings}.
     * {@link #getConcreteSetting(Setting.AffixSetting)} is used to obtain a <em>concrete setting</em> from the provided
     * {@link Setting.AffixSetting}.
     * If this <em>concrete setting</em> {@link Setting#exists(Settings) exists} in the global settings, then its value is returned,
     * otherwise the {@code onElse} {@link Supplier} is executed and returned.
     */
    public <T> T getSetting(Setting.AffixSetting<T> setting, Supplier<T> orElse) {
        final Setting<T> concrete = setting.getConcreteSettingForNamespace(name());
        if (concrete.exists(settings)) {
            return concrete.get(settings);
        } else {
            return orElse.get();
        }
    }

    /**
     * Determines whether the provided {@code setting} has an explicit value in the node's {@link #settings global settings}.
     * {@link #getConcreteSetting(Function)} is used to obtain a <em>concrete setting</em> from the provided
     * {@link Function}/{@link Setting.AffixSetting}, and this <em>concrete setting</em> is then used to
     * {@link Setting#exists(Settings) check} for a value.
     */
    public <T> boolean hasSetting(Function<String, Setting.AffixSetting<T>> settingFactory) {
        return getConcreteSetting(settingFactory).exists(settings);
    }

    /**
     * Determines whether the provided {@code setting} has an explicit value in the node's {@link #settings global settings}.
     * {@link #getConcreteSetting(Setting.AffixSetting)} is used to obtain a <em>concrete setting</em> from the provided
     * {@link Setting.AffixSetting}, and this <em>concrete setting</em> is then used to {@link Setting#exists(Settings) check} for a value.
     */
    public <T> boolean hasSetting(Setting.AffixSetting<T> setting) {
        return getConcreteSetting(setting).exists(settings);
    }

    /**
     * A realm identifier consists of a realm's {@link RealmConfig#type() type} and {@link RealmConfig#name() name}.
     * Because realms are configured using a key that contains both of these parts
     * (e.g. {@code xpack.security.authc.realms.native.native_realm.order}), it is often necessary to be able to
     * pass this pair of variables as a single type (e.g. in method parameters, or return values).
     */
    public static class RealmIdentifier {
        private final String type;
        private final String name;

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
            return type + '/' + name;
        }
    }
}
