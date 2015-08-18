/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Arrays;

public abstract class MarvelSetting<V> {

    private final String name;
    private final String description;
    private final boolean dynamic;

    private volatile V value;

    MarvelSetting(String name, String description, V defaultValue, boolean dynamic) {
        this.name = name;
        this.description = description;
        this.value = defaultValue;
        this.dynamic = dynamic;
    }

    abstract boolean onRefresh(Settings settings);

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public V getValue() {
        return value;
    }

    public synchronized void setValue(V value) {
        this.value = value;
    }

    public String getValueAsString() {
        return getValue() != null ? getValue().toString() : "null";
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public String dynamicSettingName() {
        return getName();
    }

    public Validator dynamicValidator() {
        return Validator.EMPTY;
    }

    @Override
    public String toString() {
        return "marvel setting [" + getName() + " : " + getValueAsString() + "]";
    }

    public static BooleanSetting booleanSetting(String name, Boolean defaultValue, String description, boolean dynamic) {
        return new BooleanSetting(name, description, defaultValue, dynamic);
    }

    public static StringSetting stringSetting(String name, String defaultValue, String description, boolean dynamic) {
        return new StringSetting(name, description, defaultValue, dynamic);
    }

    public static StringArraySetting arraySetting(String name, String[] defaultValue, String description, boolean dynamic) {
        return new StringArraySetting(name, description, defaultValue, dynamic);
    }

    public static TimeValueSetting timeSetting(String name, TimeValue defaultValue, String description, boolean dynamic) {
        return new TimeValueSetting(name, description, defaultValue, dynamic);
    }

    public static TimeoutValueSetting timeoutSetting(String name, TimeValue defaultTimeoutValue, String description, boolean dynamic) {
        return new TimeoutValueSetting(name, description, defaultTimeoutValue, dynamic);
    }

    static class BooleanSetting extends MarvelSetting<Boolean> {

        BooleanSetting(String name, String description, Boolean defaultValue, boolean dynamic) {
            super(name, description, defaultValue, dynamic);
        }

        @Override
        boolean onRefresh(Settings settings) {
            Boolean updated = settings.getAsBoolean(getName(), null);
            if ((updated != null) && !updated.equals(getValue())) {
                setValue(updated);
                return true;
            }
            return false;
        }

        @Override
        public Validator dynamicValidator() {
            return Validator.BOOLEAN;
        }
    }

    static class StringSetting extends MarvelSetting<String> {

        StringSetting(String name, String description, String defaultValue, boolean dynamic) {
            super(name, description, defaultValue, dynamic);
        }

        @Override
        boolean onRefresh(Settings settings) {
            String updated = settings.get(getName(), null);
            if ((updated != null) && !updated.equals(getValue())) {
                setValue(updated);
                return true;
            }
            return false;
        }
    }

    static class StringArraySetting extends MarvelSetting<String[]> {

        StringArraySetting(String name, String description, String[] defaultValue, boolean dynamic) {
            super(name, description, defaultValue, dynamic);
        }

        @Override
        boolean onRefresh(Settings settings) {
            String[] updated = settings.getAsArray(getName(), null);
            if ((updated != null) && (!Arrays.equals(updated, getValue()))) {
                setValue(updated);
                return true;
            }
            return false;
        }

        @Override
        public String dynamicSettingName() {
            // array settings
            return super.dynamicSettingName() + ".*";
        }

        @Override
        public String getValueAsString() {
            return Strings.arrayToCommaDelimitedString(getValue());
        }
    }

    static class TimeValueSetting extends MarvelSetting<TimeValue> {

        TimeValueSetting(String name, String description, TimeValue defaultValue, boolean dynamic) {
            super(name, description, defaultValue, dynamic);
        }

        @Override
        boolean onRefresh(Settings settings) {
            TimeValue updated = get(settings, null);
            if ((updated != null) && ((getValue() == null) || (updated.millis() != getValue().millis()))) {
                setValue(updated);
                return true;
            }
            return false;
        }

        private TimeValue get(Settings settings, TimeValue defaultValue) {
            try {
                TimeValue t = settings.getAsTime(getName(), defaultValue);
                if (t != null) {
                    return t;
                }
            } catch (ElasticsearchParseException e) {
                Long l = settings.getAsLong(getName(), defaultValue != null ? defaultValue.millis() : null);
                if (l != null) {
                    return TimeValue.timeValueMillis(l);
                }
            }
            return null;
        }

        @Override
        public Validator dynamicValidator() {
            return Validator.TIME;
        }
    }

    static class TimeoutValueSetting extends TimeValueSetting {

        TimeoutValueSetting(String name, String description, TimeValue defaultValue, boolean dynamic) {
            super(name, description, defaultValue, dynamic);
        }

        @Override
        public Validator dynamicValidator() {
            return Validator.TIMEOUT;
        }
    }
}
