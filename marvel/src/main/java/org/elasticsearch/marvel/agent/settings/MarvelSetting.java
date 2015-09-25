/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Arrays;

public abstract class MarvelSetting<V> {

    private final String name;
    private final String description;

    private final V defaultValue;
    private volatile V value;

    MarvelSetting(String name, String description, V defaultValue) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.value = defaultValue;
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

    public V getDefaultValue() {
        return defaultValue;
    }

    public synchronized void setValue(V value) {
        this.value = value;
    }

    public String getValueAsString() {
        return getValue() != null ? getValue().toString() : "null";
    }

    @Override
    public String toString() {
        return "marvel setting [" + getName() + " : " + getValueAsString() + "]";
    }

    public static BooleanSetting booleanSetting(String name, Boolean defaultValue, String description) {
        return new BooleanSetting(name, description, defaultValue);
    }

    public static StringSetting stringSetting(String name, String defaultValue, String description) {
        return new StringSetting(name, description, defaultValue);
    }

    public static StringArraySetting arraySetting(String name, String[] defaultValue, String description) {
        return new StringArraySetting(name, description, defaultValue);
    }

    public static TimeValueSetting timeSetting(String name, TimeValue defaultValue, String description) {
        return new TimeValueSetting(name, description, defaultValue);
    }

    public static TimeoutValueSetting timeoutSetting(String name, TimeValue defaultTimeoutValue, String description) {
        return new TimeoutValueSetting(name, description, defaultTimeoutValue);
    }

    static class BooleanSetting extends MarvelSetting<Boolean> {

        BooleanSetting(String name, String description, Boolean defaultValue) {
            super(name, description, defaultValue);
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
    }

    static class StringSetting extends MarvelSetting<String> {

        StringSetting(String name, String description, String defaultValue) {
            super(name, description, defaultValue);
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

        StringArraySetting(String name, String description, String[] defaultValue) {
            super(name, description, defaultValue);
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
        public String getValueAsString() {
            return Strings.arrayToCommaDelimitedString(getValue());
        }
    }

    static class TimeValueSetting extends MarvelSetting<TimeValue> {

        TimeValueSetting(String name, String description, TimeValue defaultValue) {
            super(name, description, defaultValue);
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
    }

    static class TimeoutValueSetting extends TimeValueSetting {

        TimeoutValueSetting(String name, String description, TimeValue defaultValue) {
            super(name, description, defaultValue);
        }
    }
}
