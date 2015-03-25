/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Classes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.property.PropertyPlaceholder;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.settings.loader.SettingsLoaderFactory;
import org.elasticsearch.common.unit.*;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.Strings.toCamelCase;
import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;
import static org.elasticsearch.common.unit.SizeValue.parseSizeValue;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

/**
 * An immutable implementation of {@link Settings}.
 */
public class ImmutableSettings implements Settings {

    public static final Settings EMPTY = new Builder().build();
    private final static Pattern ARRAY_PATTERN = Pattern.compile("(.*)\\.\\d+$");

    private ImmutableMap<String, String> settings;
    private final ImmutableMap<String, String> forcedUnderscoreSettings;
    private transient ClassLoader classLoader;

    ImmutableSettings(Map<String, String> settings, ClassLoader classLoader) {
        this.settings = ImmutableMap.copyOf(settings);
        Map<String, String> forcedUnderscoreSettings = null;
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            String toUnderscoreCase = Strings.toUnderscoreCase(entry.getKey());
            if (!toUnderscoreCase.equals(entry.getKey())) {
                if (forcedUnderscoreSettings == null) {
                    forcedUnderscoreSettings = new HashMap<>();
                }
                forcedUnderscoreSettings.put(toUnderscoreCase, entry.getValue());
            }
        }
        this.forcedUnderscoreSettings = forcedUnderscoreSettings == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(forcedUnderscoreSettings);
        this.classLoader = classLoader;
    }

    @Override
    public ClassLoader getClassLoader() {
        return this.classLoader == null ? Classes.getDefaultClassLoader() : classLoader;
    }

    @Override
    public ClassLoader getClassLoaderIfSet() {
        return this.classLoader;
    }

    @Override
    public ImmutableMap<String, String> getAsMap() {
        return this.settings;
    }

    @Override
    public Map<String, Object> getAsStructuredMap() {
        Map<String, Object> map = Maps.newHashMapWithExpectedSize(2);
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            processSetting(map, "", entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
                entry.setValue(convertMapsToArrays(valMap));
            }
        }

        return map;
    }

    private void processSetting(Map<String, Object> map, String prefix, String setting, String value) {
        int prefixLength = setting.indexOf('.');
        if (prefixLength == -1) {
            @SuppressWarnings("unchecked") Map<String, Object> innerMap = (Map<String, Object>) map.get(prefix + setting);
            if (innerMap != null) {
                // It supposed to be a value, but we already have a map stored, need to convert this map to "." notation
                for (Map.Entry<String, Object> entry : innerMap.entrySet()) {
                    map.put(prefix + setting + "." + entry.getKey(), entry.getValue());
                }
            }
            map.put(prefix + setting, value);
        } else {
            String key = setting.substring(0, prefixLength);
            String rest = setting.substring(prefixLength + 1);
            Object existingValue = map.get(prefix + key);
            if (existingValue == null) {
                Map<String, Object> newMap = Maps.newHashMapWithExpectedSize(2);
                processSetting(newMap, "", rest, value);
                map.put(key, newMap);
            } else {
                if (existingValue instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> innerMap = (Map<String, Object>) existingValue;
                    processSetting(innerMap, "", rest, value);
                    map.put(key, innerMap);
                } else {
                    // It supposed to be a map, but we already have a value stored, which is not a map
                    // fall back to "." notation
                    processSetting(map, prefix + key + ".", rest, value);
                }
            }
        }
    }

    private Object convertMapsToArrays(Map<String, Object> map) {
        if (map.isEmpty()) {
            return map;
        }
        boolean isArray = true;
        int maxIndex = -1;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (isArray) {
                try {
                    int index = Integer.parseInt(entry.getKey());
                    if (index >= 0) {
                        maxIndex = Math.max(maxIndex, index);
                    } else {
                        isArray = false;
                    }
                } catch (NumberFormatException ex) {
                    isArray = false;
                }
            }
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
                entry.setValue(convertMapsToArrays(valMap));
            }
        }
        if (isArray && (maxIndex + 1) == map.size()) {
            ArrayList<Object> newValue = Lists.newArrayListWithExpectedSize(maxIndex + 1);
            for (int i = 0; i <= maxIndex; i++) {
                Object obj = map.get(Integer.toString(i));
                if (obj == null) {
                    // Something went wrong. Different format?
                    // Bailout!
                    return map;
                }
                newValue.add(obj);
            }
            return newValue;
        }
        return map;
    }

    @Override
    public Settings getByPrefix(String prefix) {
        Builder builder = new Builder();
        for (Map.Entry<String, String> entry : getAsMap().entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                if (entry.getKey().length() < prefix.length()) {
                    // ignore this. one
                    continue;
                }
                builder.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }
        builder.classLoader(classLoader);
        return builder.build();
    }

    @Override
    public Settings getAsSettings(String setting) {
        return getByPrefix(setting + ".");
    }

    @Override
    public String get(String setting) {
        String retVal = settings.get(setting);
        if (retVal != null) {
            return retVal;
        }
        return forcedUnderscoreSettings.get(setting);
    }

    @Override
    public String get(String[] settings) {
        for (String setting : settings) {
            String retVal = get(setting);
            if (retVal != null) {
                return retVal;
            }
        }
        return null;
    }

    @Override
    public String get(String setting, String defaultValue) {
        String retVal = get(setting);
        return retVal == null ? defaultValue : retVal;
    }

    @Override
    public String get(String[] settings, String defaultValue) {
        String retVal = get(settings);
        return retVal == null ? defaultValue : retVal;
    }

    @Override
    public Float getAsFloat(String setting, Float defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse float setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Float getAsFloat(String[] settings, Float defaultValue) throws SettingsException {
        String sValue = get(settings);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse float setting [" + Arrays.toString(settings) + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Double getAsDouble(String setting, Double defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse double setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Double getAsDouble(String[] settings, Double defaultValue) {
        String sValue = get(settings);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse double setting [" + Arrays.toString(settings) + "] with value [" + sValue + "]", e);
        }
    }


    @Override
    public Integer getAsInt(String setting, Integer defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse int setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Integer getAsInt(String[] settings, Integer defaultValue) {
        String sValue = get(settings);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse int setting [" + Arrays.toString(settings) + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Long getAsLong(String setting, Long defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse long setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Long getAsLong(String[] settings, Long defaultValue) {
        String sValue = get(settings);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse long setting [" + Arrays.toString(settings) + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Boolean getAsBoolean(String setting, Boolean defaultValue) {
        return Booleans.parseBoolean(get(setting), defaultValue);
    }

    @Override
    public Boolean getAsBoolean(String[] settings, Boolean defaultValue) {
        return Booleans.parseBoolean(get(settings), defaultValue);
    }

    @Override
    public TimeValue getAsTime(String setting, TimeValue defaultValue) {
        return parseTimeValue(get(setting), defaultValue);
    }

    @Override
    public TimeValue getAsTime(String[] settings, TimeValue defaultValue) {
        return parseTimeValue(get(settings), defaultValue);
    }

    @Override
    public ByteSizeValue getAsBytesSize(String setting, ByteSizeValue defaultValue) throws SettingsException {
        return parseBytesSizeValue(get(setting), defaultValue);
    }

    @Override
    public ByteSizeValue getAsBytesSize(String[] settings, ByteSizeValue defaultValue) throws SettingsException {
        return parseBytesSizeValue(get(settings), defaultValue);
    }

    @Override
    public ByteSizeValue getAsMemory(String setting, String defaultValue) throws SettingsException {
        return MemorySizeValue.parseBytesSizeValueOrHeapRatio(get(setting, defaultValue));
    }

    @Override
    public ByteSizeValue getAsMemory(String[] settings, String defaultValue) throws SettingsException {
        return MemorySizeValue.parseBytesSizeValueOrHeapRatio(get(settings, defaultValue));
    }

    @Override
    public RatioValue getAsRatio(String setting, String defaultValue) throws SettingsException {
        return RatioValue.parseRatioValue(get(setting, defaultValue));
    }

    @Override
    public RatioValue getAsRatio(String[] settings, String defaultValue) throws SettingsException {
        return RatioValue.parseRatioValue(get(settings, defaultValue));
    }

    @Override
    public SizeValue getAsSize(String setting, SizeValue defaultValue) throws SettingsException {
        return parseSizeValue(get(setting), defaultValue);
    }

    @Override
    public SizeValue getAsSize(String[] settings, SizeValue defaultValue) throws SettingsException {
        return parseSizeValue(get(settings), defaultValue);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> Class<? extends T> getAsClass(String setting, Class<? extends T> defaultClazz) throws NoClassSettingsException {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultClazz;
        }
        try {
            return (Class<? extends T>) getClassLoader().loadClass(sValue);
        } catch (ClassNotFoundException e) {
            throw new NoClassSettingsException("Failed to load class setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> Class<? extends T> getAsClass(String setting, Class<? extends T> defaultClazz, String prefixPackage, String suffixClassName) throws NoClassSettingsException {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultClazz;
        }
        String fullClassName = sValue;
        try {
            return (Class<? extends T>) getClassLoader().loadClass(fullClassName);
        } catch (ClassNotFoundException e) {
            String prefixValue = prefixPackage;
            int packageSeparator = sValue.lastIndexOf('.');
            if (packageSeparator > 0) {
                prefixValue = sValue.substring(0, packageSeparator + 1);
                sValue = sValue.substring(packageSeparator + 1);
            }
            fullClassName = prefixValue + Strings.capitalize(toCamelCase(sValue)) + suffixClassName;
            try {
                return (Class<? extends T>) getClassLoader().loadClass(fullClassName);
            } catch (ClassNotFoundException e1) {
                return loadClass(prefixValue, sValue, suffixClassName, setting);
            } catch (NoClassDefFoundError e1) {
                return loadClass(prefixValue, sValue, suffixClassName, setting);
            }
        }
    }

    private <T> Class<? extends T> loadClass(String prefixValue, String sValue, String suffixClassName, String setting) {
        String fullClassName = prefixValue + toCamelCase(sValue).toLowerCase(Locale.ROOT) + "." + Strings.capitalize(toCamelCase(sValue)) + suffixClassName;
        try {
            return (Class<? extends T>) getClassLoader().loadClass(fullClassName);
        } catch (ClassNotFoundException e2) {
            throw new NoClassSettingsException("Failed to load class setting [" + setting + "] with value [" + get(setting) + "]", e2);
        }
    }

    @Override
    public String[] getAsArray(String settingPrefix) throws SettingsException {
        return getAsArray(settingPrefix, Strings.EMPTY_ARRAY, true);
    }

    @Override
    public String[] getAsArray(String settingPrefix, String[] defaultArray) throws SettingsException {
        return getAsArray(settingPrefix, defaultArray, true);
    }

    @Override
    public String[] getAsArray(String settingPrefix, String[] defaultArray, Boolean commaDelimited) throws SettingsException {
        List<String> result = Lists.newArrayList();

        if (get(settingPrefix) != null) {
            if (commaDelimited) {
                String[] strings = Strings.splitStringByCommaToArray(get(settingPrefix));
                if (strings.length > 0) {
                    for (String string : strings) {
                        result.add(string.trim());
                    }
                }
            } else {
                result.add(get(settingPrefix).trim());
            }
        }

        int counter = 0;
        while (true) {
            String value = get(settingPrefix + '.' + (counter++));
            if (value == null) {
                break;
            }
            result.add(value.trim());
        }
        if (result.isEmpty()) {
            return defaultArray;
        }
        return result.toArray(new String[result.size()]);
    }

    @Override
    public Map<String, Settings> getGroups(String settingPrefix) throws SettingsException {
        return getGroups(settingPrefix, false);
    }

    @Override
    public Map<String, Settings> getGroups(String settingPrefix, boolean ignoreNonGrouped) throws SettingsException {
        if (!Strings.hasLength(settingPrefix)) {
            throw new ElasticsearchIllegalArgumentException("illegal setting prefix " + settingPrefix);
        }
        if (settingPrefix.charAt(settingPrefix.length() - 1) != '.') {
            settingPrefix = settingPrefix + ".";
        }
        // we don't really care that it might happen twice
        Map<String, Map<String, String>> map = new LinkedHashMap<>();
        for (Object o : settings.keySet()) {
            String setting = (String) o;
            if (setting.startsWith(settingPrefix)) {
                String nameValue = setting.substring(settingPrefix.length());
                int dotIndex = nameValue.indexOf('.');
                if (dotIndex == -1) {
                    if (ignoreNonGrouped) {
                        continue;
                    }
                    throw new SettingsException("Failed to get setting group for [" + settingPrefix + "] setting prefix and setting [" + setting + "] because of a missing '.'");
                }
                String name = nameValue.substring(0, dotIndex);
                String value = nameValue.substring(dotIndex + 1);
                Map<String, String> groupSettings = map.get(name);
                if (groupSettings == null) {
                    groupSettings = new LinkedHashMap<>();
                    map.put(name, groupSettings);
                }
                groupSettings.put(value, get(setting));
            }
        }
        Map<String, Settings> retVal = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
            retVal.put(entry.getKey(), new ImmutableSettings(Collections.unmodifiableMap(entry.getValue()), classLoader));
        }
        return Collections.unmodifiableMap(retVal);
    }

    @Override
    public Version getAsVersion(String setting, Version defaultVersion) throws SettingsException {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultVersion;
        }
        try {
            return Version.fromId(Integer.parseInt(sValue));
        } catch (Exception e) {
            throw new SettingsException("Failed to parse version setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public Set<String> names() {
        Set<String> names = new HashSet<>();
        for (String key : settings.keySet()) {
            int i = key.indexOf(".");
            if (i < 0) {
                names.add(key);
            } else {
                names.add(key.substring(0, i));
            }
        }
        return names;
    }

    @Override
    public String toDelimitedString(char delimiter) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(delimiter);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImmutableSettings that = (ImmutableSettings) o;

        if (classLoader != null ? !classLoader.equals(that.classLoader) : that.classLoader != null) return false;
        if (settings != null ? !settings.equals(that.settings) : that.settings != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = settings != null ? settings.hashCode() : 0;
        result = 31 * result + (classLoader != null ? classLoader.hashCode() : 0);
        return result;
    }

    public static Settings readSettingsFromStream(StreamInput in) throws IOException {
        Builder builder = new Builder();
        int numberOfSettings = in.readVInt();
        for (int i = 0; i < numberOfSettings; i++) {
            builder.put(in.readString(), in.readString());
        }
        return builder.build();
    }

    public static void writeSettingsToStream(Settings settings, StreamOutput out) throws IOException {
        out.writeVInt(settings.getAsMap().size());
        for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a builder to be used in order to build settings.
     */
    public static Builder settingsBuilder() {
        return new Builder();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Settings settings = SettingsFilter.filterSettings(params, this);
        if (!params.paramAsBoolean("flat_settings", false)) {
            for (Map.Entry<String, Object> entry : settings.getAsStructuredMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        } else {
            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), XContentBuilder.FieldCaseConversion.NONE);
            }
        }
        return builder;
    }

    /**
     * A builder allowing to put different settings and then {@link #build()} an immutable
     * settings implementation. Use {@link ImmutableSettings#settingsBuilder()} in order to
     * construct it.
     */
    public static class Builder implements Settings.Builder {

        public static final Settings EMPTY_SETTINGS = new Builder().build();

        private final Map<String, String> map = new LinkedHashMap<>();

        private ClassLoader classLoader;

        private Builder() {

        }

        public Map<String, String> internalMap() {
            return this.map;
        }

        /**
         * Removes the provided setting from the internal map holding the current list of settings.
         */
        public String remove(String key) {
            return map.remove(key);
        }

        /**
         * Returns a setting value based on the setting key.
         */
        public String get(String key) {
            String retVal = map.get(key);
            if (retVal != null) {
                return retVal;
            }
            // try camel case version
            return map.get(toCamelCase(key));
        }

        /**
         * Puts tuples of key value pairs of settings. Simplified version instead of repeating calling
         * put for each one.
         */
        public Builder put(Object... settings) {
            if (settings.length == 1) {
                // support cases where the actual type gets lost down the road...
                if (settings[0] instanceof Map) {
                    //noinspection unchecked
                    return put((Map) settings[0]);
                } else if (settings[0] instanceof Settings) {
                    return put((Settings) settings[0]);
                }
            }
            if ((settings.length % 2) != 0) {
                throw new ElasticsearchIllegalArgumentException("array settings of key + value order doesn't hold correct number of arguments (" + settings.length + ")");
            }
            for (int i = 0; i < settings.length; i++) {
                put(settings[i++].toString(), settings[i].toString());
            }
            return this;
        }

        /**
         * Sets a setting with the provided setting key and value.
         *
         * @param key   The setting key
         * @param value The setting value
         * @return The builder
         */
        public Builder put(String key, String value) {
            map.put(key, value);
            return this;
        }

        /**
         * Sets a setting with the provided setting key and class as value.
         *
         * @param key   The setting key
         * @param clazz The setting class value
         * @return The builder
         */
        public Builder put(String key, Class clazz) {
            map.put(key, clazz.getName());
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the boolean value.
         *
         * @param setting The setting key
         * @param value   The boolean value
         * @return The builder
         */
        public Builder put(String setting, boolean value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the int value.
         *
         * @param setting The setting key
         * @param value   The int value
         * @return The builder
         */
        public Builder put(String setting, int value) {
            put(setting, String.valueOf(value));
            return this;
        }

        public Builder put(String setting, Version version) {
            put(setting, version.id);
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the long value.
         *
         * @param setting The setting key
         * @param value   The long value
         * @return The builder
         */
        public Builder put(String setting, long value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the float value.
         *
         * @param setting The setting key
         * @param value   The float value
         * @return The builder
         */
        public Builder put(String setting, float value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the double value.
         *
         * @param setting The setting key
         * @param value   The double value
         * @return The builder
         */
        public Builder put(String setting, double value) {
            put(setting, String.valueOf(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the time value.
         *
         * @param setting The setting key
         * @param value   The time value
         * @return The builder
         */
        public Builder put(String setting, long value, TimeUnit timeUnit) {
            put(setting, timeUnit.toMillis(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and the size value.
         *
         * @param setting The setting key
         * @param value   The size value
         * @return The builder
         */
        public Builder put(String setting, long value, ByteSizeUnit sizeUnit) {
            put(setting, sizeUnit.toBytes(value));
            return this;
        }

        /**
         * Sets the setting with the provided setting key and an array of values.
         *
         * @param setting The setting key
         * @param values  The values
         * @return The builder
         */
        public Builder putArray(String setting, String... values) {
            remove(setting);
            int counter = 0;
            while (true) {
                String value = map.remove(setting + '.' + (counter++));
                if (value == null) {
                    break;
                }
            }
            for (int i = 0; i < values.length; i++) {
                put(setting + "." + i, values[i]);
            }
            return this;
        }

        /**
         * Sets the setting group.
         */
        public Builder put(String settingPrefix, String groupName, String[] settings, String[] values) throws SettingsException {
            if (settings.length != values.length) {
                throw new SettingsException("The settings length must match the value length");
            }
            for (int i = 0; i < settings.length; i++) {
                if (values[i] == null) {
                    continue;
                }
                put(settingPrefix + "." + groupName + "." + settings[i], values[i]);
            }
            return this;
        }

        /**
         * Sets all the provided settings.
         */
        public Builder put(Settings settings) {
            removeNonArraysFieldsIfNewSettingsContainsFieldAsArray(settings.getAsMap());
            map.putAll(settings.getAsMap());
            classLoader = settings.getClassLoaderIfSet();
            return this;
        }

        /**
         * Sets all the provided settings.
         */
        public Builder put(Map<String, String> settings) {
            removeNonArraysFieldsIfNewSettingsContainsFieldAsArray(settings);
            map.putAll(settings);
            return this;
        }

        /**
         * Removes non array values from the existing map, if settings contains an array value instead
         *
         * Example:
         *   Existing map contains: {key:value}
         *   New map contains: {key:[value1,value2]} (which has been flattened to {}key.0:value1,key.1:value2})
         *
         *   This ensure that that the 'key' field gets removed from the map in order to override all the
         *   data instead of merging
         */
        private void removeNonArraysFieldsIfNewSettingsContainsFieldAsArray(Map<String, String> settings) {
            List<String> prefixesToRemove = new ArrayList<>();
            for (final Map.Entry<String, String> entry : settings.entrySet()) {
                final Matcher matcher = ARRAY_PATTERN.matcher(entry.getKey());
                if (matcher.matches()) {
                    prefixesToRemove.add(matcher.group(1));
                } else if (Iterables.any(map.keySet(), startsWith(entry.getKey() + "."))) {
                    prefixesToRemove.add(entry.getKey());
                }
            }
            for (String prefix : prefixesToRemove) {
                Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    if (entry.getKey().startsWith(prefix + ".") || entry.getKey().equals(prefix)) {
                        iterator.remove();
                    }
                }
            }
        }

        /**
         * Sets all the provided settings.
         */
        public Builder put(Properties properties) {
            for (Map.Entry entry : properties.entrySet()) {
                map.put((String) entry.getKey(), (String) entry.getValue());
            }
            return this;
        }

        public Builder loadFromDelimitedString(String value, char delimiter) {
            String[] values = Strings.splitStringToArray(value, delimiter);
            for (String s : values) {
                int index = s.indexOf('=');
                if (index == -1) {
                    throw new ElasticsearchIllegalArgumentException("value [" + s + "] for settings loaded with delimiter [" + delimiter + "] is malformed, missing =");
                }
                map.put(s.substring(0, index), s.substring(index + 1));
            }
            return this;
        }

        /**
         * Loads settings from the actual string content that represents them using the
         * {@link SettingsLoaderFactory#loaderFromSource(String)}.
         */
        public Builder loadFromSource(String source) {
            SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromSource(source);
            try {
                Map<String, String> loadedSettings = settingsLoader.load(source);
                put(loadedSettings);
            } catch (Exception e) {
                throw new SettingsException("Failed to load settings from [" + source + "]", e);
            }
            return this;
        }

        /**
         * Loads settings from a url that represents them using the
         * {@link SettingsLoaderFactory#loaderFromSource(String)}.
         */
        public Builder loadFromUrl(URL url) throws SettingsException {
            try {
                return loadFromStream(url.toExternalForm(), url.openStream());
            } catch (IOException e) {
                throw new SettingsException("Failed to open stream for url [" + url.toExternalForm() + "]", e);
            }
        }

        /**
         * Loads settings from a url that represents them using the
         * {@link SettingsLoaderFactory#loaderFromSource(String)}.
         */
        public Builder loadFromPath(Path path) throws SettingsException {
            try {
                return loadFromStream(path.getFileName().toString(), Files.newInputStream(path));
            } catch (IOException e) {
                throw new SettingsException("Failed to open stream for url [" + path + "]", e);
            }
        }

        /**
         * Loads settings from a stream that represents them using the
         * {@link SettingsLoaderFactory#loaderFromSource(String)}.
         */
        public Builder loadFromStream(String resourceName, InputStream is) throws SettingsException {
            SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(resourceName);
            try {
                Map<String, String> loadedSettings = settingsLoader.load(Streams.copyToString(new InputStreamReader(is, Charsets.UTF_8)));
                put(loadedSettings);
            } catch (Exception e) {
                throw new SettingsException("Failed to load settings from [" + resourceName + "]", e);
            }
            return this;
        }

        /**
         * Loads settings from classpath that represents them using the
         * {@link SettingsLoaderFactory#loaderFromSource(String)}.
         */
        public Builder loadFromClasspath(String resourceName) throws SettingsException {
            ClassLoader classLoader = this.classLoader;
            if (classLoader == null) {
                classLoader = Classes.getDefaultClassLoader();
            }
            InputStream is = classLoader.getResourceAsStream(resourceName);
            if (is == null) {
                return this;
            }

            return loadFromStream(resourceName, is);
        }

        /**
         * Sets the class loader associated with the settings built.
         */
        public Builder classLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        /**
         * Puts all the properties with keys starting with the provided <tt>prefix</tt>.
         *
         * @param prefix     The prefix to filter property key by
         * @param properties The properties to put
         * @return The builder
         */
        public Builder putProperties(String prefix, Properties properties) {
            for (Object key1 : properties.keySet()) {
                String key = (String) key1;
                String value = properties.getProperty(key);
                if (key.startsWith(prefix)) {
                    map.put(key.substring(prefix.length()), value);
                }
            }
            return this;
        }

        /**
         * Puts all the properties with keys starting with the provided <tt>prefix</tt>.
         *
         * @param prefix     The prefix to filter property key by
         * @param properties The properties to put
         * @return The builder
         */
        public Builder putProperties(String prefix, Properties properties, String[] ignorePrefixes) {
            for (Object key1 : properties.keySet()) {
                String key = (String) key1;
                String value = properties.getProperty(key);
                if (key.startsWith(prefix)) {
                    boolean ignore = false;
                    for (String ignorePrefix : ignorePrefixes) {
                        if (key.startsWith(ignorePrefix)) {
                            ignore = true;
                            break;
                        }
                    }
                    if (!ignore) {
                        map.put(key.substring(prefix.length()), value);
                    }
                }
            }
            return this;
        }

        /**
         * Runs across all the settings set on this builder and replaces <tt>${...}</tt> elements in the
         * each setting value according to the following logic:
         * <p/>
         * <p>First, tries to resolve it against a System property ({@link System#getProperty(String)}), next,
         * tries and resolve it against an environment variable ({@link System#getenv(String)}), and last, tries
         * and replace it with another setting already set on this builder.
         */
        public Builder replacePropertyPlaceholders() {
            PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
            PropertyPlaceholder.PlaceholderResolver placeholderResolver = new PropertyPlaceholder.PlaceholderResolver() {
                @Override
                public String resolvePlaceholder(String placeholderName) {
                    if (placeholderName.startsWith("env.")) {
                        // explicit env var prefix
                        return System.getenv(placeholderName.substring("env.".length()));
                    }
                    String value = System.getProperty(placeholderName);
                    if (value != null) {
                        return value;
                    }
                    value = System.getenv(placeholderName);
                    if (value != null) {
                        return value;
                    }
                    return map.get(placeholderName);
                }

                @Override
                public boolean shouldIgnoreMissing(String placeholderName) {
                    // if its an explicit env var, we are ok with not having a value for it and treat it as optional
                    if (placeholderName.startsWith("env.")) {
                        return true;
                    }
                    return false;
                }
            };
            for (Map.Entry<String, String> entry : Maps.newHashMap(map).entrySet()) {
                String value = propertyPlaceholder.replacePlaceholders(entry.getValue(), placeholderResolver);
                // if the values exists and has length, we should maintain it  in the map
                // otherwise, the replace process resolved into removing it
                if (Strings.hasLength(value)) {
                    map.put(entry.getKey(), value);
                } else {
                    map.remove(entry.getKey());
                }
            }
            return this;
        }

        /**
         * Checks that all settings in the builder start with the specified prefix.
         *
         * If a setting doesn't start with the prefix, the builder appends the prefix to such setting.
         */
        public Builder normalizePrefix(String prefix) {
            Map<String, String> replacements = Maps.newHashMap();
            Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                if (entry.getKey().startsWith(prefix) == false) {
                    replacements.put(prefix + entry.getKey(), entry.getValue());
                    iterator.remove();
                }
            }
            map.putAll(replacements);
            return this;
        }

        /**
         * Builds a {@link Settings} (underlying uses {@link ImmutableSettings}) based on everything
         * set on this builder.
         */
        @Override
        public Settings build() {
            return new ImmutableSettings(Collections.unmodifiableMap(map), classLoader);
        }
    }

    private static StartsWithPredicate startsWith(String prefix) {
        return new StartsWithPredicate(prefix);
    }

    private static final class StartsWithPredicate implements Predicate<String> {

        private String prefix;

        public StartsWithPredicate(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public boolean apply(String input) {
            return input.startsWith(prefix);
        }
    }
}
