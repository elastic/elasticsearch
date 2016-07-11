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

import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.settings.loader.SettingsLoaderFactory;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;
import static org.elasticsearch.common.unit.SizeValue.parseSizeValue;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

/**
 * An immutable settings implementation.
 */
public final class Settings implements ToXContent {

    public static final Settings EMPTY = new Builder().build();
    private static final Pattern ARRAY_PATTERN = Pattern.compile("(.*)\\.\\d+$");

    private SortedMap<String, String> settings;

    Settings(Map<String, String> settings) {
        // we use a sorted map for consistent serialization when using getAsMap()
        this.settings = Collections.unmodifiableSortedMap(new TreeMap<>(settings));
    }

    /**
     * The settings as a flat {@link java.util.Map}.
     * @return an unmodifiable map of settings
     */
    public Map<String, String> getAsMap() {
        return Collections.unmodifiableMap(this.settings);
    }

    /**
     * The settings as a structured {@link java.util.Map}.
     */
    public Map<String, Object> getAsStructuredMap() {
        Map<String, Object> map = new HashMap<>(2);
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
                Map<String, Object> newMap = new HashMap<>(2);
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
            ArrayList<Object> newValue = new ArrayList<>(maxIndex + 1);
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

    /**
     * A settings that are filtered (and key is removed) with the specified prefix.
     */
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
        return builder.build();
    }

    /**
     * Returns a new settings object that contains all setting of the current one filtered by the given settings key predicate.
     */
    public Settings filter(Predicate<String> predicate) {
        Builder builder = new Builder();
        for (Map.Entry<String, String> entry : getAsMap().entrySet()) {
            if (predicate.test(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        return builder.build();
    }

    /**
     * Returns the settings mapped to the given setting name.
     */
    public Settings getAsSettings(String setting) {
        return getByPrefix(setting + ".");
    }

    /**
     * Returns the setting value associated with the setting key.
     *
     * @param setting The setting key
     * @return The setting value, <tt>null</tt> if it does not exists.
     */
    public String get(String setting) {
        return settings.get(setting);
    }

    /**
     * Returns the setting value associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public String get(String setting, String defaultValue) {
        String retVal = get(setting);
        return retVal == null ? defaultValue : retVal;
    }

    /**
     * Returns the setting value (as float) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
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

    /**
     * Returns the setting value (as double) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
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

    /**
     * Returns the setting value (as int) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
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

    /**
     * Returns the setting value (as long) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
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

    /**
     * Returns the setting value (as boolean) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public Boolean getAsBoolean(String setting, Boolean defaultValue) {
        return Booleans.parseBoolean(get(setting), defaultValue);
    }

    /**
     * Returns the setting value (as time) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public TimeValue getAsTime(String setting, TimeValue defaultValue) {
        return parseTimeValue(get(setting), defaultValue, setting);
    }

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public ByteSizeValue getAsBytesSize(String setting, ByteSizeValue defaultValue) throws SettingsException {
        return parseBytesSizeValue(get(setting), defaultValue, setting);
    }

    /**
     * Returns the setting value (as size) associated with the setting key. Provided values can either be
     * absolute values (interpreted as a number of bytes), byte sizes (eg. 1mb) or percentage of the heap size
     * (eg. 12%). If it does not exists, parses the default value provided.
     */
    public ByteSizeValue getAsMemory(String setting, String defaultValue) throws SettingsException {
        return MemorySizeValue.parseBytesSizeValueOrHeapRatio(get(setting, defaultValue), setting);
    }

    /**
     * Returns the setting value (as a RatioValue) associated with the setting key. Provided values can
     * either be a percentage value (eg. 23%), or expressed as a floating point number (eg. 0.23). If
     * it does not exist, parses the default value provided.
     */
    public RatioValue getAsRatio(String setting, String defaultValue) throws SettingsException {
        return RatioValue.parseRatioValue(get(setting, defaultValue));
    }

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    public SizeValue getAsSize(String setting, SizeValue defaultValue) throws SettingsException {
        return parseSizeValue(get(setting), defaultValue);
    }

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p>
     * It will also automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix The setting prefix to load the array by
     * @return The setting array values
     */
    public String[] getAsArray(String settingPrefix) throws SettingsException {
        return getAsArray(settingPrefix, Strings.EMPTY_ARRAY, true);
    }

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p>
     * If commaDelimited is true, it will automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix The setting prefix to load the array by
     * @return The setting array values
     */
    public String[] getAsArray(String settingPrefix, String[] defaultArray) throws SettingsException {
        return getAsArray(settingPrefix, defaultArray, true);
    }

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p>
     * It will also automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix  The setting prefix to load the array by
     * @param defaultArray   The default array to use if no value is specified
     * @param commaDelimited Whether to try to parse a string as a comma-delimited value
     * @return The setting array values
     */
    public String[] getAsArray(String settingPrefix, String[] defaultArray, Boolean commaDelimited) throws SettingsException {
        List<String> result = new ArrayList<>();

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



    /**
     * Returns group settings for the given setting prefix.
     */
    public Map<String, Settings> getGroups(String settingPrefix) throws SettingsException {
        return getGroups(settingPrefix, false);
    }

    /**
     * Returns group settings for the given setting prefix.
     */
    public Map<String, Settings> getGroups(String settingPrefix, boolean ignoreNonGrouped) throws SettingsException {
        if (!Strings.hasLength(settingPrefix)) {
            throw new IllegalArgumentException("illegal setting prefix " + settingPrefix);
        }
        if (settingPrefix.charAt(settingPrefix.length() - 1) != '.') {
            settingPrefix = settingPrefix + ".";
        }
        return getGroupsInternal(settingPrefix, ignoreNonGrouped);
    }
    private Map<String, Settings> getGroupsInternal(String settingPrefix, boolean ignoreNonGrouped) throws SettingsException {
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
                    throw new SettingsException("Failed to get setting group for [" + settingPrefix + "] setting prefix and setting ["
                            + setting + "] because of a missing '.'");
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
            retVal.put(entry.getKey(), new Settings(Collections.unmodifiableMap(entry.getValue())));
        }
        return Collections.unmodifiableMap(retVal);
    }
    /**
     * Returns group settings for the given setting prefix.
     */
    public Map<String, Settings> getAsGroups() throws SettingsException {
        return getAsGroups(false);
    }

    public Map<String, Settings> getAsGroups(boolean ignoreNonGrouped) throws SettingsException {
        return getGroupsInternal("", ignoreNonGrouped);
    }

    /**
     * Returns a parsed version.
     */
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

    /**
     * @return  The direct keys of this settings
     */
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

    /**
     * Returns the settings as delimited string.
     */
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

        Settings that = (Settings) o;
        if (settings != null ? !settings.equals(that.settings) : that.settings != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = settings != null ? settings.hashCode() : 0;
        return result;
    }

    public static Settings readSettingsFromStream(StreamInput in) throws IOException {
        Builder builder = new Builder();
        int numberOfSettings = in.readVInt();
        for (int i = 0; i < numberOfSettings; i++) {
            builder.put(in.readString(), in.readOptionalString());
        }
        return builder.build();
    }

    public static void writeSettingsToStream(Settings settings, StreamOutput out) throws IOException {
        out.writeVInt(settings.getAsMap().size());
        for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            out.writeString(entry.getKey());
            out.writeOptionalString(entry.getValue());
        }
    }

    /**
     * Returns a builder to be used in order to build settings.
     */
    public static Builder builder() {
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
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        return builder;
    }

    /**
     * Returns <tt>true</tt> if this settings object contains no settings
     * @return <tt>true</tt> if this settings object contains no settings
     */
    public boolean isEmpty() {
        return this.settings.isEmpty();
    }

    /**
     * A builder allowing to put different settings and then {@link #build()} an immutable
     * settings implementation. Use {@link Settings#builder()} in order to
     * construct it.
     */
    public static class Builder {

        public static final Settings EMPTY_SETTINGS = new Builder().build();

        private final Map<String, String> map = new LinkedHashMap<>();

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
            return map.get(key);
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
                throw new IllegalArgumentException(
                        "array settings of key + value order doesn't hold correct number of arguments (" + settings.length + ")");
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

        public Builder putNull(String key) {
            return put(key, (String) null);
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
            put(setting, timeUnit.toMillis(value) + "ms");
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
            put(setting, sizeUnit.toBytes(value) + "b");
            return this;
        }

        /**
         * Sets the setting with the provided setting key and an array of values.
         *
         * @param setting The setting key
         * @param values  The values
         * @return The builder
         */

        /**
         * Sets the setting with the provided setting key and an array of values.
         *
         * @param setting The setting key
         * @param values  The values
         * @return The builder
         */
        public Builder putArray(String setting, String... values) {
            return putArray(setting, Arrays.asList(values));
        }

        /**
         * Sets the setting with the provided setting key and a list of values.
         *
         * @param setting The setting key
         * @param values  The values
         * @return The builder
         */
        public Builder putArray(String setting, List<String> values) {
            remove(setting);
            int counter = 0;
            while (true) {
                String value = map.remove(setting + '.' + (counter++));
                if (value == null) {
                    break;
                }
            }
            for (int i = 0; i < values.size(); i++) {
                put(setting + "." + i, values.get(i));
            }
            return this;
        }

        /**
         * Sets the setting as an array of values, but keeps existing elements for the key.
         */
        public Builder extendArray(String setting, String... values) {
            // check for a singular (non array) value
            String oldSingle = remove(setting);
            // find the highest array index
            int counter = 0;
            while (map.containsKey(setting + '.' + counter)) {
                ++counter;
            }
            if (oldSingle != null) {
                put(setting + '.' + counter++, oldSingle);
            }
            for (String value : values) {
                put(setting + '.' + counter++, value);
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
                } else if (map.keySet().stream().anyMatch(key -> key.startsWith(entry.getKey() + "."))) {
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
        public Builder put(Dictionary<Object,Object> properties) {
            for (Object key : Collections.list(properties.keys())) {
                map.put(Objects.toString(key), Objects.toString(properties.get(key)));
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
        public Builder loadFromPath(Path path) throws IOException {
            // NOTE: loadFromStream will close the input stream
            return loadFromStream(path.getFileName().toString(), Files.newInputStream(path));
        }

        /**
         * Loads settings from a stream that represents them using the
         * {@link SettingsLoaderFactory#loaderFromSource(String)}.
         */
        public Builder loadFromStream(String resourceName, InputStream is) throws IOException {
            SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(resourceName);
            // NOTE: copyToString will close the input stream
            Map<String, String> loadedSettings =
                settingsLoader.load(Streams.copyToString(new InputStreamReader(is, StandardCharsets.UTF_8)));
            put(loadedSettings);
            return this;
        }

        public Builder putProperties(Map<String, String> esSettings, Predicate<String> keyPredicate, Function<String, String> keyFunction) {
            for (final Map.Entry<String, String> esSetting : esSettings.entrySet()) {
                final String key = esSetting.getKey();
                if (keyPredicate.test(key)) {
                    map.put(keyFunction.apply(key), esSetting.getValue());
                }
            }
            return this;
        }

        /**
         * Runs across all the settings set on this builder and
         * replaces <tt>${...}</tt> elements in each setting with
         * another setting already set on this builder.
         */
        public Builder replacePropertyPlaceholders() {
            return replacePropertyPlaceholders(System::getenv);
        }

        // visible for testing
        Builder replacePropertyPlaceholders(Function<String, String> getenv) {
            PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
            PropertyPlaceholder.PlaceholderResolver placeholderResolver = new PropertyPlaceholder.PlaceholderResolver() {
                @Override
                public String resolvePlaceholder(String placeholderName) {
                    final String value = getenv.apply(placeholderName);
                    if (value != null) {
                        return value;
                    }
                    return map.get(placeholderName);
                }

                @Override
                public boolean shouldIgnoreMissing(String placeholderName) {
                    if (placeholderName.startsWith("prompt.")) {
                        return true;
                    }
                    return false;
                }

                @Override
                public boolean shouldRemoveMissingPlaceholder(String placeholderName) {
                    if (placeholderName.startsWith("prompt.")) {
                        return false;
                    }
                    return true;
                }
            };

            Iterator<Map.Entry<String, String>> entryItr = map.entrySet().iterator();
            while (entryItr.hasNext()) {
                Map.Entry<String, String> entry = entryItr.next();
                if (entry.getValue() == null) {
                    // a null value obviously can't be replaced
                    continue;
                }
                String value = propertyPlaceholder.replacePlaceholders(entry.getValue(), placeholderResolver);
                // if the values exists and has length, we should maintain it  in the map
                // otherwise, the replace process resolved into removing it
                if (Strings.hasLength(value)) {
                    entry.setValue(value);
                } else {
                    entryItr.remove();
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
            Map<String, String> replacements = new HashMap<>();
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
         * Builds a {@link Settings} (underlying uses {@link Settings}) based on everything
         * set on this builder.
         */
        public Settings build() {
            return new Settings(Collections.unmodifiableMap(map));
        }
    }
}
