/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.settings;

import org.elasticsearch.util.*;
import org.elasticsearch.util.concurrent.Immutable;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.io.Streams;
import org.elasticsearch.util.settings.loader.SettingsLoader;
import org.elasticsearch.util.settings.loader.SettingsLoaderFactory;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
@Immutable
public class ImmutableSettings implements Settings {

    private Map<String, String> settings;

    private Settings globalSettings;

    private transient ClassLoader classLoader;

    private ImmutableSettings(Map<String, String> settings, Settings globalSettings, ClassLoader classLoader) {
        this.settings = settings;
        this.globalSettings = globalSettings == null ? this : globalSettings;
        this.classLoader = classLoader == null ? buildClassLoader() : classLoader;
    }

    @Override public Settings getGlobalSettings() {
        return this.globalSettings;
    }

    @Override public ClassLoader getClassLoader() {
        return this.classLoader;
    }

    @Override public Map<String, String> getAsMap() {
        return Collections.unmodifiableMap(this.settings);
    }

    @Override public Settings getComponentSettings(Class component) {
        return getComponentSettings("org.elasticsearch", component);
    }

    @Override public Settings getComponentSettings(String prefix, Class component) {
        String type = component.getName();
        if (!type.startsWith(prefix)) {
            throw new SettingsException("Component [" + type + "] does not start with prefix [" + prefix + "]");
        }
        String settingPrefix = type.substring(prefix.length() + 1); // 1 for the '.'
        settingPrefix = settingPrefix.substring(0, settingPrefix.length() - component.getSimpleName().length() - 1); // remove the simple class name
        Builder builder = new Builder();
        for (Map.Entry<String, String> entry : getAsMap().entrySet()) {
            if (entry.getKey().startsWith(settingPrefix)) {
                if (entry.getKey().length() <= settingPrefix.length()) {
                    // ignore this one
                    continue;
                }
                builder.put(entry.getKey().substring(settingPrefix.length() + 1), entry.getValue());
            }
        }
        builder.globalSettings(this);
        builder.classLoader(classLoader);
        return builder.build();
    }

    @Override public String get(String setting) {
        return settings.get(setting);
    }

    @Override public String get(String setting, String defaultValue) {
        String retVal = settings.get(setting);
        return retVal == null ? defaultValue : retVal;
    }

    @Override public Float getAsFloat(String setting, Float defaultValue) {
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

    @Override public Double getAsDouble(String setting, Double defaultValue) {
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

    @Override public Integer getAsInt(String setting, Integer defaultValue) {
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

    @Override public Long getAsLong(String setting, Long defaultValue) {
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

    @Override public Boolean getAsBoolean(String setting, Boolean defaultValue) {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Boolean.valueOf(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException("Failed to parse boolean setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override public TimeValue getAsTime(String setting, TimeValue defaultValue) {
        return TimeValue.parseTimeValue(get(setting), defaultValue);
    }

    @Override public SizeValue getAsSize(String setting, SizeValue defaultValue) throws SettingsException {
        return SizeValue.parse(get(setting), defaultValue);
    }

    @SuppressWarnings({"unchecked"})
    @Override public <T> Class<? extends T> getAsClass(String setting, Class<? extends T> defaultClazz) throws SettingsException {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultClazz;
        }
        try {
            return (Class<? extends T>) getClassLoader().loadClass(sValue);
        } catch (ClassNotFoundException e) {
            throw new SettingsException("Failed to load class setting [" + setting + "] with value [" + sValue + "]", e);
        }
    }

    @Override public <T> Class<? extends T> getAsClass(String setting, Class<? extends T> defaultClazz, String prefixPackage, String suffixClassName) throws SettingsException {
        String sValue = get(setting);
        if (sValue == null) {
            return defaultClazz;
        }
        String fullClassName = sValue;
        try {
            return (Class<? extends T>) getClassLoader().loadClass(fullClassName);
        } catch (ClassNotFoundException e) {
            fullClassName = prefixPackage + Strings.capitalize(sValue) + suffixClassName;
            try {
                return (Class<? extends T>) getClassLoader().loadClass(fullClassName);
            } catch (ClassNotFoundException e1) {
                fullClassName = prefixPackage + sValue + "." + Strings.capitalize(sValue) + suffixClassName;
                try {
                    return (Class<? extends T>) getClassLoader().loadClass(fullClassName);
                } catch (ClassNotFoundException e2) {
                    throw new NoClassSettingsException("Failed to load class setting [" + setting + "] with value [" + sValue + "]", e);
                }
            }
        }
    }

    @Override public String[] getAsArray(String settingPrefix) throws SettingsException {
        List<String> result = newArrayList();
        int counter = 0;
        while (true) {
            String value = get(settingPrefix + '.' + (counter++));
            if (value == null) {
                break;
            }
            result.add(value);
        }
        return result.toArray(new String[result.size()]);
    }

    @Override public Map<String, Settings> getGroups(String settingPrefix) throws SettingsException {
        if (settingPrefix.charAt(settingPrefix.length() - 1) != '.') {
            settingPrefix = settingPrefix + ".";
        }
        // we don't really care that it might happen twice
        Map<String, Map<String, String>> map = new LinkedHashMap<String, Map<String, String>>();
        for (Object o : settings.keySet()) {
            String setting = (String) o;
            if (setting.startsWith(settingPrefix)) {
                String nameValue = setting.substring(settingPrefix.length());
                int dotIndex = nameValue.indexOf('.');
                if (dotIndex == -1) {
                    throw new SettingsException("Failed to get setting group for [" + settingPrefix + "] setting prefix and setting [" + setting + "] because of a missing '.'");
                }
                String name = nameValue.substring(0, dotIndex);
                String value = nameValue.substring(dotIndex + 1);
                Map<String, String> groupSettings = map.get(name);
                if (groupSettings == null) {
                    groupSettings = new LinkedHashMap<String, String>();
                    map.put(name, groupSettings);
                }
                groupSettings.put(value, get(setting));
            }
        }
        Map<String, Settings> retVal = new LinkedHashMap<String, Settings>();
        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
            retVal.put(entry.getKey(), new ImmutableSettings(Collections.unmodifiableMap(entry.getValue()), globalSettings, classLoader));
        }
        return Collections.unmodifiableMap(retVal);
    }

    private static ClassLoader buildClassLoader() {
        return Classes.getDefaultClassLoader();
    }

    public static Settings readSettingsFromStream(DataInput in) throws IOException {
        return readSettingsFromStream(in, null);
    }

    public static Settings readSettingsFromStream(DataInput in, Settings globalSettings) throws IOException {
        Builder builder = new Builder();
        int numberOfSettings = in.readInt();
        for (int i = 0; i < numberOfSettings; i++) {
            builder.put(in.readUTF(), in.readUTF());
        }
        builder.globalSettings(globalSettings);
        return builder.build();
    }

    public static void writeSettingsToStream(Settings settings, DataOutput out) throws IOException {
        out.writeInt(settings.getAsMap().size());
        for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    public static Builder settingsBuilder() {
        return new Builder();
    }

    public static class Builder implements Settings.Builder {

        public static final Settings EMPTY_SETTINGS = new Builder().build();

        private final Map<String, String> map = new LinkedHashMap<String, String>();

        private ClassLoader classLoader;

        private Settings globalSettings;

        public Builder() {

        }

        public String get(String key) {
            return map.get(key);
        }

        public Builder put(String key, String value) {
            map.put(key, value);
            return this;
        }

        public Builder putClass(String key, Class clazz) {
            map.put(key, clazz.getName());
            return this;
        }

        public Builder putBoolean(String setting, boolean value) {
            put(setting, String.valueOf(value));
            return this;
        }

        public Builder putInt(String setting, int value) {
            put(setting, String.valueOf(value));
            return this;
        }

        public Builder putLong(String setting, long value) {
            put(setting, String.valueOf(value));
            return this;
        }

        public Builder putFloat(String setting, float value) {
            put(setting, String.valueOf(value));
            return this;
        }

        public Builder putDouble(String setting, double value) {
            put(setting, String.valueOf(value));
            return this;
        }

        public Builder putTime(String setting, long value, TimeUnit timeUnit) {
            putLong(setting, timeUnit.toMillis(value));
            return this;
        }

        public Builder putSize(String setting, long value, SizeUnit sizeUnit) {
            putLong(setting, sizeUnit.toBytes(value));
            return this;
        }

        public Builder putGroup(String settingPrefix, String groupName, String[] settings, String[] values) throws SettingsException {
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

        public Builder putAll(Settings settings) {
            map.putAll(settings.getAsMap());
            return this;
        }

        public Builder putAll(Map<String, String> settings) {
            map.putAll(settings);
            return this;
        }

        public Builder putAll(Properties properties) {
            for (Map.Entry entry : properties.entrySet()) {
                map.put((String) entry.getKey(), (String) entry.getValue());
            }
            return this;
        }

        /**
         * Loads settings from the actual string content that represents them.
         */
        public Builder loadFromSource(String source) {
            SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromSource(source);
            try {
                Map<String, String> loadedSettings = settingsLoader.load(source);
                putAll(loadedSettings);
            } catch (IOException e) {
                throw new SettingsException("Failed to load settings from [" + source + "]");
            }
            return this;
        }

        public Builder loadFromUrl(URL url) throws SettingsException {
            try {
                return loadFromStream(url.toExternalForm(), url.openStream());
            } catch (IOException e) {
                throw new SettingsException("Failed to open stream for url [" + url.toExternalForm() + "]", e);
            }
        }

        public Builder loadFromStream(String resourceName, InputStream is) throws SettingsException {
            SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(resourceName);
            try {
                Map<String, String> loadedSettings = settingsLoader.load(Streams.copyToString(new InputStreamReader(is)));
                putAll(loadedSettings);
            } catch (IOException e) {
                throw new SettingsException("Failed to load settings from [" + resourceName + "]");
            }
            return this;
        }

        /**
         * Loads the resource name from the classpath, returning <code>true</code> if it
         * was loaded.
         */
        public Builder loadFromClasspath(String resourceName) throws SettingsException {
            ClassLoader classLoader = this.classLoader;
            if (classLoader == null) {
                classLoader = buildClassLoader();
            }
            InputStream is = classLoader.getResourceAsStream(resourceName);
            if (is == null) {
                return this;
            }

            return loadFromStream(resourceName, is);
        }

        public Builder classLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public Builder globalSettings(Settings globalSettings) {
            this.globalSettings = globalSettings;
            return this;
        }

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

        public Builder replacePropertyPlaceholders() {
            PropertyPlaceholder propertyPlaceholder = new PropertyPlaceholder("${", "}", false);
            PropertyPlaceholder.PlaceholderResolver placeholderResolver = new PropertyPlaceholder.PlaceholderResolver() {
                @Override public String resolvePlaceholder(String placeholderName) {
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
            };
            for (Map.Entry<String, String> entry : map.entrySet()) {
                map.put(entry.getKey(), propertyPlaceholder.replacePlaceholders(entry.getValue(), placeholderResolver));
            }
            return this;
        }

        public Settings build() {
            return new ImmutableSettings(
                    Collections.unmodifiableMap(map),
                    globalSettings, classLoader);
        }
    }
}
