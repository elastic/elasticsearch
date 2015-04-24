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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;

import java.util.Map;
import java.util.Set;

/**
 * Immutable settings allowing to control the configuration.
 * <p/>
 * <p>Using {@link ImmutableSettings#settingsBuilder()} in order to create a builder
 * which in turn can create an immutable implementation of settings.
 *
 * @see ImmutableSettings
 */
public interface Settings extends ToXContent {

    /**
     * A settings that are filtered (and key is removed) with the specified prefix.
     */
    Settings getByPrefix(String prefix);

    /**
     * Returns the settings mapped to the given setting name.
     */
    Settings getAsSettings(String setting);

    /**
     * The class loader associated with this settings, or {@link org.elasticsearch.common.Classes#getDefaultClassLoader()}
     * if not set.
     */
    ClassLoader getClassLoader();

    /**
     * The class loader associated with this settings, but only if explicitly set, otherwise <tt>null</tt>.
     */
    @Nullable
    ClassLoader getClassLoaderIfSet();

    /**
     * The settings as a flat {@link java.util.Map}.
     */
    ImmutableMap<String, String> getAsMap();

    /**
     * The settings as a structured {@link java.util.Map}.
     */
    Map<String, Object> getAsStructuredMap();

    /**
     * Returns the setting value associated with the setting key.
     *
     * @param setting The setting key
     * @return The setting value, <tt>null</tt> if it does not exists.
     */
    String get(String setting);

    /**
     * Returns the setting value associated with the first setting key.
     */
    String get(String[] settings);

    /**
     * Returns the setting value associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    String get(String setting, String defaultValue);

    /**
     * Returns the setting value associated with the first setting key, if none exists,
     * returns the default value provided.
     */
    String get(String[] settings, String defaultValue);

    /**
     * Returns group settings for the given setting prefix.
     */
    Map<String, Settings> getGroups(String settingPrefix) throws SettingsException;

    /**
     * Returns group settings for the given setting prefix.
     */
    Map<String, Settings> getGroups(String settingPrefix, boolean ignoreNonGrouped) throws SettingsException;

    /**
     * Returns the setting value (as float) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Float getAsFloat(String setting, Float defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as float) associated with teh first setting key, if none
     * exists, returns the default value provided.
     */
    Float getAsFloat(String[] settings, Float defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as double) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Double getAsDouble(String setting, Double defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as double) associated with teh first setting key, if none
     * exists, returns the default value provided.
     */
    Double getAsDouble(String[] settings, Double defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as int) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Integer getAsInt(String setting, Integer defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as int) associated with the first setting key. If it does not exists,
     * returns the default value provided.
     */
    Integer getAsInt(String[] settings, Integer defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as long) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Long getAsLong(String setting, Long defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as long) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Long getAsLong(String[] settings, Long defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as boolean) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Boolean getAsBoolean(String setting, Boolean defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as boolean) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    Boolean getAsBoolean(String[] settings, Boolean defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as time) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    TimeValue getAsTime(String setting, TimeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as time) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    TimeValue getAsTime(String[] settings, TimeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    ByteSizeValue getAsBytesSize(String setting, ByteSizeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    ByteSizeValue getAsBytesSize(String[] settings, ByteSizeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. Provided values can either be
     * absolute values (intepreted as a number of bytes), byte sizes (eg. 1mb) or percentage of the heap size
     * (eg. 12%). If it does not exists, parses the default value provided.
     */
    ByteSizeValue getAsMemory(String setting, String defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. Provided values can either be
     * absolute values (intepreted as a number of bytes), byte sizes (eg. 1mb) or percentage of the heap size
     * (eg. 12%). If it does not exists, parses the default value provided.
     */
    ByteSizeValue getAsMemory(String[] setting, String defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as a RatioValue) associated with the setting key. Provided values can
     * either be a percentage value (eg. 23%), or expressed as a floating point number (eg. 0.23). If
     * it does not exist, parses the default value provided.
     */
    RatioValue getAsRatio(String setting, String defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as a RatioValue) associated with the setting key. Provided values can
     * either be a percentage value (eg. 23%), or expressed as a floating point number (eg. 0.23). If
     * it does not exist, parses the default value provided.
     */
    RatioValue getAsRatio(String[] settings, String defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    SizeValue getAsSize(String setting, SizeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     */
    SizeValue getAsSize(String[] settings, SizeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as a class) associated with the setting key. If it does not exists,
     * returns the default class provided.
     *
     * @param setting      The setting key
     * @param defaultClazz The class to return if no value is associated with the setting
     * @param <T>          The type of the class
     * @return The class setting value, or the default class provided is no value exists
     * @throws NoClassSettingsException Failure to load a class
     */
    <T> Class<? extends T> getAsClass(String setting, Class<? extends T> defaultClazz) throws NoClassSettingsException;

    /**
     * Returns the setting value (as a class) associated with the setting key. If the value itself fails to
     * represent a loadable class, the value will be appended to the <tt>prefixPackage</tt> and suffixed with the
     * <tt>suffixClassName</tt> and it will try to be loaded with it.
     *
     * @param setting         The setting key
     * @param defaultClazz    The class to return if no value is associated with the setting
     * @param prefixPackage   The prefix package to prefix the value with if failing to load the class as is
     * @param suffixClassName The suffix class name to prefix the value with if failing to load the class as is
     * @param <T>             The type of the class
     * @return The class represented by the setting value, or the default class provided if no value exists
     * @throws NoClassSettingsException Failure to load the class
     */
    <T> Class<? extends T> getAsClass(String setting, Class<? extends T> defaultClazz, String prefixPackage, String suffixClassName) throws NoClassSettingsException;

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p/>
     * <p>It will also automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix  The setting prefix to load the array by
     * @param defaultArray   The default array to use if no value is specified
     * @param commaDelimited Whether to try to parse a string as a comma-delimited value
     * @return The setting array values
     * @throws SettingsException
     */
    String[] getAsArray(String settingPrefix, String[] defaultArray, Boolean commaDelimited) throws SettingsException;

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p/>
     * <p>If commaDelimited is true, it will automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix The setting prefix to load the array by
     * @return The setting array values
     * @throws SettingsException
     */
    String[] getAsArray(String settingPrefix, String[] defaultArray) throws SettingsException;

    /**
     * The values associated with a setting prefix as an array. The settings array is in the format of:
     * <tt>settingPrefix.[index]</tt>.
     * <p/>
     * <p>It will also automatically load a comma separated list under the settingPrefix and merge with
     * the numbered format.
     *
     * @param settingPrefix The setting prefix to load the array by
     * @return The setting array values
     * @throws SettingsException
     */
    String[] getAsArray(String settingPrefix) throws SettingsException;

    /**
     * Returns a parsed version.
     */
    Version getAsVersion(String setting, Version defaultVersion) throws SettingsException;

    /**
     * @return  The direct keys of this settings
     */
    Set<String> names();

    /**
     * Returns the settings as delimited string.
     */
    String toDelimitedString(char delimiter);

    /**
     * A settings builder interface.
     */
    interface Builder {

        /**
         * Builds the settings.
         */
        Settings build();
    }
}
