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

import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.concurrent.ThreadSafe;

import java.util.Map;

/**
 * Immutable settings allowing to control the configuration.
 *
 * <p>Using {@link ImmutableSettings#settingsBuilder()} in order to create a builder
 * which in turn can create an immutable implementation of settings.
 *
 * @author kimchy (Shay Banon)
 * @see ImmutableSettings
 */
@ThreadSafe
public interface Settings {

    /**
     * The global settings if these settings are group settings.
     */
    Settings getGlobalSettings();

    /**
     * Component settings for a specific component. Returns all the settings for the given class, where the
     * FQN of the class is used, without the <tt>org.elasticsearch<tt> prefix.
     */
    Settings getComponentSettings(Class component);

    /**
     * Component settings for a specific component. Returns all the settings for the given class, where the
     * FQN of the class is used, without provided prefix.
     */
    Settings getComponentSettings(String prefix, Class component);

    /**
     * The class loader associted with this settings.
     */
    ClassLoader getClassLoader();

    /**
     * The settings as a {@link java.util.Map}.
     */
    Map<String, String> getAsMap();

    /**
     * Returns the setting value associated with the setting key.
     *
     * @param setting The setting key
     * @return The setting value, <tt>null</tt> if it does not exists.
     */
    String get(String setting);

    /**
     * Returns the setting value associated with the setting key. If it does not exists,
     * returns the default value provided.
     *
     * @param setting      The setting key
     * @param defaultValue The value to return if no value is associated with the setting
     * @return The setting value, or the default value if no value exists
     */
    String get(String setting, String defaultValue);

    /**
     * Returns group settings for the given setting prefix.
     */
    Map<String, Settings> getGroups(String settingPrefix) throws SettingsException;

    /**
     * Returns the setting value (as float) associated with the setting key. If it does not exists,
     * returns the default value provided.
     *
     * @param setting      The setting key
     * @param defaultValue The value to return if no value is associated with the setting
     * @return The (float) value, or the default value if no value exists.
     * @throws SettingsException Failure to parse the setting
     */
    Float getAsFloat(String setting, Float defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as double) associated with the setting key. If it does not exists,
     * returns the default value provided.
     *
     * @param setting      The setting key
     * @param defaultValue The value to return if no value is associated with the setting
     * @return The (double) value, or the default value if no value exists.
     * @throws SettingsException Failure to parse the setting
     */
    Double getAsDouble(String setting, Double defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as int) associated with the setting key. If it does not exists,
     * returns the default value provided.
     *
     * @param setting      The setting key
     * @param defaultValue The value to return if no value is associated with the setting
     * @return The (int) value, or the default value if no value exists.
     * @throws SettingsException Failure to parse the setting
     */
    Integer getAsInt(String setting, Integer defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as long) associated with the setting key. If it does not exists,
     * returns the default value provided.
     *
     * @param setting      The setting key
     * @param defaultValue The value to return if no value is associated with the setting
     * @return The (long) value, or the default value if no value exists.
     * @throws SettingsException Failure to parse the setting
     */
    Long getAsLong(String setting, Long defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as boolean) associated with the setting key. If it does not exists,
     * returns the default value provided.
     *
     * @param setting      The setting key
     * @param defaultValue The value to return if no value is associated with the setting
     * @return The (boolean) value, or the default value if no value exists.
     * @throws SettingsException Failure to parse the setting
     */
    Boolean getAsBoolean(String setting, Boolean defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as time) associated with the setting key. If it does not exists,
     * returns the default value provided.
     *
     * @param setting      The setting key
     * @param defaultValue The value to return if no value is associated with the setting
     * @return The (time) value, or the default value if no value exists.
     * @throws SettingsException Failure to parse the setting
     * @see TimeValue#parseTimeValue(String, org.elasticsearch.util.TimeValue)
     */
    TimeValue getAsTime(String setting, TimeValue defaultValue) throws SettingsException;

    /**
     * Returns the setting value (as size) associated with the setting key. If it does not exists,
     * returns the default value provided.
     *
     * @param setting      The setting key
     * @param defaultValue The value to return if no value is associated with the setting
     * @return The (size) value, or the default value if no value exists.
     * @throws SettingsException Failure to parse the setting
     * @see SizeValue#parse(String, SizeValue)
     */
    SizeValue getAsSize(String setting, SizeValue defaultValue) throws SettingsException;

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
     *
     * @param settingPrefix The setting prefix to load the array by
     * @return The setting array values
     * @throws SettingsException
     */
    String[] getAsArray(String settingPrefix) throws SettingsException;

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
