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
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public interface Settings {

    Settings getGlobalSettings();

    Settings getComponentSettings(Class component);

    Settings getComponentSettings(String prefix, Class component);

    ClassLoader getClassLoader();

    Map<String, String> getAsMap();

    String get(String setting);

    String get(String setting, String defaultValue);

    Map<String, Settings> getGroups(String settingPrefix) throws SettingsException;

    Float getAsFloat(String setting, Float defaultValue) throws SettingsException;

    Double getAsDouble(String setting, Double defaultValue) throws SettingsException;

    Integer getAsInt(String setting, Integer defaultValue) throws SettingsException;

    Long getAsLong(String setting, Long defaultValue) throws SettingsException;

    Boolean getAsBoolean(String setting, Boolean defaultValue) throws SettingsException;

    TimeValue getAsTime(String setting, TimeValue defaultValue) throws SettingsException;

    SizeValue getAsSize(String setting, SizeValue defaultValue) throws SettingsException;

    <T> Class<? extends T> getAsClass(String setting, Class<? extends T> defaultClazz) throws SettingsException;

    <T> Class<? extends T> getAsClass(String setting, Class<? extends T> defaultClazz, String prefixPackage, String suffixClassName) throws SettingsException;

    String[] getAsArray(String settingPrefix) throws SettingsException;

    interface Builder {
        Settings build();
    }
}
