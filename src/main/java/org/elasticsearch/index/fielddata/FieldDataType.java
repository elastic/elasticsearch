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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.FieldMapper.Loading;

/**
 */
public class FieldDataType {

    public static final String FORMAT_KEY = "format";
    public static final String DOC_VALUES_FORMAT_VALUE = "doc_values";

    private final String type;
    private final String typeFormat;
    private final Loading loading;
    private final Settings settings;

    public FieldDataType(String type) {
        this(type, ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public FieldDataType(String type, Settings.Builder builder) {
        this(type, builder.build());
    }

    public FieldDataType(String type, Settings settings) {
        this.type = type;
        this.typeFormat = "index.fielddata.type." + type + "." + FORMAT_KEY;
        this.settings = settings;
        final String loading = settings.get(Loading.KEY);
        this.loading = Loading.parse(loading, Loading.LAZY);
    }

    public String getType() {
        return this.type;
    }

    public Settings getSettings() {
        return this.settings;
    }

    public Loading getLoading() {
        return loading;
    }

    public String getFormat(Settings indexSettings) {
        String format = settings.get(FORMAT_KEY);
        if (format == null && indexSettings != null) {
            format = indexSettings.get(typeFormat);
        }
        return format;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldDataType that = (FieldDataType) o;

        if (!settings.equals(that.settings)) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + settings.hashCode();
        return result;
    }
}
