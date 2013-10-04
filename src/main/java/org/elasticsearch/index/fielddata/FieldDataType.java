/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperParsingException;

/**
 */
public class FieldDataType {

    private static final String LOADING_KEY = "loading";
    private static final String EAGER_LOADING_VALUE = "eager";
    private static final String LAZY_LOADING_VALUE = "lazy";

    public static enum Loading {
        LAZY, EAGER;
    }

    private final String type;
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
        this.settings = settings;
        final String loading = settings.get(LOADING_KEY);
        if (loading == null || loading.equals(LAZY_LOADING_VALUE)) {
            this.loading = Loading.LAZY;
        } else if (loading.equals(EAGER_LOADING_VALUE)) {
            this.loading = Loading.EAGER;
        } else {
            throw new MapperParsingException("Unknown [" + LOADING_KEY + "] value: [" + loading + "]");
        }
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
