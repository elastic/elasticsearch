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

package org.elasticsearch.rest.support;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

/**
 *
 */
public abstract class AbstractRestRequest implements RestRequest {

    @Override
    public final String path() {
        return RestUtils.decodeComponent(rawPath());
    }

    @Override
    public float paramAsFloat(String key, float defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new ElasticSearchIllegalArgumentException("Failed to parse float parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public int paramAsInt(String key, int defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new ElasticSearchIllegalArgumentException("Failed to parse int parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public long paramAsLong(String key, long defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new ElasticSearchIllegalArgumentException("Failed to parse int parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public boolean paramAsBoolean(String key, boolean defaultValue) {
        return Booleans.parseBoolean(param(key), defaultValue);
    }

    @Override
    public Boolean paramAsBooleanOptional(String key, Boolean defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        return !(sValue.equals("false") || sValue.equals("0") || sValue.equals("off"));
    }

    @Override
    public TimeValue paramAsTime(String key, TimeValue defaultValue) {
        return parseTimeValue(param(key), defaultValue);
    }

    @Override
    public ByteSizeValue paramAsSize(String key, ByteSizeValue defaultValue) {
        return parseBytesSizeValue(param(key), defaultValue);
    }

    @Override
    public String[] paramAsStringArray(String key, String[] defaultValue) {
        String value = param(key);
        if (value == null) {
            return defaultValue;
        }
        return Strings.splitStringByCommaToArray(value);
    }
}
