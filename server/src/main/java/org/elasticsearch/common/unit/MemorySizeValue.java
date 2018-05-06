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

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Objects;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;

/** Utility methods to get memory sizes. */
public enum MemorySizeValue {
    ;

    /** Parse the provided string as a memory size. This method either accepts absolute values such as
     *  {@code 42} (default assumed unit is byte) or {@code 2mb}, or percentages of the heap size: if
     *  the heap is 1G, {@code 10%} will be parsed as {@code 100mb}.  */
    public static ByteSizeValue parseBytesSizeValueOrHeapRatio(String sValue, String settingName) {
        settingName = Objects.requireNonNull(settingName);
        if (sValue != null && sValue.endsWith("%")) {
            final String percentAsString = sValue.substring(0, sValue.length() - 1);
            try {
                final double percent = Double.parseDouble(percentAsString);
                if (percent < 0 || percent > 100) {
                    throw new ElasticsearchParseException("percentage should be in [0-100], got [{}]", percentAsString);
                }
                return new ByteSizeValue((long) ((percent / 100) * JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()), ByteSizeUnit.BYTES);
            } catch (NumberFormatException e) {
                throw new ElasticsearchParseException("failed to parse [{}] as a double", e, percentAsString);
            }
        } else {
            return parseBytesSizeValue(sValue, settingName);
        }
    }
}
