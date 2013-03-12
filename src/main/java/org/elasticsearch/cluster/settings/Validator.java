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

package org.elasticsearch.cluster.settings;

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.unit.TimeValue;

/**
 */
public interface Validator {
    String validate(String setting, String value);

    public static class EmptyValidator implements Validator {

        public static final EmptyValidator INSTANCE = new EmptyValidator();

        private EmptyValidator() {

        }

        @Override
        public String validate(String setting, String value) {
            return null;
        }
    }

    public static class TimeValueValidator implements Validator {

        public static final TimeValueValidator INSTANCE = new TimeValueValidator();

        private TimeValueValidator() {

        }

        @Override
        public String validate(String setting, String value) {
            try {
                if (TimeValue.parseTimeValue(value, null) == null) {
                    return "cannot parse value [" + value + "] as time";
                }
            } catch (ElasticSearchParseException ex) {
                return "cannot parse value [" + value + "] as time";
            }
            return null;
        }
    }
}
