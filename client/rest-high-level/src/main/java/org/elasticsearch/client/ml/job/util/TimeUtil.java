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
package org.elasticsearch.client.ml.job.util;

import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public final class TimeUtil {

    /**
     * Parse out a Date object given the current parser and field name.
     *
     * @param parser current XContentParser
     * @param fieldName the field's preferred name (utilized in exception)
     * @return parsed Date object
     * @throws IOException from XContentParser
     */
    public static Date parseTimeField(XContentParser parser, String fieldName) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            return new Date(parser.longValue());
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new Date(DateFormatters.from(DateTimeFormatter.ISO_INSTANT.parse(parser.text())).toInstant().toEpochMilli());
        }
        throw new IllegalArgumentException(
            "unexpected token [" + parser.currentToken() + "] for [" + fieldName + "]");
    }

}
