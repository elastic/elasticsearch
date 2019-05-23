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

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public final class MapParser {

    public interface MapFactory<T> {
        Map<String, T> newMap();
    }

    public interface MapValueParser<T> {
        T apply(XContentParser parser) throws IOException;
    }

    public static <T> Map<String, T> readGenericMap(
            XContentParser parser,
            MapFactory<T> mapFactory,
            MapValueParser<T> mapValueParser) throws IOException {
        Map<String, T> map = mapFactory.newMap();
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            parser.nextToken();
            T value = mapValueParser.apply(parser);
            map.put(fieldName, value);
        }
        return map;
    }

    private MapParser() {}
}
