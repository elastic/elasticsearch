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

package org.elasticsearch.index.query;

import org.apache.lucene.queries.intervals.IntervalsSource;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

public abstract class IntervalFilter implements NamedWriteable, ToXContentObject {
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);

    public abstract IntervalsSource filter(IntervalsSource input, QueryShardContext context, MappedFieldType fieldType)
        throws IOException;

    public static IntervalFilter fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
        }
        String type = parser.currentName();
        try {
            return parser.namedObject(IntervalFilter.class, type, null);
        } catch (NamedObjectNotFoundException e) {
            throw new ParsingException(
                new XContentLocation(e.getLineNumber(), e.getColumnNumber()), "Unknown interval filter [" + type + "]", e);
        }
    }
}
