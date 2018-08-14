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
package org.elasticsearch.protocol.xpack.watcher.status;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.joda.time.DateTime;

import java.io.IOException;

import static org.joda.time.DateTimeZone.UTC;

public class WatchStatusState implements ToXContentObject {

    final boolean active;
    final DateTime timestamp;

    public WatchStatusState(boolean active, DateTime timestamp) {
        this.active = active;
        this.timestamp = timestamp;
    }

    public boolean isActive() {
        return active;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(WatchStatusField.ACTIVE.getPreferredName(), active);
        WatcherDateTimeUtils.writeDate(WatchStatusField.TIMESTAMP.getPreferredName(), builder, timestamp);
        return builder.endObject();
    }

    public static WatchStatusState parse(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected an object but found [{}] instead", parser.currentToken());
        }
        boolean active = true;
        DateTime timestamp = DateTime.now(UTC);
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (WatchStatusField.ACTIVE.match(currentFieldName, parser.getDeprecationHandler())) {
                active = parser.booleanValue();
            } else if (WatchStatusField.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                timestamp = WatcherDateTimeUtils.parseDate(currentFieldName, parser, UTC);
            }
        }
        return new WatchStatusState(active, timestamp);
    }

    @Override
    public String toString() {
        return "WatchStatusState{" +
                "active=" + active +
                ", timestamp=" + timestamp +
                '}';
    }
}
