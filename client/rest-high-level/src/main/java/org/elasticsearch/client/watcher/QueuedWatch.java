/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.joda.time.DateTime;

import java.util.Objects;

public class QueuedWatch {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<QueuedWatch, Void> PARSER =
        new ConstructingObjectParser<>("watcher_stats_node", true, (args, c) -> new QueuedWatch(
            (String) args[0],
            (String) args[1],
            DateTime.parse((String) args[2]),
            DateTime.parse((String) args[3])
        ));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("watch_id"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("watch_record_id"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("triggered_time"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("execution_time"));
    }


    private final String watchId;
    private final String watchRecordId;
    private final DateTime triggeredTime;
    private final DateTime executionTime;

    public QueuedWatch(String watchId, String watchRecordId, DateTime triggeredTime, DateTime executionTime) {
        this.watchId = watchId;
        this.watchRecordId = watchRecordId;
        this.triggeredTime = triggeredTime;
        this.executionTime = executionTime;
    }

    public String getWatchId() {
        return watchId;
    }

    public String getWatchRecordId() {
        return watchRecordId;
    }

    public DateTime getTriggeredTime() {
        return triggeredTime;
    }

    public DateTime getExecutionTime() {
        return executionTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueuedWatch that = (QueuedWatch) o;
        return Objects.equals(watchId, that.watchId) &&
            Objects.equals(watchRecordId, that.watchRecordId) &&
            Objects.equals(triggeredTime, that.triggeredTime) &&
            Objects.equals(executionTime, that.executionTime);
    }

    @Override
    public int hashCode() {

        return Objects.hash(watchId, watchRecordId, triggeredTime, executionTime);
    }
}
