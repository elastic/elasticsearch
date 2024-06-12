/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.List;
import java.util.Map;

// modelled after UpdateIndexShardSnapshotStatusRequest

/**
 * Internal request that is used to send changes in event.ingested min/max cluster state ranges (per index) to master
 */
public class UpdateEventIngestedRangeRequest extends MasterNodeRequest<UpdateEventIngestedRangeRequest> {
    private static final Logger logger = LogManager.getLogger(UpdateEventIngestedRangeRequest.class);

    private Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> eventIngestedRangeMap;

    protected UpdateEventIngestedRangeRequest(Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> rangeMap) {
        super(TimeValue.MAX_VALUE); // By default, keep trying to post updates to avoid snapshot processes getting stuck // TODO: this ok?
        this.eventIngestedRangeMap = rangeMap;
    }

    protected UpdateEventIngestedRangeRequest(StreamInput in) throws IOException {
        // TODO: likely need put TransportVersion guards here like we did for _resolve/cluster?
        super(in);
        logger.warn("LLL: DEBUG 1 READ from StreamInput of UpdateEventIngestedRangeRequest");
        this.eventIngestedRangeMap = in.readMap(
            Index::new,
            i -> i.readCollectionAsList(EventIngestedRangeClusterStateService.ShardRangeInfo::new)
        );
        logger.warn(
            "LLL: successfully DEBUG 2 READ from StreamInput of UpdateEventIngestedRangeRequest; dummyMap: {}",
            eventIngestedRangeMap
        );
    }

    public Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> getEventIngestedRangeMap() {
        return eventIngestedRangeMap;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // TODO: likely need put TransportVersion guards here like we did for _resolve/cluster?
        logger.warn("LLL: DEBUG 1 WROTE to StreamOutput of UpdateEventIngestedRangeRequest");
        out.writeMap(eventIngestedRangeMap, StreamOutput::writeWriteable, StreamOutput::writeCollection);
        logger.warn("LLL: successfully DEBUG 2 WROTE to StreamOutput of UpdateEventIngestedRangeRequest");
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String getDescription() {
        return "update event.ingested min/max range request"; // MP TODO: remove this?
    }

    @Override
    public String toString() {
        return "UpdateEventIngestedRangeRequest{" + eventIngestedRangeMap + "}";
    }
}
