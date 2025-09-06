/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

public class PITHelper {

    public static SearchContextId decodePITId(String id) {
        return decodePITId(new BytesArray(Base64.getUrlDecoder().decode(id)));
    }

    public static SearchContextId decodePITId(BytesReference id) {
        try (var in = id.streamInput()) {
            final TransportVersion version = TransportVersion.readVersion(in);
            in.setTransportVersion(version);
            final Map<ShardId, SearchContextIdForNode> shards = Collections.unmodifiableMap(
                in.readCollection(Maps::newHashMapWithExpectedSize, (i, map) -> map.put(new ShardId(in), new SearchContextIdForNode(in)))
            );
            return new SearchContextId(shards, Collections.emptyMap());
        } catch (IOException e) {
            assert false : e;
            throw new IllegalArgumentException(e);
        }
    }
}
