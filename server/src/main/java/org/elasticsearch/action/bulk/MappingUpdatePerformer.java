/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.shard.ShardId;

public interface MappingUpdatePerformer {

    /**
     * Update the mappings on the master.
     */
    void updateMappings(Mapping update, ShardId shardId, ActionListener<Void> listener);

}
