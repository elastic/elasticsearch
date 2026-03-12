/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.shard.ShardId;

public interface MappingUpdatePerformer {

    /**
     * Update the mappings on the master.
     */
    void updateMappings(CompressedXContent update, ShardId shardId, ActionListener<Void> listener);

}
