/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.common.io.stream.VersionedNamedWriteable;

/**
 * This is an interface used a marker for a {@link VersionedNamedWriteable}.
 * Rank methods should subclass this with enough information to send back from
 * each shard to the coordinator to perform global ranking after the query phase.
 */
public interface RankShardResult extends VersionedNamedWriteable {

}
