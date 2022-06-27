/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.index.shard.ShardId;

import java.util.SortedSet;

record ShardsSize(long sizeInBytes, SortedSet<ShardId> shardIds) {}
