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

/**
 * Dangling indices are indices that exist on disk on one or more nodes but
 * which do not currently exist in the cluster state. They arise in a
 * number of situations, such as:
 *
 * <ul>
 * <li>A user overflows the index graveyard by deleting more than 500 indices while a node is offline and then the node rejoins the
 * cluster</li>
 * <li>A node (unsafely) moves from one cluster to another, perhaps because the original cluster lost all its master nodes</li>
 * <li>A user (unsafely) meddles with the contents of the data path, maybe restoring an old index folder from a backup</li>
 * <li>A disk partially fails and the user has no replicas and no snapshots and wants to (unsafely) recover whatever they can</li>
 * <li>A cluster loses all master nodes and those are (unsafely) restored from backup, but the backup does not contain the index</li>
 * </ul>
 *
 * <p>The classes in this package form an API for managing dangling indices, allowing them to be listed, imported or deleted.
 */
package org.elasticsearch.action.admin.indices.dangling;
