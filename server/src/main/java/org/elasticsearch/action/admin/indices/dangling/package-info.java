/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
