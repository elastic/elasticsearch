/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * <p>This package contains the key rotation functionality for the encrypted repository.</p>
 *
 * Key rotation happens by creating a {@link org.elasticsearch.repositories.encrypted.keyrotation.KeyRotationsInProgress.Entry} for the
 * repository that should rotate to a new key. This causes all new {@link org.elasticsearch.repositories.RepositoryOperation} to be queued
 * up instead of starting to execute for the repository. The key rotation process will then wait for existing {@code RepositoryOperation}
 * to finish executing to ensure that no new files relevant to any snapshot are written to the repository before it starts.
 *
 * Once the rotation is ready to execute its target key (TODO: hash/setting/identifier???)
 * its status is moved to {@link org.elasticsearch.repositories.encrypted.keyrotation.KeyRotationsInProgress.State#STARTED}.
 *
 * After the cluster state update that moves the rotations state to {@code STARTED} has been executed the physical rotation to the new
 * key can be executed.
 * Once the physical key rotation has finished, a new {@link org.elasticsearch.repositories.RepositoryData} is written to the repository,
 * encrypted and prepended with the new key identifier (TODO: can't we just put the key identifier used when writing a blob to every blob
 *                                                            we write to avoid any special path for RepositoryData? This seems like it
 *                                                            would also help debugging in other situations and the cost is trivial
 *                                                            relative to any blob we write out?).
 * The writing of this new repositoryData goes through the standard
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#writeIndexGen} path and uses a state update filter that moves the
 * repository settings to the new password and moves the rotation entry to state {@code CLEANUP} in the same update that points the
 * safe generation to the new repository data.
 *
 * Once the entry has been moved to state {@code CLEANUP} the previous key container referenced by the {@code #source} of the entry can be
 * deleted from the repository safely.
 *
 * After the previous key's container has been deleted from the repository, the key rotation entry is removed from the cluster state, thus
 * completing the rotation operation.
 *
 * Notes on failure modes:
 * This state machine is completely resilient to master failover by using the same mechanism of action used in snapshot deletes.
 * Concretely, it behaves as follows in failover scenarios:
 *
 * <p>a dangling waiting operation is executed on the new master node once it is ready to execute</p>
 * <p>a dangling started operation is executed on the new master node, continuing to use the existing identifier (TODO: how idempotent can
 *                                                                                                                      we get file
 *                                                                                                                      operations
 *                                                                                                                      here or do we just
 *                                                                                                                      want to bail in
 *                                                                                                                      this case?)</p>
 * <p>a dangling cleanup state rotation is executed on the new master node</p>
 */
package org.elasticsearch.repositories.encrypted.keyrotation;

