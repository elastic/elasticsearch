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
 */
package org.elasticsearch.repositories.encrypted.keyrotation;

