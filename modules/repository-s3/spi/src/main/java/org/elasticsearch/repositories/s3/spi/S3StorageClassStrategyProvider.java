/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3.spi;

import org.elasticsearch.common.settings.Settings;

/**
 * A provider for a {@link S3StorageClassStrategy}, registered through SPI.
 */
public interface S3StorageClassStrategyProvider {
    S3StorageClassStrategy getS3StorageClassStrategy(Settings repositorySettings);
}
