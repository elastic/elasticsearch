/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.repositories.s3.advancedstoragetiering {
    requires org.elasticsearch.server;
    requires org.elasticsearch.repositories.s3;

    provides org.elasticsearch.repositories.s3.S3StorageClassStrategyProvider
        with
            org.elasticsearch.repositories.s3.advancedstoragetiering.AdvancedS3StorageClassStrategyProvider;
}
