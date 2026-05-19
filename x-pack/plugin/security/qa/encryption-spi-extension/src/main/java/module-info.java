/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.xpack.core.crypto.EncryptedDataHandlerProvider;
import org.elasticsearch.xpack.security.encryption.spi.test.TestEncryptedDataHandlerProvider;

module org.elasticsearch.internal.security.encryption.spi.test {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    provides EncryptedDataHandlerProvider with TestEncryptedDataHandlerProvider;
}
