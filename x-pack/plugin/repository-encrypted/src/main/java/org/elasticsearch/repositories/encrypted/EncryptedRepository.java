/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

public class EncryptedRepository {
    static final int GCM_TAG_SIZE_IN_BYTES = 16;
    static final int GCM_IV_SIZE_IN_BYTES = 12;
    static final String GCM_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
}
