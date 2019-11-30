package org.elasticsearch.repositories.encrypted;

public class EncryptedRepository {
    static final int GCM_TAG_SIZE_IN_BYTES = 16;
    static final int GCM_IV_SIZE_IN_BYTES = 12;
    static final String GCM_ENCRYPTION_SCHEME = "AES/GCM/NoPadding";
    static final long PACKET_START_COUNTER = Long.MIN_VALUE;
}
