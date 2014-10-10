/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.Crypt;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.Md5Crypt;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.os.OsUtils;

import java.util.Locale;

/**
 *
 */
public enum Hasher {

    /**
     * A hasher that is compatible with apache htpasswd. Hashes by default using bcrypt type $2a$
     * but can verify any of the hashes supported by htpasswd.
     */
    HTPASSWD() {

        @Override
        public char[] hash(SecuredString text) {
            String salt = org.elasticsearch.shield.authc.support.BCrypt.gensalt();
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (hashStr.startsWith(BCRYPT_PREFIX_Y)) {
                hashStr = BCRYPT_PREFIX + hashStr.substring(BCRYPT_PREFIX_Y.length());
            }
            if (hashStr.startsWith(BCRYPT_PREFIX)) {
                return BCrypt.checkpw(text, hashStr);
            }
            if (hashStr.startsWith(PLAIN_PREFIX)) {
                hashStr = hashStr.substring(PLAIN_PREFIX.length());
                return text.equals(hashStr);
            }
            byte[] textBytes = CharArrays.toUtf8Bytes(text.internalChars());
            if (hashStr.startsWith(APR1_PREFIX)) {
                return hashStr.compareTo(Md5Crypt.apr1Crypt(textBytes, hashStr)) == 0;
            }
            if (hashStr.startsWith(SHA1_PREFIX)) {
                String passwd64 = Base64.encodeBase64String(DigestUtils.sha1(textBytes));
                return hashStr.substring(SHA1_PREFIX.length()).compareTo(passwd64) == 0;
            }
            return CRYPT_SUPPORTED ?
                    hashStr.compareTo(Crypt.crypt(textBytes, hashStr)) == 0 :     // crypt algo
                    text.equals(hashStr);                    // plain text
        }
    };

    private static final String APR1_PREFIX = "$apr1$";
    private static final String BCRYPT_PREFIX = "$2a$";
    private static final String BCRYPT_PREFIX_Y = "$2y$";
    private static final String SHA1_PREFIX = "{SHA}";
    private static final String PLAIN_PREFIX = "{plain}";
    static final boolean CRYPT_SUPPORTED = !OsUtils.WINDOWS;

    public static Hasher resolve(String name, Hasher defaultHasher) {
        if (name == null) {
            return defaultHasher;
        }
        switch (name.toLowerCase(Locale.ROOT)) {
            case "htpasswd" : return HTPASSWD;
            default:
                return defaultHasher;
        }
    }

    public static Hasher resolve(String name) {
        Hasher hasher = resolve(name, null);
        if (hasher == null) {
            throw new ElasticsearchIllegalArgumentException("Unknown hash function [" + name + "]");
        }
        return hasher;
    }

    public abstract char[] hash(SecuredString data);

    public abstract boolean verify(SecuredString data, char[] hash);

}
