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
import org.apache.commons.codec.digest.Sha2Crypt;
import org.elasticsearch.common.os.OsUtils;
import org.elasticsearch.shield.ShieldSettingsException;

import java.util.Arrays;
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
            if (hashStr.startsWith(SHA2_PREFIX_5) || hashStr.startsWith(SHA2_PREFIX_6)) {
                return hashStr.compareTo(Sha2Crypt.sha256Crypt(textBytes, hashStr)) == 0;
            }
            return CRYPT_SUPPORTED ?
                    hashStr.compareTo(Crypt.crypt(textBytes, hashStr)) == 0 :  // crypt algo
                    text.equals(hashStr);                                      // plain text
        }
    },


    BCRYPT() {
        @Override
        public char[] hash(SecuredString text) {
            String salt = org.elasticsearch.shield.authc.support.BCrypt.gensalt();
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT4() {
        @Override
        public char[] hash(SecuredString text) {
            String salt = org.elasticsearch.shield.authc.support.BCrypt.gensalt(4);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT5() {
        @Override
        public char[] hash(SecuredString text) {
            String salt = org.elasticsearch.shield.authc.support.BCrypt.gensalt(5);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT6() {
        @Override
        public char[] hash(SecuredString text) {
            String salt = org.elasticsearch.shield.authc.support.BCrypt.gensalt(6);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT7() {
        @Override
        public char[] hash(SecuredString text) {
            String salt = org.elasticsearch.shield.authc.support.BCrypt.gensalt(7);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT8() {
        @Override
        public char[] hash(SecuredString text) {
            String salt = org.elasticsearch.shield.authc.support.BCrypt.gensalt(8);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT9() {
        @Override
        public char[] hash(SecuredString text) {
            String salt = org.elasticsearch.shield.authc.support.BCrypt.gensalt(9);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    MD5() {
        @Override
        public char[] hash(SecuredString text) {
            byte[] textBytes = CharArrays.toUtf8Bytes(text.internalChars());
            return Md5Crypt.apr1Crypt(textBytes).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(APR1_PREFIX)) {
                return false;
            }
            byte[] textBytes = CharArrays.toUtf8Bytes(text.internalChars());
            return hashStr.compareTo(Md5Crypt.apr1Crypt(textBytes, hashStr)) == 0;
        }
    },

    SHA1() {
        @Override
        public char[] hash(SecuredString text) {
            byte[] textBytes = CharArrays.toUtf8Bytes(text.internalChars());
            String hash = Base64.encodeBase64String(DigestUtils.sha1(textBytes));
            return (SHA1_PREFIX + hash).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(SHA1_PREFIX)) {
                return false;
            }
            byte[] textBytes = CharArrays.toUtf8Bytes(text.internalChars());
            String passwd64 = Base64.encodeBase64String(DigestUtils.sha1(textBytes));
            return hashStr.substring(SHA1_PREFIX.length()).compareTo(passwd64) == 0;
        }
    },

    SHA2() {
        @Override
        public char[] hash(SecuredString text) {
            byte[] textBytes = CharArrays.toUtf8Bytes(text.internalChars());
            return Sha2Crypt.sha256Crypt(textBytes).toCharArray();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            String hashStr = new String(hash);
            if (hashStr.startsWith(SHA2_PREFIX_5) || hashStr.startsWith(SHA2_PREFIX_6)) {
                byte[] textBytes = CharArrays.toUtf8Bytes(text.internalChars());
                return hashStr.compareTo(Sha2Crypt.sha256Crypt(textBytes, hashStr)) == 0;
            }
            return false;
        }
    },

    NOOP() {
        @Override
        public char[] hash(SecuredString text) {
            return text.copyChars();
        }

        @Override
        public boolean verify(SecuredString text, char[] hash) {
            return Arrays.equals(text.internalChars(), hash);
        }
    };

    private static final String APR1_PREFIX = "$apr1$";
    private static final String BCRYPT_PREFIX = "$2a$";
    private static final String SHA1_PREFIX = "{SHA}";
    private static final String SHA2_PREFIX_5 = "$5$";
    private static final String SHA2_PREFIX_6 = "$6$";
    private static final String PLAIN_PREFIX = "{plain}";
    static final boolean CRYPT_SUPPORTED = !OsUtils.WINDOWS;

    public static Hasher resolve(String name, Hasher defaultHasher) {
        if (name == null) {
            return defaultHasher;
        }
        switch (name.toLowerCase(Locale.ROOT)) {
            case "htpasswd"     : return HTPASSWD;
            case "bcrypt"       : return BCRYPT;
            case "bcrypt4"      : return BCRYPT4;
            case "bcrypt5"      : return BCRYPT5;
            case "bcrypt6"      : return BCRYPT6;
            case "bcrypt7"      : return BCRYPT7;
            case "bcrypt8"      : return BCRYPT8;
            case "bcrypt9"      : return BCRYPT9;
            case "sha1"         : return SHA1;
            case "sha2"         : return SHA2;
            case "md5"          : return MD5;
            case "noop"         :
            case "clear_text"   :  return NOOP;
            default:
                return defaultHasher;
        }
    }

    public static Hasher resolve(String name) {
        Hasher hasher = resolve(name, null);
        if (hasher == null) {
            throw new ShieldSettingsException("unknown hash function [" + name + "]");
        }
        return hasher;
    }

    public abstract char[] hash(SecuredString data);

    public abstract boolean verify(SecuredString data, char[] hash);

}
