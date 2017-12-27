/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SecureString;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Locale;
import java.util.Random;

public enum Hasher {

    BCRYPT() {
        @Override
        public char[] hash(SecureString text) {
            String salt = org.elasticsearch.xpack.security.authc.support.BCrypt.gensalt();
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT4() {
        @Override
        public char[] hash(SecureString text) {
            String salt = org.elasticsearch.xpack.security.authc.support.BCrypt.gensalt(4);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT5() {
        @Override
        public char[] hash(SecureString text) {
            String salt = org.elasticsearch.xpack.security.authc.support.BCrypt.gensalt(5);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT6() {
        @Override
        public char[] hash(SecureString text) {
            String salt = org.elasticsearch.xpack.security.authc.support.BCrypt.gensalt(6);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT7() {
        @Override
        public char[] hash(SecureString text) {
            String salt = org.elasticsearch.xpack.security.authc.support.BCrypt.gensalt(7);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT8() {
        @Override
        public char[] hash(SecureString text) {
            String salt = org.elasticsearch.xpack.security.authc.support.BCrypt.gensalt(8);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    BCRYPT9() {
        @Override
        public char[] hash(SecureString text) {
            String salt = org.elasticsearch.xpack.security.authc.support.BCrypt.gensalt(9);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(text, hashStr);
        }
    },

    SHA1() {
        @Override
        public char[] hash(SecureString text) {
            byte[] textBytes = CharArrays.toUtf8Bytes(text.getChars());
            MessageDigest md = MessageDigests.sha1();
            md.update(textBytes);
            String hash = Base64.getEncoder().encodeToString(md.digest());
            return (SHA1_PREFIX + hash).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(SHA1_PREFIX)) {
                return false;
            }
            byte[] textBytes = CharArrays.toUtf8Bytes(text.getChars());
            MessageDigest md = MessageDigests.sha1();
            md.update(textBytes);
            String passwd64 = Base64.getEncoder().encodeToString(md.digest());
            String hashNoPrefix = hashStr.substring(SHA1_PREFIX.length());
            return CharArrays.constantTimeEquals(hashNoPrefix, passwd64);
        }
    },

    MD5() {
        @Override
        public char[] hash(SecureString text) {
            MessageDigest md = MessageDigests.md5();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            String hash = Base64.getEncoder().encodeToString(md.digest());
            return (MD5_PREFIX + hash).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(MD5_PREFIX)) {
                return false;
            }
            hashStr = hashStr.substring(MD5_PREFIX.length());
            MessageDigest md = MessageDigests.md5();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            String computedHashStr = Base64.getEncoder().encodeToString(md.digest());
            return CharArrays.constantTimeEquals(hashStr, computedHashStr);
        }
    },

    SSHA256() {
        @Override
        public char[] hash(SecureString text) {
            MessageDigest md = MessageDigests.sha256();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            char[] salt = SaltProvider.salt(8);
            md.update(CharArrays.toUtf8Bytes(salt));
            String hash = Base64.getEncoder().encodeToString(md.digest());
            char[] result = new char[SSHA256_PREFIX.length() + salt.length + hash.length()];
            System.arraycopy(SSHA256_PREFIX.toCharArray(), 0, result, 0, SSHA256_PREFIX.length());
            System.arraycopy(salt, 0, result, SSHA256_PREFIX.length(), salt.length);
            System.arraycopy(hash.toCharArray(), 0, result, SSHA256_PREFIX.length() + salt.length, hash.length());
            return result;
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(SSHA256_PREFIX)) {
                return false;
            }
            hashStr = hashStr.substring(SSHA256_PREFIX.length());
            char[] saltAndHash = hashStr.toCharArray();
            MessageDigest md = MessageDigests.sha256();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            md.update(new String(saltAndHash, 0, 8).getBytes(StandardCharsets.UTF_8));
            String computedHash = Base64.getEncoder().encodeToString(md.digest());
            return CharArrays.constantTimeEquals(computedHash, new String(saltAndHash, 8, saltAndHash.length - 8));
        }
    },

    NOOP() {
        @Override
        public char[] hash(SecureString text) {
            return text.clone().getChars();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return CharArrays.constantTimeEquals(text.getChars(), hash);
        }
    };

    private static final String BCRYPT_PREFIX = "$2a$";
    private static final String SHA1_PREFIX = "{SHA}";
    private static final String MD5_PREFIX = "{MD5}";
    private static final String SSHA256_PREFIX = "{SSHA256}";

    public static Hasher resolve(String name, Hasher defaultHasher) {
        if (name == null) {
            return defaultHasher;
        }
        switch (name.toLowerCase(Locale.ROOT)) {
            case "bcrypt":
                return BCRYPT;
            case "bcrypt4":
                return BCRYPT4;
            case "bcrypt5":
                return BCRYPT5;
            case "bcrypt6":
                return BCRYPT6;
            case "bcrypt7":
                return BCRYPT7;
            case "bcrypt8":
                return BCRYPT8;
            case "bcrypt9":
                return BCRYPT9;
            case "sha1":
                return SHA1;
            case "md5":
                return MD5;
            case "ssha256":
                return SSHA256;
            case "noop":
            case "clear_text":
                return NOOP;
            default:
                return defaultHasher;
        }
    }

    public static Hasher resolve(String name) {
        Hasher hasher = resolve(name, null);
        if (hasher == null) {
            throw new IllegalArgumentException("unknown hash function [" + name + "]");
        }
        return hasher;
    }

    public abstract char[] hash(SecureString data);

    public abstract boolean verify(SecureString data, char[] hash);

    static final class SaltProvider {

        static final char[] ALPHABET = new char[]{
                '.', '/', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
                'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
                'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
        };

        public static char[] salt(int length) {
            Random random = Randomness.get();
            char[] salt = new char[length];
            for (int i = 0; i < length; i++) {
                salt[i] = ALPHABET[(random.nextInt(ALPHABET.length))];
            }
            return salt;
        }
    }
}
