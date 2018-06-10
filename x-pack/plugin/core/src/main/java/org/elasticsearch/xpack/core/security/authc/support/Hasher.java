/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SecureString;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Random;

public interface Hasher {

    char[] hash(SecureString data);

    char[] hash(SecureString data, int cost);

    boolean verify(SecureString data, char[] hash);

    String getAlgorithm();


    class NoopHasher implements Hasher {

        @Override
        public char[] hash(SecureString data, int cost) {
            return data.clone().getChars();
        }

        @Override
        public char[] hash(SecureString data) {
            return data.clone().getChars();
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return CharArrays.constantTimeEquals(data.getChars(), hash);
        }

        @Override
        public String getAlgorithm() {
            return "noop";
        }
    }

    class BcryptHasher implements Hasher {
        private int defaultCost;
        private static final String BCRYPT_PREFIX = "$2a$";

        public BcryptHasher(int cost) {
            this.defaultCost = cost;
        }

        @Override
        public char[] hash(SecureString data) {
            return hash(data, defaultCost);
        }

        @Override
        public char[] hash(SecureString data, int cost) {
            String salt = BCrypt.gensalt(cost);
            return BCrypt.hashpw(data, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            String hashStr = new String(hash);
            if (!hashStr.startsWith(BCRYPT_PREFIX)) {
                return false;
            }
            return BCrypt.checkpw(data, hashStr);
        }

        @Override
        public String getAlgorithm() {
            return "bcrypt";
        }
    }

    class PBKDF2Hasher implements Hasher {
        private int defaultCost;
        private static final String PBKDF2_PREFIX = "{PBKDF2}";

        public PBKDF2Hasher(int cost) {
            this.defaultCost = cost;
        }

        @Override
        public char[] hash(SecureString data, int cost) {
            try {
                StringBuilder result = new StringBuilder();
                result.append(PBKDF2_PREFIX);
                String costString = String.valueOf(cost);
                result.append(costString);
                result.append("$");
                char[] salt = SaltProvider.salt(32);  // 32 characters for 64 bytes
                result.append(salt);
                result.append("$");
                SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
                PBEKeySpec keySpec = new PBEKeySpec(data.getChars(), CharArrays.toUtf8Bytes(salt), cost, 256);
                String pwdHash = Base64.getEncoder().encodeToString(secretKeyFactory.generateSecret(keySpec).getEncoded());
                result.append(pwdHash);
                return result.toString().toCharArray();
            } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
                throw new IllegalStateException("Can't use PKDF2 for password hashing", e);
            }
        }

        @Override
        public char[] hash(SecureString data) {
            return hash(data, defaultCost);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            try {
            String hashStr = new String(hash);
                if (!hashStr.startsWith(PBKDF2_PREFIX)) {
                    return false;
                }
                hashStr = hashStr.replace(PBKDF2_PREFIX, "");
                String[] tokens = hashStr.split("\\$");
                if (tokens.length != 3) {
                return false;
            }
                int cost = Integer.parseInt(tokens[0]);
                String salt = tokens[1];
                SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
                PBEKeySpec keySpec = new PBEKeySpec(data.getChars(), salt.getBytes(StandardCharsets.UTF_8), cost, 256);
                String pwdHash = Base64.getEncoder().encodeToString(secretKeyFactory.generateSecret(keySpec).getEncoded());
                return CharArrays.constantTimeEquals(pwdHash, tokens[2]);
            } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
                throw new IllegalStateException("Can't use PKDF2 for password hashing", e);
            }
        }

        @Override
        public String getAlgorithm() {
            return "pbkdf2";
        }
    }

    class SSHA256Hasher implements Hasher {
        private static final String SSHA256_PREFIX = "{SSHA256}";

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
        public char[] hash(SecureString data, int cost) {
            // No cost factor for SSHA256
            return hash(data);
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

        @Override
        public String getAlgorithm() {
            return "ssha256";
        }
    }

    class SHA1Hasher implements Hasher {
        private static final String SHA1_PREFIX = "{SHA}";

        @Override
        public char[] hash(SecureString text) {
            byte[] textBytes = CharArrays.toUtf8Bytes(text.getChars());
            MessageDigest md = MessageDigests.sha1();
            md.update(textBytes);
            String hash = Base64.getEncoder().encodeToString(md.digest());
            return (SHA1_PREFIX + hash).toCharArray();
        }

        @Override
        public char[] hash(SecureString data, int cost) {
            return hash(data);
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

        @Override
        public String getAlgorithm() {
            return "sha1";
        }
    }

    class MD5Hasher implements Hasher {
        private static final String MD5_PREFIX = "{MD5}";

        @Override
        public char[] hash(SecureString text) {
            MessageDigest md = MessageDigests.md5();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            String hash = Base64.getEncoder().encodeToString(md.digest());
            return (MD5_PREFIX + hash).toCharArray();
        }

        @Override
        public char[] hash(SecureString data, int cost) {
            return hash(data);
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

        @Override
        public String getAlgorithm() {
            return "md5";
    }
    }

    final class SaltProvider {

        private SaltProvider() {
            throw new IllegalStateException("Utility class should not be instantiated");
        }

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
