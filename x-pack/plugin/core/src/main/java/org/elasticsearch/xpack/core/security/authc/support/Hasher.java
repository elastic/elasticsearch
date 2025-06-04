/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.SuppressForbidden;

import java.nio.CharBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public enum Hasher {

    BCRYPT() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt();
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT4() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(4);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT5() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(5);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT6() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(6);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT7() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(7);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT8() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(8);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT9() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(9);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT10() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(10);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT11() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(11);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT12() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(12);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT13() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(13);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    BCRYPT14() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(14);
            return BCrypt.hashpw(text, salt).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            return verifyBcryptHash(text, hash);
        }
    },

    PBKDF2() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, PBKDF2_DEFAULT_COST, PBKDF2_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash, PBKDF2_DEFAULT_COST, PBKDF2_PREFIX);
        }

    },

    PBKDF2_1000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 1000, PBKDF2_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash, 1000, PBKDF2_PREFIX);
        }

    },

    PBKDF2_10000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 10000, PBKDF2_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash, 10000, PBKDF2_PREFIX);
        }

    },

    PBKDF2_50000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 50000, PBKDF2_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash, 50000, PBKDF2_PREFIX);
        }

    },

    PBKDF2_100000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 100000, PBKDF2_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash, 100000, PBKDF2_PREFIX);
        }

    },

    PBKDF2_500000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 500000, PBKDF2_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash, 500000, PBKDF2_PREFIX);
        }

    },

    PBKDF2_1000000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 1000000, PBKDF2_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash, 1000000, PBKDF2_PREFIX);
        }

    },

    PBKDF2_STRETCH() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(new SecureString(hashSha512(data)), PBKDF2_DEFAULT_COST, PBKDF2_STRETCH_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(new SecureString(hashSha512(data)), hash, PBKDF2_DEFAULT_COST, PBKDF2_STRETCH_PREFIX);
        }

    },

    PBKDF2_STRETCH_1000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(new SecureString(hashSha512(data)), 1000, PBKDF2_STRETCH_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(new SecureString(hashSha512(data)), hash, 1000, PBKDF2_STRETCH_PREFIX);
        }

    },

    PBKDF2_STRETCH_10000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(new SecureString(hashSha512(data)), 10000, PBKDF2_STRETCH_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(new SecureString(hashSha512(data)), hash, 10000, PBKDF2_STRETCH_PREFIX);
        }

    },

    PBKDF2_STRETCH_50000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(new SecureString(hashSha512(data)), 50000, PBKDF2_STRETCH_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(new SecureString(hashSha512(data)), hash, 50000, PBKDF2_STRETCH_PREFIX);
        }

    },

    PBKDF2_STRETCH_100000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(new SecureString(hashSha512(data)), 100000, PBKDF2_STRETCH_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(new SecureString(hashSha512(data)), hash, 100000, PBKDF2_STRETCH_PREFIX);
        }

    },

    PBKDF2_STRETCH_500000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(new SecureString(hashSha512(data)), 500000, PBKDF2_STRETCH_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(new SecureString(hashSha512(data)), hash, 500000, PBKDF2_STRETCH_PREFIX);
        }

    },

    PBKDF2_STRETCH_1000000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(new SecureString(hashSha512(data)), 1000000, PBKDF2_STRETCH_PREFIX);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(new SecureString(hashSha512(data)), hash, 1000000, PBKDF2_STRETCH_PREFIX);
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
            if (hashStr.startsWith(SHA1_PREFIX) == false) {
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
            if (hashStr.startsWith(MD5_PREFIX) == false) {
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
            byte[] salt = generateSalt(8);
            md.update(salt);
            String hash = Base64.getEncoder().encodeToString(md.digest());
            char[] result = new char[SSHA256_PREFIX.length() + 12 + hash.length()];
            System.arraycopy(SSHA256_PREFIX.toCharArray(), 0, result, 0, SSHA256_PREFIX.length());
            System.arraycopy(Base64.getEncoder().encodeToString(salt).toCharArray(), 0, result, SSHA256_PREFIX.length(), 12);
            System.arraycopy(hash.toCharArray(), 0, result, SSHA256_PREFIX.length() + 12, hash.length());
            return result;
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            String hashStr = new String(hash);
            if (hashStr.startsWith(SSHA256_PREFIX) == false) {
                return false;
            }
            hashStr = hashStr.substring(SSHA256_PREFIX.length());
            char[] saltAndHash = hashStr.toCharArray();
            MessageDigest md = MessageDigests.sha256();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            // Base64 string length : (4*(n/3)) rounded up to the next multiple of 4 because of padding, 12 for 8 bytes
            md.update(Base64.getDecoder().decode(new String(saltAndHash, 0, 12)));
            String computedHash = Base64.getEncoder().encodeToString(md.digest());
            return CharArrays.constantTimeEquals(computedHash, new String(saltAndHash, 12, saltAndHash.length - 12));
        }
    },
    /*
     * Unsalted SHA-256 , not suited for password storage.
     */
    SHA256() {
        @Override
        public char[] hash(SecureString text) {
            MessageDigest md = MessageDigests.sha256();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(md.digest()).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            MessageDigest md = MessageDigests.sha256();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            return CharArrays.constantTimeEquals(Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(md.digest()).toCharArray(), hash);
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

    private static final int BCRYPT_PREFIX_LENGTH = 4;
    private static final String SHA1_PREFIX = "{SHA}";
    private static final String MD5_PREFIX = "{MD5}";
    private static final String SSHA256_PREFIX = "{SSHA256}";
    private static final String PBKDF2_PREFIX = "{PBKDF2}";
    private static final String PBKDF2_STRETCH_PREFIX = "{PBKDF2_STRETCH}";
    private static final int PBKDF2_DEFAULT_COST = 10000;
    private static final int PBKDF2_KEY_LENGTH = 256;
    private static final int BCRYPT_DEFAULT_COST = 10;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final int HMAC_SHA512_BLOCK_SIZE_IN_BITS = 128;
    private static final int PBKDF2_MIN_SALT_LENGTH_IN_BYTES = 8;

    /**
     * Returns a {@link Hasher} instance of the appropriate algorithm and associated cost as
     * indicated by the {@code name}. Name identifiers for the default costs for
     * BCRYPT and PBKDF2 return the default BCRYPT and PBKDF2 Hasher instead of the specific
     * instances for the associated cost.
     *
     * @param name The name of the algorithm and cost combination identifier
     * @return the hasher associated with the identifier
     */
    public static Hasher resolve(String name) {
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "bcrypt" -> BCRYPT;
            case "bcrypt4" -> BCRYPT4;
            case "bcrypt5" -> BCRYPT5;
            case "bcrypt6" -> BCRYPT6;
            case "bcrypt7" -> BCRYPT7;
            case "bcrypt8" -> BCRYPT8;
            case "bcrypt9" -> BCRYPT9;
            case "bcrypt10" -> BCRYPT;
            case "bcrypt11" -> BCRYPT11;
            case "bcrypt12" -> BCRYPT12;
            case "bcrypt13" -> BCRYPT13;
            case "bcrypt14" -> BCRYPT14;
            case "pbkdf2" -> PBKDF2;
            case "pbkdf2_1000" -> PBKDF2_1000;
            case "pbkdf2_10000" -> PBKDF2;
            case "pbkdf2_50000" -> PBKDF2_50000;
            case "pbkdf2_100000" -> PBKDF2_100000;
            case "pbkdf2_500000" -> PBKDF2_500000;
            case "pbkdf2_1000000" -> PBKDF2_1000000;
            case "pbkdf2_stretch" -> PBKDF2_STRETCH;
            case "pbkdf2_stretch_1000" -> PBKDF2_STRETCH_1000;
            case "pbkdf2_stretch_10000" -> PBKDF2_STRETCH_10000;
            case "pbkdf2_stretch_50000" -> PBKDF2_STRETCH_50000;
            case "pbkdf2_stretch_100000" -> PBKDF2_STRETCH_100000;
            case "pbkdf2_stretch_500000" -> PBKDF2_STRETCH_500000;
            case "pbkdf2_stretch_1000000" -> PBKDF2_STRETCH_1000000;
            case "sha1" -> SHA1;
            case "md5" -> MD5;
            case "ssha256" -> SSHA256;
            case "noop", "clear_text" -> NOOP;
            default -> throw new IllegalArgumentException("unknown hash function [" + name + "]");
        };
    }

    /**
     * Returns a {@link Hasher} instance that can be used to verify the {@code hash} by inspecting the
     * hash prefix and determining the algorithm used for its generation. If no specific algorithm
     * prefix, can be determined {@code Hasher.NOOP} is returned.
     *
     * @param hash the char array from which the hashing algorithm is to be deduced
     * @return the hasher that can be used for validation
     */
    public static Hasher resolveFromHash(char[] hash) {
        if (isBcryptPrefix(hash)) {
            int cost = Integer.parseInt(new String(Arrays.copyOfRange(hash, BCRYPT_PREFIX_LENGTH, hash.length - 54)));
            return cost == BCRYPT_DEFAULT_COST ? Hasher.BCRYPT : resolve("bcrypt" + cost);
        } else if (CharArrays.charsBeginsWith(PBKDF2_STRETCH_PREFIX, hash)) {
            int cost = parsePbkdf2Iterations(hash, PBKDF2_STRETCH_PREFIX);
            return cost == PBKDF2_DEFAULT_COST ? Hasher.PBKDF2_STRETCH : resolve("pbkdf2_stretch_" + cost);
        } else if (CharArrays.charsBeginsWith(PBKDF2_PREFIX, hash)) {
            int cost = parsePbkdf2Iterations(hash, PBKDF2_PREFIX);
            return cost == PBKDF2_DEFAULT_COST ? Hasher.PBKDF2 : resolve("pbkdf2_" + cost);
        } else if (CharArrays.charsBeginsWith(SHA1_PREFIX, hash)) {
            return Hasher.SHA1;
        } else if (CharArrays.charsBeginsWith(MD5_PREFIX, hash)) {
            return Hasher.MD5;
        } else if (CharArrays.charsBeginsWith(SSHA256_PREFIX, hash)) {
            return Hasher.SSHA256;
        } else {
            // This is either a non hashed password from cache or a corrupted hash string.
            return Hasher.NOOP;
        }
    }

    private static boolean isBcryptPrefix(char[] hash) {
        if (hash.length < 4) {
            return false;
        }
        if (hash[0] == '$' && hash[1] == '2' && hash[3] == '$') {
            return BCrypt.valid_minor(hash[2]);
        }
        return false;
    }

    /**
     * Verifies that the cryptographic hash of {@code data} is the same as {@code hash}. The
     * hashing algorithm and its parameters(cost factor-iterations, salt) are deduced from the
     * hash itself. The {@code hash} char array is not cleared after verification.
     *
     * @param data the SecureString to be hashed and verified
     * @param hash the char array with the hash against which the string is verified
     * @return true if the hash corresponds to the data, false otherwise
     */
    public static boolean verifyHash(SecureString data, char[] hash) {
        final Hasher hasher = resolveFromHash(hash);
        return hasher.verify(data, hash);
    }

    private static char[] getPbkdf2Hash(SecureString data, int cost, String prefix) {
        try {
            // Base64 string length : (4*(n/3)) rounded up to the next multiple of 4 because of padding.
            // n is 32 (PBKDF2_KEY_LENGTH in bytes) and 2 is because of the dollar sign delimiters.
            CharBuffer result = CharBuffer.allocate(prefix.length() + String.valueOf(cost).length() + 2 + 44 + 44);
            result.put(prefix);
            result.put(String.valueOf(cost));
            result.put("$");
            byte[] salt = generateSalt(32);
            result.put(Base64.getEncoder().encodeToString(salt));
            result.put("$");
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            PBEKeySpec keySpec = new PBEKeySpec(data.getChars(), salt, cost, PBKDF2_KEY_LENGTH);
            result.put(Base64.getEncoder().encodeToString(secretKeyFactory.generateSecret(keySpec).getEncoded()));
            return result.array();
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            throw new ElasticsearchException("Error using PBKDF2 for password hashing", e);
        } catch (Error e) {
            // Security Providers might throw a subclass of Error in FIPS 140 mode, if some prerequisite like
            // salt, iv, or password length is not met. We catch this because we don't want the JVM to exit.
            throw new ElasticsearchException("Error using PBKDF2 implementation from the selected Security Provider", e);
        }
    }

    private static int parsePbkdf2Iterations(char[] hash, String prefix) {
        int separator = -1;
        for (int i = prefix.length(); i < hash.length; i++) {
            if (hash[i] == '$') {
                separator = i;
                break;
            }
        }
        if (separator == -1) {
            throw new IllegalArgumentException("The number of iterations could not be determined from the provided PBKDF2 hash.");
        }
        try {
            return Integer.parseInt(new String(hash, prefix.length(), separator - prefix.length()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The number of iterations could not be determined from the provided PBKDF2 hash.", e);
        }
    }

    /**
     * Hashes the given clear text password {@code data} and compares it against the given {@code hash}.
     *
     * <p>
     * <b>Note:</b> When verifying the password hashes we dynamically determine the key and salt lengths for user provided hashes,
     * but for hashes we create we always use the get method to create hashes with fixed key {@link #PBKDF2_KEY_LENGTH}
     * and fixed salt lengths.
     *
     * @param data the clear text password to hash and verify
     * @param hash the stored hash against to verify password
     * @param iterations the number of iterations for PBKDF2 function
     * @param prefix the PBKDF2 hash prefix
     * @return {@code true} if password data matches given hash after hashing it, otherwise {@code false}
     */
    private static boolean verifyPbkdf2Hash(final SecureString data, final char[] hash, final int iterations, final String prefix) {
        if (CharArrays.charsBeginsWith(prefix, hash) == false) {
            return false;
        }

        int separator1 = -1, separator2 = -1;
        for (int i = prefix.length() + String.valueOf(iterations).length(); i < hash.length; i++) {
            if (hash[i] == '$') {
                separator1 = i;
                break;
            }
        }
        if (separator1 == -1) {
            return false;
        }
        for (int i = separator1 + 1; i < hash.length; i++) {
            if (hash[i] == '$') {
                separator2 = i;
                break;
            }
        }
        if (separator2 == -1) {
            return false;
        }

        char[] hashChars = null;
        char[] saltChars = null;
        try {
            hashChars = Arrays.copyOfRange(hash, separator2 + 1, hash.length);
            saltChars = Arrays.copyOfRange(hash, separator1 + 1, separator2);
            // Convert from base64 (n * 3/4) to the nearest multiple of 16 bytes (/16 * 16) to bits (*8)
            // (n * 3/4) / 16 * 16 * 8 ==> n * 3 / 64 * 128
            final int keySize = (hashChars.length * 3) / 64 * 128;
            return verifyPbkdf2Hash(data, iterations, keySize, saltChars, hashChars);
        } finally {
            if (null != hashChars) {
                Arrays.fill(hashChars, '\u0000');
            }
            if (null != saltChars) {
                Arrays.fill(saltChars, '\u0000');
            }
        }
    }

    private static boolean verifyPbkdf2Hash(SecureString data, int iterations, int keyLength, char[] saltChars, char[] hashChars) {
        if (keyLength <= 0 || keyLength % HMAC_SHA512_BLOCK_SIZE_IN_BITS != 0) {
            throw new ElasticsearchException(
                "PBKDF2 key length must be positive and multiple of [" + HMAC_SHA512_BLOCK_SIZE_IN_BITS + " bits]"
            );
        }
        final byte[] saltBytes = Base64.getDecoder().decode(CharArrays.toUtf8Bytes(saltChars));
        if (saltBytes.length < PBKDF2_MIN_SALT_LENGTH_IN_BYTES) {
            throw new ElasticsearchException("PBKDF2 salt must be at least [" + PBKDF2_MIN_SALT_LENGTH_IN_BYTES + " bytes] long");
        }
        char[] computedPwdHash = null;
        try {
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            PBEKeySpec keySpec = new PBEKeySpec(data.getChars(), saltBytes, iterations, keyLength);
            computedPwdHash = CharArrays.utf8BytesToChars(
                Base64.getEncoder().encode(secretKeyFactory.generateSecret(keySpec).getEncoded())
            );
            final boolean result = CharArrays.constantTimeEquals(computedPwdHash, hashChars);
            return result;
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            throw new ElasticsearchException("Error using PBKDF2 for password hashing", e);
        } catch (Error e) {
            // Security Providers might throw a subclass of Error in FIPS 140 mode, if some prerequisite like
            // salt, iv, or password length is not met. We catch this because we don't want the JVM to exit.
            throw new ElasticsearchException("Error using PBKDF2 implementation from the selected Security Provider", e);
        } finally {
            if (null != computedPwdHash) {
                Arrays.fill(computedPwdHash, '\u0000');
            }
        }
    }

    private static boolean verifyBcryptHash(SecureString text, char[] hash) {
        String hashStr = new String(hash);
        if (isBcryptPrefix(hash) == false) {
            return false;
        }
        return BCrypt.checkpw(text, hashStr);
    }

    /**
     * Returns a list of lower case String identifiers for the Hashing algorithm and parameter
     * combinations that can be used for password hashing. The identifiers can be used to get
     * an instance of the appropriate {@link Hasher} by using {@link #resolve(String) resolve()}
     */
    @SuppressForbidden(reason = "This is the only allowed way to get available values")
    public static List<String> getAvailableAlgoStoredPasswordHash() {
        return Arrays.stream(Hasher.values())
            .map(Hasher::name)
            .map(name -> name.toLowerCase(Locale.ROOT))
            .filter(name -> (name.startsWith("pbkdf2") || name.startsWith("bcrypt")))
            .collect(Collectors.toList());
    }

    /**
     * Returns a list of lower case String identifiers for the Hashing algorithm and parameter
     * combinations that can be used for secure token hashing. The identifiers can be used to get
     * an instance of the appropriate {@link Hasher} by using {@link #resolve(String) resolve()}
     */
    @SuppressForbidden(reason = "This is the only allowed way to get available values")
    public static List<String> getAvailableAlgoStoredSecureTokenHash() {
        return Arrays.stream(Hasher.values())
            .map(Hasher::name)
            .map(name -> name.toLowerCase(Locale.ROOT))
            .filter(name -> (name.startsWith("pbkdf2") || name.startsWith("bcrypt") || name.equals("ssha256")))
            .collect(Collectors.toList());
    }

    /**
     * Returns a list of lower case String identifiers for the Hashing algorithm and parameter
     * combinations that can be used for password hashing in the cache. The identifiers can be used to get
     * an instance of the appropriate {@link Hasher} by using {@link #resolve(String) resolve()}
     */
    @SuppressForbidden(reason = "This is the only allowed way to get available values")
    public static List<String> getAvailableAlgoCacheHash() {
        return Arrays.stream(Hasher.values())
            .map(Hasher::name)
            .map(name -> name.toLowerCase(Locale.ROOT))
            .filter(name -> (name.equals("sha256") == false))
            .collect(Collectors.toList());
    }

    public abstract char[] hash(SecureString data);

    public abstract boolean verify(SecureString data, char[] hash);

    /**
     * Generates an array of {@code length} random bytes using {@link java.security.SecureRandom}
     */
    private static byte[] generateSalt(int length) {
        byte[] salt = new byte[length];
        SECURE_RANDOM.nextBytes(salt);
        return salt;
    }

    private static char[] hashSha512(SecureString text) {
        MessageDigest md = MessageDigests.sha512();
        md.update(CharArrays.toUtf8Bytes(text.getChars()));
        return MessageDigests.toHexCharArray(md.digest());
    }
}
