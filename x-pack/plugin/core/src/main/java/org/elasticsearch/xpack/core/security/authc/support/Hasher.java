/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SecureString;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
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
            return getPbkdf2Hash(data, PBKDF2_DEFAULT_COST);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash);
        }

    },

    PBKDF2_1000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 1000);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash);
        }

    },

    PBKDF2_10000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 10000);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash);
        }

    },

    PBKDF2_50000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 50000);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash);
        }

    },

    PBKDF2_100000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 100000);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash);
        }

    },

    PBKDF2_500000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 500000);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash);
        }

    },

    PBKDF2_1000000() {
        @Override
        public char[] hash(SecureString data) {
            return getPbkdf2Hash(data, 1000000);
        }

        @Override
        public boolean verify(SecureString data, char[] hash) {
            return verifyPbkdf2Hash(data, hash);
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
            return Base64.getUrlEncoder().withoutPadding().encodeToString(md.digest()).toCharArray();
        }

        @Override
        public boolean verify(SecureString text, char[] hash) {
            MessageDigest md = MessageDigests.sha256();
            md.update(CharArrays.toUtf8Bytes(text.getChars()));
            return CharArrays.constantTimeEquals(Base64.getUrlEncoder().withoutPadding().encodeToString(md.digest()).toCharArray(), hash);
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
    private static final String PBKDF2_PREFIX = "{PBKDF2}";
    private static final int PBKDF2_DEFAULT_COST = 10000;
    private static final int PBKDF2_KEY_LENGTH = 256;
    private static final int BCRYPT_DEFAULT_COST = 10;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    /**
     * Returns a {@link Hasher} instance of the appropriate algorithm and associated cost as
     * indicated by the {@code name}. Name identifiers for the default costs for
     * BCRYPT and PBKDF2 return the he default BCRYPT and PBKDF2 Hasher instead of the specific
     * instances for the associated cost.
     *
     * @param name The name of the algorithm and cost combination identifier
     * @return the hasher associated with the identifier
     */
    public static Hasher resolve(String name) {
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
            case "bcrypt10":
                return BCRYPT;
            case "bcrypt11":
                return BCRYPT11;
            case "bcrypt12":
                return BCRYPT12;
            case "bcrypt13":
                return BCRYPT13;
            case "bcrypt14":
                return BCRYPT14;
            case "pbkdf2":
                return PBKDF2;
            case "pbkdf2_1000":
                return PBKDF2_1000;
            case "pbkdf2_10000":
                return PBKDF2;
            case "pbkdf2_50000":
                return PBKDF2_50000;
            case "pbkdf2_100000":
                return PBKDF2_100000;
            case "pbkdf2_500000":
                return PBKDF2_500000;
            case "pbkdf2_1000000":
                return PBKDF2_1000000;
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
                throw new IllegalArgumentException("unknown hash function [" + name + "]");
        }
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
        if (CharArrays.charsBeginsWith(BCRYPT_PREFIX, hash)) {
            int cost = Integer.parseInt(new String(Arrays.copyOfRange(hash, BCRYPT_PREFIX.length(), hash.length - 54)));
            return cost == BCRYPT_DEFAULT_COST ? Hasher.BCRYPT : resolve("bcrypt" + cost);
        } else if (CharArrays.charsBeginsWith(PBKDF2_PREFIX, hash)) {
            int cost = Integer.parseInt(new String(Arrays.copyOfRange(hash, PBKDF2_PREFIX.length(), hash.length - 90)));
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

    private static char[] getPbkdf2Hash(SecureString data, int cost) {
        try {
            // Base64 string length : (4*(n/3)) rounded up to the next multiple of 4 because of padding.
            // n is 32 (PBKDF2_KEY_LENGTH in bytes) and 2 is because of the dollar sign delimiters.
            CharBuffer result = CharBuffer.allocate(PBKDF2_PREFIX.length() + String.valueOf(cost).length() + 2 + 44 + 44);
            result.put(PBKDF2_PREFIX);
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
            throw new ElasticsearchException("Can't use PBKDF2 for password hashing", e);
        }
    }

    private static boolean verifyPbkdf2Hash(SecureString data, char[] hash) {
        // Base64 string length : (4*(n/3)) rounded up to the next multiple of 4 because of padding.
        // n is 32 (PBKDF2_KEY_LENGTH in bytes), so tokenLength is 44
        final int tokenLength = 44;
        char[] hashChars = null;
        char[] saltChars = null;
        char[] computedPwdHash = null;
        try {
            if (CharArrays.charsBeginsWith(PBKDF2_PREFIX, hash) == false) {
                return false;
            }
            hashChars = Arrays.copyOfRange(hash, hash.length - tokenLength, hash.length);
            saltChars = Arrays.copyOfRange(hash, hash.length - (2 * tokenLength + 1), hash.length - (tokenLength + 1));
            int cost = Integer.parseInt(new String(Arrays.copyOfRange(hash, PBKDF2_PREFIX.length(), hash.length - (2 * tokenLength + 2))));
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            PBEKeySpec keySpec = new PBEKeySpec(data.getChars(), Base64.getDecoder().decode(CharArrays.toUtf8Bytes(saltChars)),
                cost, PBKDF2_KEY_LENGTH);
            computedPwdHash = CharArrays.utf8BytesToChars(Base64.getEncoder()
                .encode(secretKeyFactory.generateSecret(keySpec).getEncoded()));
            final boolean result = CharArrays.constantTimeEquals(computedPwdHash, hashChars);
            return result;
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            throw new ElasticsearchException("Can't use PBKDF2 for password hashing", e);
        } finally {
            if (null != hashChars) {
                Arrays.fill(hashChars, '\u0000');
            }
            if (null != saltChars) {
                Arrays.fill(saltChars, '\u0000');
            }
            if (null != computedPwdHash) {
                Arrays.fill(computedPwdHash, '\u0000');
            }
        }
    }

    private static boolean verifyBcryptHash(SecureString text, char[] hash) {
        String hashStr = new String(hash);
        if (hashStr.startsWith(BCRYPT_PREFIX) == false) {
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
    public static List<String> getAvailableAlgoStoredHash() {
        return Arrays.stream(Hasher.values()).map(Hasher::name).map(name -> name.toLowerCase(Locale.ROOT))
            .filter(name -> (name.startsWith("pbkdf2") || name.startsWith("bcrypt")))
            .collect(Collectors.toList());
    }

    /**
     * Returns a list of lower case String identifiers for the Hashing algorithm and parameter
     * combinations that can be used for password hashing in the cache. The identifiers can be used to get
     * an instance of the appropriate {@link Hasher} by using {@link #resolve(String) resolve()}
     */
    @SuppressForbidden(reason = "This is the only allowed way to get available values")
    public static List<String> getAvailableAlgoCacheHash() {
        return Arrays.stream(Hasher.values()).map(Hasher::name).map(name -> name.toLowerCase(Locale.ROOT))
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
}
