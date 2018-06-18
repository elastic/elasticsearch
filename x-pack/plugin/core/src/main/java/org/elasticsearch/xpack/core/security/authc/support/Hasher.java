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
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Random;
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
            String salt = BCrypt.gensalt(4);
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
            String salt = BCrypt.gensalt(5);
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
            String salt = BCrypt.gensalt(6);
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
            String salt = BCrypt.gensalt(7);
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
            String salt = BCrypt.gensalt(8);
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
            String salt = BCrypt.gensalt(9);
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

    BCRYPT10() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(10);
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

    BCRYPT11() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(11);
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

    BCRYPT12() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(12);
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

    BCRYPT13() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(13);
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

    BCRYPT14() {
        @Override
        public char[] hash(SecureString text) {
            String salt = BCrypt.gensalt(14);
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
    private static final String PBKDF2_PREFIX = "{PBKDF2}";
    private static final int PBKDF2_DEFAULT_COST = 10000;

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
            case "bcrypt10":
                return BCRYPT10;
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
                return PBKDF2_10000;
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
                return defaultHasher;
        }
    }

    public static Hasher resolveFromHash(char[] hash) {
        String hashString = new String(hash);
        if (hashString.startsWith(BCRYPT_PREFIX)) {
            return Hasher.BCRYPT;
        } else if (hashString.startsWith(PBKDF2_PREFIX)) {
            return Hasher.PBKDF2;
        } else if (hashString.startsWith(SHA1_PREFIX)) {
            return Hasher.SHA1;
        } else if (hashString.startsWith(MD5_PREFIX)) {
            return Hasher.MD5;
        } else if (hashString.startsWith(SSHA256_PREFIX)) {
            return Hasher.SSHA256;
        } else {
            throw new IllegalArgumentException("unknown hash format for hash [" + hashString + "]");
        }
    }

    public static boolean verifyHash(SecureString data, char[] hash) {
        try {
            final Hasher hasher = resolveFromHash(hash);
            return hasher.verify(data, hash);
        } catch (IllegalArgumentException e) {
            // The password hash format is invalid, we're unable to verify password
            return false;
        }
    }

    private static char[] getPbkdf2Hash(SecureString data, int cost) {
        try {
            // Base64 string length : ((4*n/3)+3) rounded up to the next multiple of 4 because of padding = 44
            CharBuffer result = CharBuffer.allocate(PBKDF2_PREFIX.length() + String.valueOf(cost).length() + 2 + 32 + 44);
            result.put(PBKDF2_PREFIX);
            result.put(String.valueOf(cost));
            result.put("$");
            char[] salt = SaltProvider.salt(32);  // 32 characters for 64 bytes
            result.put(salt);
            result.put("$");
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            PBEKeySpec keySpec = new PBEKeySpec(data.getChars(), CharArrays.toUtf8Bytes(salt), cost, 256);
            result.put(Base64.getEncoder().encodeToString(secretKeyFactory.generateSecret(keySpec).getEncoded()));
            return result.array();
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Can't use PKDF2 for password hashing", e);
        }
    }

    private static boolean verifyPbkdf2Hash(SecureString data, char[] hash) {
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

    public static Hasher resolve(String name) {
        Hasher hasher = resolve(name, null);
        if (hasher == null) {
            throw new IllegalArgumentException("unknown hash function [" + name + "]");
        }
        return hasher;
    }

    public static List<String> getAvailableAlgoStoredHash() {
        return Arrays.stream(Hasher.values()).map(Hasher::name).map(name -> name.toLowerCase(Locale.ROOT))
            .filter(name -> (name.startsWith("pbkdf2") || name.startsWith("bcrypt")))
            .collect(Collectors.toList());
    }

    public abstract char[] hash(SecureString data);

    public abstract boolean verify(SecureString data, char[] hash);


    static final class SaltProvider {

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
