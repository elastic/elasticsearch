/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.hash.MessageDigests;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A disk based container for sensitive settings in Elasticsearch.
 *
 * Loading a keystore has 2 phases. First, call {@link #load(Path)}. Then call
 * {@link #decrypt(char[])} with the keystore password, or an empty char array if
 * {@link #hasPassword()} is {@code false}.  Loading and decrypting should happen
 * in a single thread. Once decrypted, settings may be read in multiple threads.
 */
public class KeyStoreWrapper implements SecureSettings {

    /** An identifier for the type of data that may be stored in a keystore entry. */
    private enum EntryType {
        STRING,
        FILE
    }

    /** An entry in the keystore. The bytes are opaque and interpreted based on the entry type. */
    private static class Entry {
        final byte[] bytes;
        final byte[] sha256Digest;

        Entry(byte[] bytes) {
            this.bytes = bytes;
            this.sha256Digest = MessageDigests.sha256().digest(bytes);
        }
    }

    /**
     * A regex for the valid characters that a setting name in the keystore may use.
     */
    private static final Pattern ALLOWED_SETTING_NAME = Pattern.compile("[A-Za-z0-9_\\-.]+");

    public static final Setting<SecureString> SEED_SETTING = SecureSetting.secureString("keystore.seed", null);

    /** Characters that may be used in the bootstrap seed setting added to all keystores. */
    private static final char[] SEED_CHARS = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789" +
        "~!@#$%^&*-_=+?").toCharArray();

    /** The name of the keystore file to read and write. */
    private static final String KEYSTORE_FILENAME = "elasticsearch.keystore";

    /** The version of the metadata written before the keystore data. */
    static final int FORMAT_VERSION = 4;

    /** The oldest metadata format version that can be read. */
    private static final int MIN_FORMAT_VERSION = 1;

    /** The algorithm used to derive the cipher key from a password. */
    private static final String KDF_ALGO = "PBKDF2WithHmacSHA512";

    /** The number of iterations to derive the cipher key. */
    private static final int KDF_ITERS = 10000;

    /**
     * The number of bits for the cipher key.
     *
     * Note: The Oracle JDK 8 ships with a limited JCE policy that restricts key length for AES to 128 bits.
     * This can be increased to 256 bits once minimum java 9 is the minimum java version.
     * See http://www.oracle.com/technetwork/java/javase/terms/readme/jdk9-readme-3852447.html#jce
     * */
    private static final int CIPHER_KEY_BITS = 128;

    /** The number of bits for the GCM tag. */
    private static final int GCM_TAG_BITS = 128;

    /** The cipher used to encrypt the keystore data. */
    private static final String CIPHER_ALGO = "AES";

    /** The mode used with the cipher algorithm. */
    private static final String CIPHER_MODE = "GCM";

    /** The padding used with the cipher algorithm. */
    private static final String CIPHER_PADDING = "NoPadding";

    // format version changelog:
    // 1: initial version, ES 5.3
    // 2: file setting, ES 5.4
    // 3: FIPS compliant algos, ES 6.3
    // 4: remove distinction between string/files, ES 6.8/7.1

    /** The metadata format version used to read the current keystore wrapper. */
    private final int formatVersion;

    /** True iff the keystore has a password needed to read. */
    private final boolean hasPassword;

    /** The raw bytes of the encrypted keystore. */
    private final byte[] dataBytes;

    /** The decrypted secret data. See {@link #decrypt(char[])}. */
    private final SetOnce<Map<String, Entry>> entries = new SetOnce<>();
    private volatile boolean closed;

    private KeyStoreWrapper(int formatVersion, boolean hasPassword, byte[] dataBytes) {
        this.formatVersion = formatVersion;
        this.hasPassword = hasPassword;
        this.dataBytes = dataBytes;
    }

    /**
     * Get the metadata format version for the keystore
     **/
    public int getFormatVersion() {
        return formatVersion;
    }

    /** Returns a path representing the ES keystore in the given config dir. */
    public static Path keystorePath(Path configDir) {
        return configDir.resolve(KEYSTORE_FILENAME);
    }

    /** Constructs a new keystore with the given password. */
    public static KeyStoreWrapper create() {
        KeyStoreWrapper wrapper = new KeyStoreWrapper(FORMAT_VERSION, false, null);
        wrapper.entries.set(new HashMap<>());
        addBootstrapSeed(wrapper);
        return wrapper;
    }

    /** Add the bootstrap seed setting, which may be used as a unique, secure, random value by the node */
    public static void addBootstrapSeed(KeyStoreWrapper wrapper) {
        assert wrapper.getSettingNames().contains(SEED_SETTING.getKey()) == false;
        SecureRandom random = Randomness.createSecure();
        int passwordLength = 20; // Generate 20 character passwords
        char[] characters = new char[passwordLength];
        for (int i = 0; i < passwordLength; ++i) {
            characters[i] = SEED_CHARS[random.nextInt(SEED_CHARS.length)];
        }
        wrapper.setString(SEED_SETTING.getKey(), characters);
        Arrays.fill(characters, (char)0);
    }

    /**
     * Loads information about the Elasticsearch keystore from the provided config directory.
     *
     * {@link #decrypt(char[])} must be called before reading or writing any entries.
     * Returns {@code null} if no keystore exists.
     */
    public static KeyStoreWrapper load(Path configDir) throws IOException {
        Path keystoreFile = keystorePath(configDir);
        if (Files.exists(keystoreFile) == false) {
            return null;
        }

        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        try (IndexInput indexInput = directory.openInput(KEYSTORE_FILENAME, IOContext.READONCE)) {
            ChecksumIndexInput input = new BufferedChecksumIndexInput(indexInput);
            final int formatVersion;
            try {
                formatVersion = CodecUtil.checkHeader(input, KEYSTORE_FILENAME, MIN_FORMAT_VERSION, FORMAT_VERSION);
            } catch (IndexFormatTooOldException e) {
                throw new IllegalStateException("The Elasticsearch keystore [" + keystoreFile + "] format is too old. " +
                    "You should delete and recreate it in order to upgrade.", e);
            } catch (IndexFormatTooNewException e) {
                throw new IllegalStateException("The Elasticsearch keystore [" + keystoreFile + "] format is too new. " +
                    "Are you trying to downgrade? You should delete and recreate it in order to downgrade.", e);
            }
            byte hasPasswordByte = input.readByte();
            boolean hasPassword = hasPasswordByte == 1;
            if (hasPassword == false && hasPasswordByte != 0) {
                throw new IllegalStateException("hasPassword boolean is corrupt: "
                    + String.format(Locale.ROOT, "%02x", hasPasswordByte));
            }

            if (formatVersion <= 2) {
                String type = input.readString();
                if (type.equals("PKCS12") == false) {
                    throw new IllegalStateException("Corrupted legacy keystore string encryption algorithm");
                }

                final String stringKeyAlgo = input.readString();
                if (stringKeyAlgo.equals("PBE") == false) {
                    throw new IllegalStateException("Corrupted legacy keystore string encryption algorithm");
                }
                if (formatVersion == 2) {
                    final String fileKeyAlgo = input.readString();
                    if (fileKeyAlgo.equals("PBE") == false) {
                        throw new IllegalStateException("Corrupted legacy keystore file encryption algorithm");
                    }
                }
            }

            final byte[] dataBytes;
            if (formatVersion == 2) {
                // For v2 we had a map of strings containing the types for each setting. In v3 this map is now
                // part of the encrypted bytes. Unfortunately we cannot seek backwards with checksum input, so
                // we cannot just read the map and find out how long it is. So instead we read the map and
                // store it back using java's builtin DataOutput in a byte array, along with the actual keystore bytes
                Map<String, String> settingTypes = input.readMapOfStrings();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (DataOutputStream output = new DataOutputStream(bytes)) {
                    output.writeInt(settingTypes.size());
                    for (Map.Entry<String, String> entry : settingTypes.entrySet()) {
                        output.writeUTF(entry.getKey());
                        output.writeUTF(entry.getValue());
                    }
                    int keystoreLen = input.readInt();
                    byte[] keystoreBytes = new byte[keystoreLen];
                    input.readBytes(keystoreBytes, 0, keystoreLen);
                    output.write(keystoreBytes);
                }
                dataBytes = bytes.toByteArray();
            } else {
                int dataBytesLen = input.readInt();
                dataBytes = new byte[dataBytesLen];
                input.readBytes(dataBytes, 0, dataBytesLen);
            }

            CodecUtil.checkFooter(input);
            return new KeyStoreWrapper(formatVersion, hasPassword, dataBytes);
        }
    }

    /** Upgrades the format of the keystore, if necessary. */
    public static void upgrade(KeyStoreWrapper wrapper, Path configDir, char[] password) throws Exception {
        if (wrapper.getFormatVersion() == FORMAT_VERSION && wrapper.getSettingNames().contains(SEED_SETTING.getKey())) {
            return;
        }
        // add keystore.seed if necessary
        if (wrapper.getSettingNames().contains(SEED_SETTING.getKey()) == false) {
            addBootstrapSeed(wrapper);
        }
        wrapper.save(configDir, password);
    }

    @Override
    public boolean isLoaded() {
        return entries.get() != null;
    }

    /** Return true iff calling {@link #decrypt(char[])} requires a non-empty password. */
    public boolean hasPassword() {
        return hasPassword;
    }

    private Cipher createCipher(int opmode, char[] password, byte[] salt, byte[] iv) throws GeneralSecurityException {
        PBEKeySpec keySpec = new PBEKeySpec(password, salt, KDF_ITERS, CIPHER_KEY_BITS);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KDF_ALGO);
        SecretKey secretKey = keyFactory.generateSecret(keySpec);
        SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), CIPHER_ALGO);

        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_BITS, iv);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGO + "/" + CIPHER_MODE + "/" + CIPHER_PADDING);
        cipher.init(opmode, secret, spec);
        cipher.updateAAD(salt);
        return cipher;
    }

    /**
     * Decrypts the underlying keystore data.
     *
     * This may only be called once.
     */
    public void decrypt(char[] password) throws GeneralSecurityException, IOException {
        if (entries.get() != null) {
            throw new IllegalStateException("Keystore has already been decrypted");
        }
        if (formatVersion <= 2) {
            decryptLegacyEntries();
            if (password.length != 0) {
                throw new IllegalArgumentException("Keystore format does not accept non-empty passwords");
            }
            return;
        }

        final byte[] salt;
        final byte[] iv;
        final byte[] encryptedBytes;
        try (ByteArrayInputStream bytesStream = new ByteArrayInputStream(dataBytes);
             DataInputStream input = new DataInputStream(bytesStream)) {
            int saltLen = input.readInt();
            salt = new byte[saltLen];
            input.readFully(salt);
            int ivLen = input.readInt();
            iv = new byte[ivLen];
            input.readFully(iv);
            int encryptedLen = input.readInt();
            encryptedBytes = new byte[encryptedLen];
            input.readFully(encryptedBytes);
            if (input.read() != -1) {
                throw new SecurityException("Keystore has been corrupted or tampered with");
            }
        } catch (EOFException e) {
            throw new SecurityException("Keystore has been corrupted or tampered with", e);
        }

        Cipher cipher = createCipher(Cipher.DECRYPT_MODE, password, salt, iv);
        try (ByteArrayInputStream bytesStream = new ByteArrayInputStream(encryptedBytes);
             CipherInputStream cipherStream = new CipherInputStream(bytesStream, cipher);
             DataInputStream input = new DataInputStream(cipherStream)) {
            entries.set(new HashMap<>());
            int numEntries = input.readInt();
            while (numEntries-- > 0) {
                String setting = input.readUTF();
                if (formatVersion == 3) {
                    // legacy, the keystore format would previously store the entry type
                    input.readUTF();
                }
                int entrySize = input.readInt();
                byte[] entryBytes = new byte[entrySize];
                input.readFully(entryBytes);
                entries.get().put(setting, new Entry(entryBytes));
            }
            if (input.read() != -1) {
                throw new SecurityException("Keystore has been corrupted or tampered with");
            }
        } catch (IOException e) {
            throw new SecurityException("Keystore has been corrupted or tampered with", e);
        }
    }

    /** Encrypt the keystore entries and return the encrypted data. */
    private byte[] encrypt(char[] password, byte[] salt, byte[] iv) throws GeneralSecurityException, IOException {
        assert isLoaded();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Cipher cipher = createCipher(Cipher.ENCRYPT_MODE, password, salt, iv);
        try (CipherOutputStream cipherStream = new CipherOutputStream(bytes, cipher);
             DataOutputStream output = new DataOutputStream(cipherStream)) {
            output.writeInt(entries.get().size());
            for (Map.Entry<String, Entry> mapEntry : entries.get().entrySet()) {
                output.writeUTF(mapEntry.getKey());
                byte[] entryBytes = mapEntry.getValue().bytes;
                output.writeInt(entryBytes.length);
                output.write(entryBytes);
            }
        }
        return bytes.toByteArray();
    }

    private void decryptLegacyEntries() throws GeneralSecurityException, IOException {
        // v1 and v2 keystores never had passwords actually used, so we always use an empty password
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        Map<String, EntryType> settingTypes = new HashMap<>();
        ByteArrayInputStream inputBytes = new ByteArrayInputStream(dataBytes);
        try (DataInputStream input = new DataInputStream(inputBytes)) {
            // first read the setting types map
            if (formatVersion == 2) {
                int numSettings = input.readInt();
                for (int i = 0; i < numSettings; ++i) {
                    String key = input.readUTF();
                    String value = input.readUTF();
                    settingTypes.put(key, EntryType.valueOf(value));
                }
            }
            // then read the actual keystore
            keystore.load(input, "".toCharArray());
        }

        // verify the settings metadata matches the keystore entries
        Enumeration<String> aliases = keystore.aliases();
        if (formatVersion == 1) {
            while (aliases.hasMoreElements()) {
                settingTypes.put(aliases.nextElement(), EntryType.STRING);
            }
        } else {
            // verify integrity: keys in keystore match what the metadata thinks exist
            Set<String> expectedSettings = new HashSet<>(settingTypes.keySet());
            while (aliases.hasMoreElements()) {
                String settingName = aliases.nextElement();
                if (expectedSettings.remove(settingName) == false) {
                    throw new SecurityException("Keystore has been corrupted or tampered with");
                }
            }
            if (expectedSettings.isEmpty() == false) {
                throw new SecurityException("Keystore has been corrupted or tampered with");
            }
        }

        // fill in the entries now that we know all the types to expect
        this.entries.set(new HashMap<>());
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBE");
        KeyStore.PasswordProtection password = new KeyStore.PasswordProtection("".toCharArray());

        for (Map.Entry<String, EntryType> settingEntry : settingTypes.entrySet()) {
            String setting = settingEntry.getKey();
            EntryType settingType = settingEntry.getValue();
            KeyStore.SecretKeyEntry keystoreEntry = (KeyStore.SecretKeyEntry) keystore.getEntry(setting, password);
            PBEKeySpec keySpec = (PBEKeySpec) keyFactory.getKeySpec(keystoreEntry.getSecretKey(), PBEKeySpec.class);
            char[] chars = keySpec.getPassword();
            keySpec.clearPassword();

            final byte[] bytes;
            if (settingType == EntryType.STRING) {
                ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(CharBuffer.wrap(chars));
                bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
                Arrays.fill(byteBuffer.array(), (byte)0);
            } else {
                assert settingType == EntryType.FILE;
                // The PBE keyspec gives us chars, we convert to bytes
                byte[] tmpBytes = new byte[chars.length];
                for (int i = 0; i < tmpBytes.length; ++i) {
                    tmpBytes[i] = (byte)chars[i]; // PBE only stores the lower 8 bits, so this narrowing is ok
                }
                bytes = Base64.getDecoder().decode(tmpBytes);
                Arrays.fill(tmpBytes, (byte)0);
            }
            Arrays.fill(chars, '\0');

            entries.get().put(setting, new Entry(bytes));
        }
    }

    /** Write the keystore to the given config directory. */
    public synchronized void save(Path configDir, char[] password) throws Exception {
        ensureOpen();

        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        // write to tmp file first, then overwrite
        String tmpFile = KEYSTORE_FILENAME + ".tmp";
        try (IndexOutput output = directory.createOutput(tmpFile, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, KEYSTORE_FILENAME, FORMAT_VERSION);
            output.writeByte(password.length == 0 ? (byte)0 : (byte)1);

            // new cipher params
            SecureRandom random = Randomness.createSecure();
            // use 64 bytes salt, which surpasses that recommended by OWASP
            // see https://www.owasp.org/index.php/Password_Storage_Cheat_Sheet
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            // use 96 bits (12 bytes) for IV as recommended by NIST
            // see http://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf section 5.2.1.1
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            // encrypted data
            byte[] encryptedBytes = encrypt(password, salt, iv);

            // size of data block
            output.writeInt(4 + salt.length + 4 + iv.length + 4 + encryptedBytes.length);

            output.writeInt(salt.length);
            output.writeBytes(salt, salt.length);
            output.writeInt(iv.length);
            output.writeBytes(iv, iv.length);
            output.writeInt(encryptedBytes.length);
            output.writeBytes(encryptedBytes, encryptedBytes.length);

            CodecUtil.writeFooter(output);

        } catch (final AccessDeniedException e) {
            final String message = String.format(
                Locale.ROOT,
                "unable to create temporary keystore at [%s], write permissions required for [%s] or run [elasticsearch-keystore upgrade]",
                configDir.resolve(tmpFile),
                configDir);
            throw new UserException(ExitCodes.CONFIG, message, e);
        }

        Path keystoreFile = keystorePath(configDir);
        Files.move(configDir.resolve(tmpFile), keystoreFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        PosixFileAttributeView attrs = Files.getFileAttributeView(keystoreFile, PosixFileAttributeView.class);
        if (attrs != null) {
            // don't rely on umask: ensure the keystore has minimal permissions
            attrs.setPermissions(PosixFilePermissions.fromString("rw-rw----"));
        }
    }

    /**
     * It is possible to retrieve the setting names even if the keystore is closed.
     * This allows {@link SecureSetting} to correctly determine that a entry exists even though it cannot be read. Thus attempting to
     * read a secure setting after the keystore is closed will generate a "keystore is closed" exception rather than using the fallback
     * setting.
     */
    @Override
    public Set<String> getSettingNames() {
        assert entries.get() != null : "Keystore is not loaded";
        return entries.get().keySet();
    }

    // TODO: make settings accessible only to code that registered the setting
    @Override
    public synchronized SecureString getString(String setting) {
        ensureOpen();
        Entry entry = entries.get().get(setting);
        ByteBuffer byteBuffer = ByteBuffer.wrap(entry.bytes);
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);
        return new SecureString(Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit()));
    }

    @Override
    public synchronized InputStream getFile(String setting) {
        ensureOpen();
        Entry entry = entries.get().get(setting);
        return new ByteArrayInputStream(entry.bytes);
    }

    /**
     * Returns the SHA256 digest for the setting's value, even after {@code #close()} has been called. The setting must exist. The digest is
     * used to check for value changes without actually storing the value.
     */
    @Override
    public byte[] getSHA256Digest(String setting) {
        assert entries.get() != null : "Keystore is not loaded";
        Entry entry = entries.get().get(setting);
        return entry.sha256Digest;
    }

    /**
     * Ensure the given setting name is allowed.
     *
     * @throws IllegalArgumentException if the setting name is not valid
     */
    public static void validateSettingName(String setting) {
        if (ALLOWED_SETTING_NAME.matcher(setting).matches() == false) {
            throw new IllegalArgumentException("Setting name [" + setting + "] does not match the allowed setting name pattern ["
                + ALLOWED_SETTING_NAME.pattern() + "]");
        }
    }

    /** Set a string setting. */
    synchronized void setString(String setting, char[] value) {
        ensureOpen();
        validateSettingName(setting);

        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(CharBuffer.wrap(value));
        byte[] bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Entry oldEntry = entries.get().put(setting, new Entry(bytes));
        if (oldEntry != null) {
            Arrays.fill(oldEntry.bytes, (byte)0);
        }
    }

    /** Set a file setting. */
    synchronized void setFile(String setting, byte[] bytes) {
        ensureOpen();
        validateSettingName(setting);

        Entry oldEntry = entries.get().put(setting, new Entry(Arrays.copyOf(bytes, bytes.length)));
        if (oldEntry != null) {
            Arrays.fill(oldEntry.bytes, (byte)0);
        }
    }

    /** Remove the given setting from the keystore. */
    void remove(String setting) {
        ensureOpen();
        Entry oldEntry = entries.get().remove(setting);
        if (oldEntry != null) {
            Arrays.fill(oldEntry.bytes, (byte)0);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Keystore is closed");
        }
        assert isLoaded() : "Keystore is not loaded";
    }

    @Override
    public synchronized void close() {
        this.closed = true;
        if (null != entries.get() && entries.get().isEmpty() == false) {
            for (Entry entry : entries.get().values()) {
                Arrays.fill(entry.bytes, (byte) 0);
            }
        }
    }
}
