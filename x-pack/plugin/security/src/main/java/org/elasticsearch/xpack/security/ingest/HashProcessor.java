/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.security.SecurityField;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * A processor that hashes the contents of a field (or fields) using various hashing algorithms
 */
public final class HashProcessor extends AbstractProcessor {
    public static final String TYPE = "hash";
    public static final Setting.AffixSetting<SecureString> HMAC_KEY_SETTING = SecureSetting
        .affixKeySetting(SecurityField.setting("ingest." + TYPE) + ".", "key",
            (key) -> SecureSetting.secureString(key, null));

    private final List<String> fields;
    private final String targetField;
    private final Method method;
    private final Mac mac;
    private final byte[] salt;
    private final boolean ignoreMissing;

    HashProcessor(String tag, List<String> fields, String targetField, byte[] salt, Method method, @Nullable Mac mac,
                  boolean ignoreMissing) {
        super(tag);
        this.fields = fields;
        this.targetField = targetField;
        this.method = method;
        this.mac = mac;
        this.salt = salt;
        this.ignoreMissing = ignoreMissing;
    }

    List<String> getFields() {
        return fields;
    }

    String getTargetField() {
        return targetField;
    }

    byte[] getSalt() {
        return salt;
    }

    @Override
    public void execute(IngestDocument document) {
        Map<String, String> hashedFieldValues = fields.stream().map(f -> {
            String value = document.getFieldValue(f, String.class, ignoreMissing);
            if (value == null && ignoreMissing) {
                return new Tuple<String, String>(null, null);
            }
            try {
                return new Tuple<>(f, method.hash(mac, salt, value));
            } catch (Exception e) {
                throw new IllegalArgumentException("field[" + f + "] could not be hashed", e);
            }
        }).filter(tuple -> Objects.nonNull(tuple.v1())).collect(Collectors.toMap(Tuple::v1, Tuple::v2));
        if (fields.size() == 1) {
            document.setFieldValue(targetField, hashedFieldValues.values().iterator().next());
        } else {
            document.setFieldValue(targetField, hashedFieldValues);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final Settings settings;
        private final Map<String, SecureString> secureKeys;

        public Factory(Settings settings) {
            this.settings = settings;
            this.secureKeys = new HashMap<>();
            HMAC_KEY_SETTING.getAllConcreteSettings(settings).forEach(k -> {
                secureKeys.put(k.getKey(), k.get(settings));
            });
        }

        private static Mac createMac(Method method, SecureString password, byte[] salt, int iterations) {
            try {
                SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2With" + method.getAlgorithm());
                PBEKeySpec keySpec = new PBEKeySpec(password.getChars(), salt, iterations, 128);
                byte[] pbkdf2 = secretKeyFactory.generateSecret(keySpec).getEncoded();
                Mac mac = Mac.getInstance(method.getAlgorithm());
                mac.init(new SecretKeySpec(pbkdf2, method.getAlgorithm()));
                return mac;
            } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException e) {
                throw new IllegalArgumentException("invalid settings", e);
            }
        }

        @Override
        public HashProcessor create(Map<String, Processor.Factory> registry, String processorTag, Map<String, Object> config) {
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            List<String> fields = ConfigurationUtils.readList(TYPE, processorTag, config, "fields");
            if (fields.isEmpty()) {
                throw ConfigurationUtils.newConfigurationException(TYPE, processorTag, "fields", "must specify at least one field");
            } else if (fields.stream().anyMatch(Strings::isNullOrEmpty)) {
                throw ConfigurationUtils.newConfigurationException(TYPE, processorTag, "fields",
                    "a field-name entry is either empty or null");
            }
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            String keySettingName = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "key_setting");
            SecureString key = secureKeys.get(keySettingName);
            if (key == null) {
                throw ConfigurationUtils.newConfigurationException(TYPE, processorTag, "key_setting",
                    "key [" + keySettingName + "] must match [xpack.security.ingest.hash.*.key]. It is not set");
            }
            String saltString = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "salt");
            byte[] salt = saltString.getBytes(StandardCharsets.UTF_8);
            String methodProperty = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "method", "SHA256");
            Method method = Method.fromString(processorTag, "method", methodProperty);
            int iterations = ConfigurationUtils.readIntProperty(TYPE, processorTag, config, "iterations", 5);
            Mac mac = createMac(method, key, salt, iterations);
            return new HashProcessor(processorTag, fields, targetField, salt, method, mac, ignoreMissing);
        }
    }

    enum Method {
        SHA1("HmacSHA1"),
        SHA256("HmacSHA256"),
        SHA384("HmacSHA384"),
        SHA512("HmacSHA512");

        private final String algorithm;

        Method(String algorithm) {
            this.algorithm = algorithm;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public String hash(Mac mac, byte[] salt, String input) {
            try {
                byte[] encrypted = mac.doFinal(input.getBytes(StandardCharsets.UTF_8));
                byte[] messageWithSalt = new byte[salt.length + encrypted.length];
                System.arraycopy(salt, 0, messageWithSalt, 0, salt.length);
                System.arraycopy(encrypted, 0, messageWithSalt, salt.length, encrypted.length);
                return Base64.getEncoder().encodeToString(messageWithSalt);
            } catch (IllegalStateException e) {
                throw new ElasticsearchException("error hashing data", e);
            }
        }

        public static Method fromString(String processorTag, String propertyName, String type) {
            try {
                return Method.valueOf(type.toUpperCase(Locale.ROOT));
            } catch(IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, propertyName, "type [" + type +
                    "] not supported, cannot convert field. Valid hash methods: " + Arrays.toString(Method.values()));
            }
        }
    }
}
