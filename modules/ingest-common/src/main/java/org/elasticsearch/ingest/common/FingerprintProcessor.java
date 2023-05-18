/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.Murmur3Hasher;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;

/**
 * Computes hash based on the content of selected fields in a document.
 */
public final class FingerprintProcessor extends AbstractProcessor {

    public static final String TYPE = "fingerprint";

    static final byte[] DELIMITER = new byte[] { 0 };
    static final byte[] TRUE_BYTES = new byte[] { 1 };
    static final byte[] FALSE_BYTES = new byte[] { 2 };

    private final List<String> fields;
    private final String targetField;
    private final ThreadLocal<Hasher> threadLocalHasher;
    private final byte[] salt;
    private final boolean ignoreMissing;

    FingerprintProcessor(
        String tag,
        String description,
        List<String> fields,
        String targetField,
        byte[] salt,
        ThreadLocal<Hasher> threadLocalHasher,
        boolean ignoreMissing
    ) {
        super(tag, description);
        this.fields = new ArrayList<>(fields);
        this.fields.sort(Comparator.naturalOrder());
        this.targetField = targetField;
        this.threadLocalHasher = threadLocalHasher;
        this.salt = salt;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Hasher hasher = threadLocalHasher.get();
        hasher.reset();
        hasher.update(salt);

        var values = new Stack<>();
        for (int k = fields.size() - 1; k >= 0; k--) {
            String field = fields.get(k);
            Object value = ingestDocument.getFieldValue(field, Object.class, true);
            if (value == null) {
                if (ignoreMissing) {
                    continue;
                } else {
                    throw new IllegalArgumentException("missing field [" + field + "] when calculating fingerprint");
                }
            }
            values.push(value);
        }

        if (values.size() > 0) {
            // iteratively traverse document fields
            while (values.isEmpty() == false) {
                var value = values.pop();
                if (value instanceof List<?> list) {
                    for (int k = list.size() - 1; k >= 0; k--) {
                        values.push(list.get(k));
                    }
                } else if (value instanceof Set) {
                    @SuppressWarnings("rawtypes")
                    var set = (Set<Comparable>) value;
                    // process set entries in consistent order
                    var setList = new ArrayList<>(set);
                    setList.sort(Comparator.naturalOrder());
                    for (int k = setList.size() - 1; k >= 0; k--) {
                        values.push(setList.get(k));
                    }
                } else if (value instanceof Map) {
                    var map = (Map<String, Object>) value;
                    // process map entries in consistent order
                    @SuppressWarnings("rawtypes")
                    var entryList = new ArrayList<>(map.entrySet());
                    entryList.sort(Map.Entry.comparingByKey(Comparator.naturalOrder()));
                    for (int k = entryList.size() - 1; k >= 0; k--) {
                        values.push(entryList.get(k));
                    }
                } else if (value instanceof Map.Entry<?, ?> entry) {
                    hasher.update(DELIMITER);
                    hasher.update(toBytes(entry.getKey()));
                    values.push(entry.getValue());
                } else {
                    // feed them through digest.update
                    hasher.update(DELIMITER);
                    hasher.update(toBytes(value));
                }
            }

            ingestDocument.setFieldValue(targetField, Base64.getEncoder().encodeToString(hasher.digest()));
        }

        return ingestDocument;
    }

    static byte[] toBytes(Object value) {
        if (value instanceof String string) {
            return string.getBytes(StandardCharsets.UTF_8);
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof Integer integer) {
            byte[] intBytes = new byte[4];
            ByteUtils.writeIntLE(integer, intBytes, 0);
            return intBytes;
        }
        if (value instanceof Long longValue) {
            byte[] longBytes = new byte[8];
            ByteUtils.writeLongLE(longValue, longBytes, 0);
            return longBytes;
        }
        if (value instanceof Float floatValue) {
            byte[] floatBytes = new byte[4];
            ByteUtils.writeFloatLE(floatValue, floatBytes, 0);
            return floatBytes;
        }
        if (value instanceof Double doubleValue) {
            byte[] doubleBytes = new byte[8];
            ByteUtils.writeDoubleLE(doubleValue, doubleBytes, 0);
            return doubleBytes;
        }
        if (value instanceof Boolean b) {
            return b ? TRUE_BYTES : FALSE_BYTES;
        }
        if (value instanceof ZonedDateTime zdt) {
            byte[] zoneIdBytes = zdt.getZone().getId().getBytes(StandardCharsets.UTF_8);
            byte[] zdtBytes = new byte[32 + zoneIdBytes.length];
            ByteUtils.writeIntLE(zdt.getYear(), zdtBytes, 0);
            ByteUtils.writeIntLE(zdt.getMonthValue(), zdtBytes, 4);
            ByteUtils.writeIntLE(zdt.getDayOfMonth(), zdtBytes, 8);
            ByteUtils.writeIntLE(zdt.getHour(), zdtBytes, 12);
            ByteUtils.writeIntLE(zdt.getMinute(), zdtBytes, 16);
            ByteUtils.writeIntLE(zdt.getSecond(), zdtBytes, 20);
            ByteUtils.writeIntLE(zdt.getNano(), zdtBytes, 24);
            ByteUtils.writeIntLE(zdt.getOffset().getTotalSeconds(), zdtBytes, 28);
            System.arraycopy(zoneIdBytes, 0, zdtBytes, 32, zoneIdBytes.length);
            return zdtBytes;
        }
        if (value instanceof Date date) {
            byte[] dateBytes = new byte[8];
            ByteUtils.writeLongLE(date.getTime(), dateBytes, 0);
            return dateBytes;
        }
        if (value == null) {
            return new byte[0];
        }
        throw new IllegalArgumentException("cannot convert object of type [" + value.getClass().getName() + "] to bytes");
    }

    public List<String> getFields() {
        return fields;
    }

    public String getTargetField() {
        return targetField;
    }

    public ThreadLocal<Hasher> getThreadLocalHasher() {
        return threadLocalHasher;
    }

    public byte[] getSalt() {
        return salt;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        public static final String[] SUPPORTED_DIGESTS = { "MD5", "SHA-1", "SHA-256", "SHA-512", MurmurHasher.METHOD };

        static final String DEFAULT_TARGET = "fingerprint";
        static final String DEFAULT_SALT = "";
        static final String DEFAULT_METHOD = "SHA-1";

        @Override
        public FingerprintProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            List<String> fields = ConfigurationUtils.readList(TYPE, processorTag, config, "fields");
            if (fields.size() < 1) {
                throw newConfigurationException(TYPE, processorTag, "fields", "must specify at least one field");
            }

            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", DEFAULT_TARGET);
            String salt = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "salt", DEFAULT_SALT);
            byte[] saltBytes = Strings.hasText(salt) ? toBytes(salt) : new byte[0];
            String method = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "method", DEFAULT_METHOD);
            if (Arrays.asList(SUPPORTED_DIGESTS).contains(method) == false) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "method",
                    String.format(
                        Locale.ROOT,
                        "[%s] must be one of the supported hash methods [%s]",
                        method,
                        Strings.arrayToCommaDelimitedString(SUPPORTED_DIGESTS)
                    )
                );
            }
            ThreadLocal<Hasher> threadLocalHasher = ThreadLocal.withInitial(() -> {
                try {
                    return MessageDigestHasher.getInstance(method);
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalStateException("unexpected exception creating MessageDigest instance for [" + method + "]", e);
                }
            });
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            return new FingerprintProcessor(processorTag, description, fields, targetField, saltBytes, threadLocalHasher, ignoreMissing);
        }
    }

    // simple interface around MessageDigest to facilitate testing
    public interface Hasher {

        void reset();

        void update(byte[] input);

        byte[] digest();

        String getAlgorithm();
    }

    static class MessageDigestHasher implements Hasher {

        private final MessageDigest md;

        private MessageDigestHasher(MessageDigest md) {
            this.md = md;
        }

        static Hasher getInstance(String method) throws NoSuchAlgorithmException {
            if (method.equalsIgnoreCase(MurmurHasher.METHOD)) {
                return MurmurHasher.getInstance(method);
            } else {
                MessageDigest md = MessageDigest.getInstance(method);
                return new MessageDigestHasher(md);
            }
        }

        @Override
        public void reset() {
            md.reset();
        }

        @Override
        public void update(byte[] input) {
            md.update(input);
        }

        @Override
        public byte[] digest() {
            return md.digest();
        }

        @Override
        public String getAlgorithm() {
            return md.getAlgorithm();
        }
    }

    static class MurmurHasher implements Hasher {

        public static final String METHOD = Murmur3Hasher.METHOD;
        private final Murmur3Hasher mh;

        private MurmurHasher() {
            this.mh = new Murmur3Hasher(0);
        }

        static Hasher getInstance(String method) throws NoSuchAlgorithmException {
            if (method.equalsIgnoreCase(METHOD) == false) {
                throw new NoSuchAlgorithmException("supports only [" + METHOD + "] as method");
            }
            return new MurmurHasher();
        }

        @Override
        public void reset() {
            mh.reset();
        }

        @Override
        public void update(byte[] input) {
            mh.update(input);
        }

        @Override
        public byte[] digest() {
            return mh.digest();
        }

        @Override
        public String getAlgorithm() {
            return Murmur3Hasher.getAlgorithm();
        }
    }

}
