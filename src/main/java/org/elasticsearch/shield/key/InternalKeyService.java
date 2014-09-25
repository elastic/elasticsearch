/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.key;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldException;
import org.elasticsearch.shield.plugin.ShieldPlugin;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 *
 */
public class InternalKeyService extends AbstractComponent implements KeyService {

    public static final String FILE_SETTING = "shield.system_key.file";
    public static final String KEY_ALGO = "HmacSHA512";
    public static final int KEY_SIZE = 1024;

    static final String FILE_NAME = ".system_key";
    static final String HMAC_ALGO = "HmacSHA1";

    private static final Pattern SIG_PATTERN = Pattern.compile("\\$\\$[0-9]+\\$\\$.+");

    private final Path keyFile;

    private volatile SecretKey key;

    @Inject
    public InternalKeyService(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, Listener.NOOP);
    }

    InternalKeyService(Settings settings, Environment env, ResourceWatcherService watcherService, Listener listener) {
        super(settings);
        keyFile = resolveFile(settings, env);
        key = readKey(keyFile);
        FileWatcher watcher = new FileWatcher(keyFile.getParent().toFile());
        watcher.addListener(new FileListener(listener));
        watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
    }

    public static byte[] generateKey() throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGO);
        generator.init(KEY_SIZE);
        return generator.generateKey().getEncoded();
    }

    public static Path resolveFile(Settings settings, Environment env) {
        String location = settings.get(FILE_SETTING);
        if (location == null) {
            return ShieldPlugin.resolveConfigFile(env, FILE_NAME);
        }
        return Paths.get(location);
    }

    static SecretKey readKey(Path file) {
        if (!Files.exists(file)) {
            return null;
        }
        try {
            byte[] bytes = Files.readAllBytes(file);
            return new SecretKeySpec(bytes, KEY_ALGO);
        } catch (IOException e) {
            throw new ShieldException("Could not read secret key", e);
        }
    }

    @Override
    public String sign(String text) {
        SecretKey key = this.key;
        if (key == null) {
            return text;
        }
        Mac mac = createMac(key);
        byte[] sig = mac.doFinal(text.getBytes(Charsets.UTF_8));
        String sigStr = Base64.encodeBase64URLSafeString(sig);
        return "$$" + sigStr.length() + "$$" + sigStr + text;
    }

    @Override
    public String unsignAndVerify(String signedText) {
        SecretKey key = this.key;
        if (key == null) {
            return signedText;
        }

        if (!signedText.startsWith("$$")) {
            throw new SignatureException("tampered signed text");
        }

        try {

            //     $$34$$sigtext

            int i = signedText.indexOf("$$", 2);
            int length = Integer.parseInt(signedText.substring(2, i));
            String sigStr = signedText.substring(i + 2, i + 2 + length);
            String text = signedText.substring(i + 2 + length);
            Mac mac = createMac(key);
            byte[] sig = mac.doFinal(text.getBytes(Charsets.UTF_8));


            if (!Base64.encodeBase64URLSafeString(sig).equals(sigStr)) {
                throw new SignatureException("tampered signed text");
            }
            return text;
        } catch (Throwable t) {
            throw new SignatureException("tampered signed text");
        }
    }

    @Override
    public boolean signed(String text) {
        return SIG_PATTERN.matcher(text).matches();
    }

    static Mac createMac(SecretKey key) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(key);
            return mac;
        } catch (Exception e) {
            throw new ElasticsearchException("Could not initialize mac", e);
        }
    }

    private class FileListener extends FileChangesListener {

        private final Listener listener;

        private FileListener(Listener listener) {
            this.listener = listener;
        }

        @Override
        public void onFileCreated(File file) {
            if (file.equals(keyFile.toFile())) {
                key = readKey(file.toPath());
                listener.onKeyRefresh();
            }
        }

        @Override
        public void onFileDeleted(File file) {
            if (file.equals(keyFile.toFile())) {
                logger.error("System key file was removed! As long as the system key file is missing, elasticsearch won't function as expected for some requests (e.g. scroll/scan)");
                key = null;
            }
        }

        @Override
        public void onFileChanged(File file) {
            if (file.equals(keyFile.toFile())) {
                key = readKey(file.toPath());
                listener.onKeyRefresh();
            }
        }
    }

    static interface Listener {

        final Listener NOOP = new Listener() {
            @Override
            public void onKeyRefresh() {
            }
        };

        void onKeyRefresh();


    }
}
