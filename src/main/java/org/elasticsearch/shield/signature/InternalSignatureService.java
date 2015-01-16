/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.signature;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldException;
import org.elasticsearch.shield.ShieldPlugin;
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
public class InternalSignatureService extends AbstractLifecycleComponent<InternalSignatureService> implements SignatureService {

    public static final String FILE_SETTING = "shield.system_key.file";
    public static final String KEY_ALGO = "HmacSHA512";
    public static final int KEY_SIZE = 1024;

    static final String FILE_NAME = "system_key";
    static final String HMAC_ALGO = "HmacSHA1";

    private static final Pattern SIG_PATTERN = Pattern.compile("^\\$\\$[0-9]+\\$\\$.+");
    private final Environment env;
    private final ResourceWatcherService watcherService;
    private final Listener listener;

    private Path keyFile;

    private volatile SecretKey key;

    @Inject
    public InternalSignatureService(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, Listener.NOOP);
    }

    InternalSignatureService(Settings settings, Environment env, ResourceWatcherService watcherService, Listener listener) {
        super(settings);
        this.env = env;
        this.watcherService = watcherService;
        this.listener = listener;
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
            throw new ShieldException("could not read secret key", e);
        }
    }

    @Override
    public String sign(String text) {
        SecretKey key = this.key;
        if (key == null) {
            return text;
        }
        String sigStr = signInternal(text);
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
            String sig = signInternal(text);
            if (!sig.equals(sigStr)) {
                throw new SignatureException("the signed texts don't match");
            }
            return text;
        } catch (SignatureException e) {
            throw e;
        } catch (Throwable t) {
            throw new SignatureException("error while verifying the signed text", t);
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
            throw new ElasticsearchException("could not initialize mac", e);
        }
    }

    private String signInternal(String text) {
        Mac mac = createMac(key);
        byte[] sig = mac.doFinal(text.getBytes(Charsets.UTF_8));
        return Base64.encodeBase64URLSafeString(sig);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        keyFile = resolveFile(settings, env);
        key = readKey(keyFile);
        FileWatcher watcher = new FileWatcher(keyFile.getParent().toFile());
        watcher.addListener(new FileListener(listener));
        watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
    }

    @Override
    protected void doStop() throws ElasticsearchException {}

    @Override
    protected void doClose() throws ElasticsearchException {}

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
                logger.error("system key file was removed! as long as the system key file is missing, elasticsearch " +
                        "won't function as expected for some requests (e.g. scroll/scan)");
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
