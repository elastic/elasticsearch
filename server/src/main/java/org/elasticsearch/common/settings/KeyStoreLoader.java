/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Implementation of {@link SecureSettingsLoader} for {@link KeyStoreWrapper}
 */
public class KeyStoreLoader implements SecureSettingsLoader {
    private Path keystoreBackupPath = null;

    @Override
    public SecureSettings load(Environment environment, Terminal terminal, AutoConfigureFunction<SecureString, Environment> autoConfigure)
        throws Exception {
        // See if we have a keystore already present
        KeyStoreWrapper secureSettings = KeyStoreWrapper.load(environment.configFile());
        // If there's no keystore or the keystore has no password, set an empty password
        var password = (secureSettings == null || secureSettings.hasPassword() == false)
            ? new SecureString(new char[0])
            : new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT));

        environment = autoConfigure.apply(password);
        secureSettings = KeyStoreWrapper.bootstrap(environment.configFile(), () -> password);
        password.close();

        return secureSettings;
    }

    @Override
    public AutoConfigureResult autoConfigure(
        Environment env,
        Terminal terminal,
        ZonedDateTime autoConfigDate,
        Consumer<SecureString> configureTransportSecrets,
        Consumer<SecureString> configureHttpSecrets
    ) {
        final Path keystorePath = KeyStoreWrapper.keystorePath(env.configFile());

        // save the existing keystore before replacing
        this.keystoreBackupPath = env.configFile()
            .resolve(
                String.format(Locale.ROOT, KeyStoreWrapper.KEYSTORE_FILENAME + ".%d.orig", autoConfigDate.toInstant().getEpochSecond())
            );
        if (Files.exists(keystorePath)) {
            try {
                Files.copy(keystorePath, keystoreBackupPath, StandardCopyOption.COPY_ATTRIBUTES);
            } catch (Exception t) {
                return new AutoConfigureResult(
                    t,
                    (e) -> onAutoConfigureSuccess(e, keystoreBackupPath),
                    (e) -> onAutoConfigureFailure(e, keystoreBackupPath)
                );
            }
        }

        AtomicReference<SecureString> password = new AtomicReference<>(null);

        try (KeyStoreWrapper nodeKeystore = KeyStoreWrapper.bootstrap(env.configFile(), () -> {
            password.set(new SecureString(terminal.readSecret("")));
            return password.get().clone();
        })) {
            // do not overwrite keystore entries
            // instead expect the admin to manually remove them herself
            if (nodeKeystore.getSettingNames().contains("xpack.security.transport.ssl.keystore.secure_password")
                || nodeKeystore.getSettingNames().contains("xpack.security.transport.ssl.truststore.secure_password")
                || nodeKeystore.getSettingNames().contains("xpack.security.http.ssl.keystore.secure_password")) {
                // this error condition is akin to condition of existing configuration in the yml file
                // this is not a fresh install and the admin has something planned for Security
                // Even though this is probably invalid configuration, do NOT fix it, let the node fail to start in its usual way.
                // Still display a message, because this can be tricky to figure out (why auto-conf did not run) if by mistake.
                throw new UserException(
                    ExitCodes.CONFIG,
                    "Aborting auto configuration because the node keystore contains password settings already"
                );
            }
            try (SecureString transportKeystorePassword = newKeystorePassword()) {
                configureTransportSecrets.accept(transportKeystorePassword);
                nodeKeystore.setString("xpack.security.transport.ssl.keystore.secure_password", transportKeystorePassword.getChars());
                // we use the same PKCS12 file for the keystore and the truststore
                nodeKeystore.setString("xpack.security.transport.ssl.truststore.secure_password", transportKeystorePassword.getChars());
            }
            try (SecureString httpKeystorePassword = newKeystorePassword()) {
                configureHttpSecrets.accept(httpKeystorePassword);
                nodeKeystore.setString("xpack.security.http.ssl.keystore.secure_password", httpKeystorePassword.getChars());
            }
            // finally overwrites the node keystore (if the keystores have been successfully written)
            nodeKeystore.save(env.configFile(), (password.get() == null) ? new char[0] : password.get().getChars());
        } catch (Exception t) {
            // restore keystore to revert possible keystore bootstrap
            try {
                if (Files.exists(keystoreBackupPath)) {
                    Files.move(
                        keystoreBackupPath,
                        keystorePath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.COPY_ATTRIBUTES
                    );
                } else {
                    Files.deleteIfExists(keystorePath);
                }
            } catch (Exception ex) {
                t.addSuppressed(ex);
            }
            return new AutoConfigureResult(
                t,
                (e) -> onAutoConfigureSuccess(e, keystoreBackupPath),
                (e) -> onAutoConfigureFailure(e, keystoreBackupPath)
            );
        } finally {
            if (password.get() != null) {
                password.get().close();
            }
        }

        return new AutoConfigureResult(
            null,
            (e) -> onAutoConfigureSuccess(e, keystoreBackupPath),
            (e) -> onAutoConfigureFailure(e, keystoreBackupPath)
        );
    }

    private void onAutoConfigureFailure(Environment env, Path keystoreBackupPath) throws Exception {
        assert keystoreBackupPath != null;
        final Path keystorePath = KeyStoreWrapper.keystorePath(env.configFile());

        if (Files.exists(keystoreBackupPath)) {
            Files.move(
                keystoreBackupPath,
                keystorePath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.COPY_ATTRIBUTES
            );
        } else {
            Files.deleteIfExists(keystorePath);
        }
    }

    public void onAutoConfigureSuccess(Environment env, Path keystoreBackupPath) throws Exception {
        assert keystoreBackupPath != null;
        // only delete the backed-up keystore file if all went well, because the new keystore contains its entries
        Files.deleteIfExists(keystoreBackupPath);
    }

    @Override
    public Exception removeAutoConfiguration(Environment env, Terminal terminal) {
        if (Files.exists(KeyStoreWrapper.keystorePath(env.configFile()))) {
            SecureString password = new SecureString(terminal.readSecret(""));
            try (KeyStoreWrapper existingKeystore = KeyStoreWrapper.load(env.configFile())) {
                existingKeystore.decrypt(password.getChars());
                List<String> secureSettingsToRemove = List.of(
                    "xpack.security.transport.ssl.keystore.secure_password",
                    "xpack.security.transport.ssl.truststore.secure_password",
                    "xpack.security.http.ssl.keystore.secure_password",
                    "autoconfiguration.password_hash"
                );
                for (String setting : secureSettingsToRemove) {
                    if (existingKeystore.getSettingNames().contains(setting) == false) {
                        throw new UserException(
                            ExitCodes.IO_ERROR,
                            "Aborting enrolling to cluster. Unable to remove existing security configuration, "
                                + "elasticsearch.keystore did not contain expected setting ["
                                + setting
                                + "]."
                        );
                    }
                    existingKeystore.remove(setting);
                }
                existingKeystore.save(env.configFile(), password.getChars());
            } catch (Exception e) {
                return e;
            } finally {
                password.close();
            }
        }

        return null;
    }

    SecureString newKeystorePassword() {
        return UUIDs.randomBase64UUIDSecureString();
    }

    @Override
    public String validate(Environment environment) {
        final Path keystorePath = KeyStoreWrapper.keystorePath(environment.configFile());
        // Empty keystore path is valid, but not if we have an unreadable keystore
        if (Files.exists(keystorePath)
            && (false == Files.isRegularFile(keystorePath, LinkOption.NOFOLLOW_LINKS) || false == Files.isReadable(keystorePath))) {
            return String.format(
                Locale.ROOT,
                "Skipping security auto configuration because the node keystore file [%s] is not a readable regular file",
                keystorePath
            );
        }

        return null;
    }
}
