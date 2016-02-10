/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.CliToolConfig.config;
import static org.elasticsearch.license.core.CryptUtils.writeEncryptedPrivateKey;
import static org.elasticsearch.license.core.CryptUtils.writeEncryptedPublicKey;

public class KeyPairGeneratorTool extends CliTool {

    public static final String NAME = "key-pair-generator";
    private static final CliToolConfig CONFIG = config("licensor", KeyPairGeneratorTool.class)
            .cmds(KeyGenerator.CMD)
            .build();

    public KeyPairGeneratorTool() {
        super(CONFIG);
    }

    @Override
    protected Command parse(String s, CommandLine commandLine) throws Exception {
        return KeyGenerator.parse(terminal, commandLine, env);
    }

    public static class KeyGenerator extends Command {

        private static final CliToolConfig.Cmd CMD = cmd(NAME, KeyGenerator.class)
                .options(
                        option("pub", "publicKeyPath").required(true).hasArg(true),
                        option("pri", "privateKeyPath").required(true).hasArg(true)
                ).build();

        public final Path publicKeyPath;
        public final Path privateKeyPath;

        protected KeyGenerator(Terminal terminal, Path publicKeyPath, Path privateKeyPath) {
            super(terminal);
            this.privateKeyPath = privateKeyPath;
            this.publicKeyPath = publicKeyPath;
        }

        public static Command parse(Terminal terminal, CommandLine commandLine, Environment environment) {
            Path publicKeyPath = environment.binFile().getParent().resolve(commandLine.getOptionValue("publicKeyPath"));
            Path privateKeyPath = environment.binFile().getParent().resolve(commandLine.getOptionValue("privateKeyPath"));

            if (Files.exists(privateKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, privateKeyPath + " already exists");
            } else if (Files.exists(publicKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, publicKeyPath + " already exists");
            }
            return new KeyGenerator(terminal, publicKeyPath, privateKeyPath);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            KeyPair keyPair = generateKeyPair(privateKeyPath, publicKeyPath);
            terminal.println(Terminal.Verbosity.VERBOSE, "generating key pair [public key: " + publicKeyPath + ", private key: "
                    + privateKeyPath + "]");
            return (keyPair != null) ? ExitStatus.OK : ExitStatus.CANT_CREATE;
        }

        private static KeyPair generateKeyPair(Path privateKeyPath, Path publicKeyPath) throws IOException, NoSuchAlgorithmException {
            SecureRandom random = new SecureRandom();

            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048, random);
            KeyPair keyPair = keyGen.generateKeyPair();

            saveKeyPairToFiles(keyPair, privateKeyPath, publicKeyPath);
            return keyPair;
        }
    }

    private static void saveKeyPairToFiles(KeyPair keyPair, Path privateKeyPath, Path publicKeyPath) throws IOException {
        Files.write(privateKeyPath, writeEncryptedPrivateKey(keyPair.getPrivate()));
        Files.write(publicKeyPath, writeEncryptedPublicKey(keyPair.getPublic()));
    }

    public static void main(String[] args) throws Exception {
        ExitStatus exitStatus = new KeyPairGeneratorTool().execute(args);
        exit(exitStatus.status());
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    private static void exit(int status) {
        System.exit(status);
    }
}
