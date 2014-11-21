/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.cli.commons.CommandLine;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.CliToolConfig.config;
import static org.elasticsearch.license.core.shaded.CryptUtils.writeEncryptedPrivateKey;
import static org.elasticsearch.license.core.shaded.CryptUtils.writeEncryptedPublicKey;

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
        return KeyGenerator.parse(terminal, commandLine);
    }

    public static class KeyGenerator extends Command {

        private static final CliToolConfig.Cmd CMD = cmd(NAME, KeyGenerator.class)
                .options(
                        option("pub", "publicKeyPath").required(true).hasArg(true),
                        option("pri", "privateKeyPath").required(true).hasArg(true)
                ).build();

        public final String publicKeyPath;
        public final String privateKeyPath;

        protected KeyGenerator(Terminal terminal, String publicKeyPath, String privateKeyPath) {
            super(terminal);
            this.privateKeyPath = privateKeyPath;
            this.publicKeyPath = publicKeyPath;
        }

        public static Command parse(Terminal terminal, CommandLine commandLine) {
            String publicKeyPath = commandLine.getOptionValue("publicKeyPath");
            String privateKeyPath = commandLine.getOptionValue("privateKeyPath");

            if (exists(privateKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, privateKeyPath + " already exists");
            } else if (exists(publicKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, publicKeyPath + " already exists");
            }
            return new KeyGenerator(terminal, publicKeyPath, privateKeyPath);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            KeyPair keyPair = generateKeyPair(privateKeyPath, publicKeyPath);
            terminal.println(Terminal.Verbosity.VERBOSE, "generating key pair [public key: " + publicKeyPath + ", private key: " + privateKeyPath + "]");
            return (keyPair != null) ? ExitStatus.OK : ExitStatus.CANT_CREATE;
        }

        private static boolean exists(String filePath) {
            return new File(filePath).exists();
        }

        private static KeyPair generateKeyPair(String privateKeyFileName, String publicKeyFileName) throws IOException, NoSuchAlgorithmException {
            SecureRandom random = new SecureRandom();

            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048, random);
            KeyPair keyPair = keyGen.generateKeyPair();

            saveKeyPairToFiles(keyPair, privateKeyFileName, publicKeyFileName);
            return keyPair;
        }
    }

    private static void saveKeyPairToFiles(KeyPair keyPair, String privateKeyFileName, String publicKeyFileName) throws IOException {
        Files.write(Paths.get(privateKeyFileName), writeEncryptedPrivateKey(keyPair.getPrivate()));
        Files.write(Paths.get(publicKeyFileName), writeEncryptedPublicKey(keyPair.getPublic()));
    }

    public static void main(String[] args) throws Exception {
        int status = new KeyPairGeneratorTool().execute(args);
        System.exit(status);
    }
}
