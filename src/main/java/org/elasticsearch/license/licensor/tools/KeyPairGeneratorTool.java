/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import net.nicholaswilliams.java.licensing.encryption.Hasher;
import net.nicholaswilliams.java.licensing.encryption.RSAKeyPairGenerator;
import net.nicholaswilliams.java.licensing.exception.AlgorithmNotSupportedException;
import net.nicholaswilliams.java.licensing.exception.InappropriateKeyException;
import net.nicholaswilliams.java.licensing.exception.InappropriateKeySpecificationException;
import net.nicholaswilliams.java.licensing.exception.RSA2048NotSupportedException;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.cli.commons.CommandLine;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.CliToolConfig.config;

public class KeyPairGeneratorTool extends CliTool {

    private static final CliToolConfig CONFIG = config("key-pair-generator", KeyPairGeneratorTool.class)
            .cmds(KeyPairGenerator.CMD)
            .build();

    public KeyPairGeneratorTool() {
        super(CONFIG);
    }

    @Override
    protected Command parse(String s, CommandLine commandLine) throws Exception {
        return KeyPairGenerator.parse(terminal, commandLine);
    }

    private static class KeyPairGenerator extends Command {

        public static String DEFAULT_PASS_PHRASE = "elasticsearch-license";
        private static final String NAME = "key-pair-generator";
        private static final CliToolConfig.Cmd CMD = cmd(NAME, KeyPairGenerator.class)
                .options(
                        option("pub", "publicKeyPath").required(true).hasArg(true),
                        option("pri", "privateKeyPath").required(true).hasArg(true)
                ).build();

        private final String publicKeyPath;
        private final String privateKeyPath;

        protected KeyPairGenerator(Terminal terminal, String publicKeyPath, String privateKeyPath) {
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
            return new KeyPairGenerator(terminal, publicKeyPath, privateKeyPath);
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

        private static KeyPair generateKeyPair(String privateKeyFileName, String publicKeyFileName) {
            RSAKeyPairGenerator generator = new RSAKeyPairGenerator();

            KeyPair keyPair;
            try {
                keyPair = generator.generateKeyPair();
            } catch (RSA2048NotSupportedException e) {
                throw new IllegalStateException(e);
            }

            try {
                generator.saveKeyPairToFiles(keyPair, privateKeyFileName, publicKeyFileName, Hasher.hash(DEFAULT_PASS_PHRASE).toCharArray());
            } catch (IOException | AlgorithmNotSupportedException | InappropriateKeyException | InappropriateKeySpecificationException e) {
                throw new IllegalStateException(e);
            }
            return keyPair;
        }
    }

    public static void main(String[] args) throws Exception {
        int status = new KeyPairGeneratorTool().execute(args);
        System.exit(status);
    }
}
