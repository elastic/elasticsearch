/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license.licensor.tools;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.LoggingAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.PathUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;

import static org.elasticsearch.license.CryptUtils.writeEncryptedPrivateKey;

public class KeyPairGeneratorTool extends LoggingAwareCommand {

    private final OptionSpec<String> publicKeyPathOption;
    private final OptionSpec<String> privateKeyPathOption;

    public KeyPairGeneratorTool() {
        super("Generates a key pair with RSA 2048-bit security");
        // TODO: in jopt-simple 5.0 we can use a PathConverter to take Path instead of File
        this.publicKeyPathOption = parser.accepts("publicKeyPath", "public key path")
            .withRequiredArg().required();
        this.privateKeyPathOption = parser.accepts("privateKeyPath", "private key path")
            .withRequiredArg().required();
    }

    public static void main(String[] args) throws Exception {
        exit(new KeyPairGeneratorTool().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("This tool generates and saves a key pair to the provided publicKeyPath");
        terminal.println("and privateKeyPath. The tool checks the existence of the provided key");
        terminal.println("paths and will not override if any existing keys are found.");
        terminal.println("");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        Path publicKeyPath = parsePath(publicKeyPathOption.value(options));
        Path privateKeyPath = parsePath(privateKeyPathOption.value(options));
        if (Files.exists(privateKeyPath)) {
            throw new UserException(ExitCodes.USAGE, privateKeyPath + " already exists");
        } else if (Files.exists(publicKeyPath)) {
            throw new UserException(ExitCodes.USAGE, publicKeyPath + " already exists");
        }

        SecureRandom random = new SecureRandom();
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048, random);
        KeyPair keyPair = keyGen.generateKeyPair();

        Files.write(privateKeyPath, writeEncryptedPrivateKey(keyPair.getPrivate()));
        Files.write(publicKeyPath, keyPair.getPublic().getEncoded());

        terminal.println(
                Terminal.Verbosity.VERBOSE,
                "generating key pair [public key: "
                        + publicKeyPath
                        + ", private key: "
                        + privateKeyPath + "]");
    }

    @SuppressForbidden(reason = "Parsing command line path")
    private static Path parsePath(String path) {
        return PathUtils.get(path);
    }

}
