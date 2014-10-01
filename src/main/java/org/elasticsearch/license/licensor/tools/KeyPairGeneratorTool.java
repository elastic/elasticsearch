/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import net.nicholaswilliams.java.licensing.encryption.RSAKeyPairGenerator;
import net.nicholaswilliams.java.licensing.exception.AlgorithmNotSupportedException;
import net.nicholaswilliams.java.licensing.exception.InappropriateKeyException;
import net.nicholaswilliams.java.licensing.exception.InappropriateKeySpecificationException;
import net.nicholaswilliams.java.licensing.exception.RSA2048NotSupportedException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.KeyPair;

public class KeyPairGeneratorTool {

    static class Options {
        private final String publicKeyFilePath;
        private final String privateKeyFilePath;
        private final String keyPass;

        Options(String publicKeyFilePath, String privateKeyFilePath, String keyPass) {
            this.publicKeyFilePath = publicKeyFilePath;
            this.privateKeyFilePath = privateKeyFilePath;
            this.keyPass = keyPass;
        }
    }

    private static Options parse(String[] args) {
        String privateKeyPath = null;
        String publicKeyPath = null;
        String keyPass = null;

        for (int i = 0; i < args.length; i++) {
            String command = args[i];
            switch (command) {
                case "--publicKeyPath":
                    publicKeyPath = args[++i];
                    break;
                case "--privateKeyPath":
                    privateKeyPath = args[++i];
                    break;
                case "--keyPass":
                    keyPass = args[++i];
                    break;
            }
        }

        if (publicKeyPath == null) {
            throw new IllegalArgumentException("mandatory option '--publicKeyPath' is missing");
        }
        if (privateKeyPath == null) {
            throw new IllegalArgumentException("mandatory option '--privateKeyPath' is missing");
        }
        if (keyPass == null) {
            throw new IllegalArgumentException("mandatory option '--keyPass' is missing");
        }

        return new Options(publicKeyPath, privateKeyPath, keyPass);
    }

    public static void main(String[] args) throws IOException {
        run(args, System.out);
    }

    public static void run(String[] args, OutputStream out) throws IOException {
        PrintWriter printWriter = new PrintWriter(out);

        Options options = parse(args);

        if (exists(options.privateKeyFilePath)) {
            throw new IllegalArgumentException("private key already exists in " + options.privateKeyFilePath);
        } else if (exists(options.publicKeyFilePath)) {
            throw new IllegalArgumentException("public key already exists in " + options.publicKeyFilePath);
        }

        KeyPair keyPair = generateKeyPair(options.privateKeyFilePath, options.publicKeyFilePath, options.keyPass);
        if (keyPair != null) {
            printWriter.println("Successfully generated new keyPair [publicKey: " + options.publicKeyFilePath + ", privateKey: " + options.privateKeyFilePath + "]");
        }
    }

    private static boolean exists(String filePath) {
        return new File(filePath).exists();
    }


    private static KeyPair generateKeyPair(String privateKeyFileName, String publicKeyFileName, String password) {
        RSAKeyPairGenerator generator = new RSAKeyPairGenerator();

        KeyPair keyPair;
        try {
            keyPair = generator.generateKeyPair();
        } catch (RSA2048NotSupportedException e) {
            return null;
        }

        try {
            generator.saveKeyPairToFiles(keyPair, privateKeyFileName, publicKeyFileName, password.toCharArray());
        } catch (IOException | AlgorithmNotSupportedException | InappropriateKeyException | InappropriateKeySpecificationException e) {
            throw new IllegalStateException(e);
        }
        return keyPair;
    }
}
