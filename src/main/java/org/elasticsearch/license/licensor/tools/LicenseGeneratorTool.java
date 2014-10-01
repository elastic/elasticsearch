/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseUtils;
import org.elasticsearch.license.licensor.ESLicenseSigner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class LicenseGeneratorTool {

    static class Options {
        private final String licensesInput;
        private final String publicKeyFilePath;
        private final String privateKeyFilePath;
        private final String keyPass;

        Options(String licensesInput, String publicKeyFilePath, String privateKeyFilePath, String keyPass) {
            this.licensesInput = licensesInput;
            this.publicKeyFilePath = publicKeyFilePath;
            this.privateKeyFilePath = privateKeyFilePath;
            this.keyPass = keyPass;
        }
    }

    private static Options parse(String[] args) throws IOException {
        String licenseInput = null;
        String licenseFilePath = null;
        String privateKeyPath = null;
        String publicKeyPath = null;
        String keyPass = null;

        for (int i = 0; i < args.length; i++) {
            String command = args[i].trim();
            switch (command) {
                case "--license":
                    licenseInput = args[++i];
                    break;
                case "--licenseFile":
                    licenseFilePath = args[++i];
                    break;
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

        if ((licenseInput == null && licenseFilePath == null) || (licenseInput != null && licenseFilePath != null)) {
            throw new IllegalArgumentException("only one of '--license' or '--licenseFile' option should be set");
        } else if (licenseFilePath != null) {
            File licenseFile = new File(licenseFilePath);
            if (licenseFile.exists()) {
                licenseInput = FileUtils.readFileToString(licenseFile, Charset.forName("UTF-8"));
            } else {
                throw new IllegalArgumentException("provided --licenseFile " + licenseFile.getAbsolutePath() + " does not exist!");
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

        return new Options(licenseInput, publicKeyPath, privateKeyPath, keyPass);
    }

    public static void main(String[] args) throws IOException {
        run(args, System.out);
    }

    public static void run(String[] args, OutputStream out) throws IOException {
        Options options = parse(args);

        ESLicenses esLicenses = LicenseUtils.readLicensesFromString(options.licensesInput);

        ESLicenseSigner signer = new ESLicenseSigner(new ESLicenseSigner.SignerOptions(options.privateKeyFilePath, options.publicKeyFilePath, options.keyPass));
        ESLicenses signedLicences = signer.sign(esLicenses);

        LicenseUtils.dumpLicenseAsJson(signedLicences, out);
    }

}
