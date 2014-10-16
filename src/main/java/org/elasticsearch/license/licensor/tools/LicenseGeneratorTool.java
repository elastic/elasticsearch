/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseUtils;
import org.elasticsearch.license.licensor.ESLicenseSigner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class LicenseGeneratorTool {

    static class Options {
        private final ESLicenses licenses;
        private final String publicKeyFilePath;
        private final String privateKeyFilePath;

        Options(ESLicenses licenses, String publicKeyFilePath, String privateKeyFilePath) {
            this.licenses = licenses;
            this.publicKeyFilePath = publicKeyFilePath;
            this.privateKeyFilePath = privateKeyFilePath;
        }
    }

    private static Options parse(String[] args) throws IOException {
        ESLicenses licenses = null;
        String privateKeyPath = null;
        String publicKeyPath = null;

        for (int i = 0; i < args.length; i++) {
            String command = args[i].trim();
            switch (command) {
                case "--license":
                    if (licenses != null) {
                        throw new IllegalArgumentException("only one of --licenses' or '--licenseFile' can be specified");
                    }
                    String licenseInput = args[++i];
                    licenses = LicenseUtils.readLicensesFromString(licenseInput);
                    break;
                case "--licenseFile":
                    if (licenses != null) {
                        throw new IllegalArgumentException("only one of --licenses' or '--licenseFile' can be specified");
                    }
                    File licenseFile = new File(args[++i]);
                    if (licenseFile.exists()) {
                        licenses = LicenseUtils.readLicenseFile(licenseFile);
                    } else {
                        throw new IllegalArgumentException(licenseFile.getAbsolutePath() + " does not exist!");
                    }
                    break;
                case "--publicKeyPath":
                    publicKeyPath = args[++i];
                    break;
                case "--privateKeyPath":
                    privateKeyPath = args[++i];
                    break;
            }
        }

        if (licenses == null) {
            throw new IllegalArgumentException("at least one of '--licenses' or '--licenseFile' has to be provided");
        }
        if (publicKeyPath == null) {
            throw new IllegalArgumentException("mandatory option '--publicKeyPath' is missing");
        }
        if (privateKeyPath == null) {
            throw new IllegalArgumentException("mandatory option '--privateKeyPath' is missing");
        }

        return new Options(licenses, publicKeyPath, privateKeyPath);
    }

    public static void main(String[] args) throws IOException {
        run(args, System.out);
    }

    public static void run(String[] args, OutputStream out) throws IOException {
        Options options = parse(args);


        ESLicenseSigner signer = new ESLicenseSigner(options.privateKeyFilePath, options.publicKeyFilePath);
        ESLicenses signedLicences = signer.sign(options.licenses);

        LicenseUtils.dumpLicenseAsJson(signedLicences, out);
    }

}
