/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseUtils;
import org.elasticsearch.license.manager.ESLicenseManager;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class LicenseVerificationTool {

    static class Options {
        private final Set<ESLicenses> licenses;
        private final String publicKeyFilePath;

        Options(Set<ESLicenses> licenses, String publicKeyFilePath) {
            this.licenses = licenses;
            this.publicKeyFilePath = publicKeyFilePath;
        }

    }

    static Set<ESLicenses> asLicensesFromFiles(Set<String> filePaths) throws IOException {
        Set<ESLicenses> licenses = new HashSet<>(filePaths.size());
        for (String filePath : filePaths) {
            final File file = new File(filePath);
            if (file.exists()) {
                licenses.add(LicenseUtils.readLicenseFile(file));
            } else {
                throw new IllegalArgumentException(file.getAbsolutePath() + " does not exist!");
            }
        }
        return licenses;
    }

    static Set<ESLicenses> asLicensesFromStrings(Set<String> fileContents) throws IOException {
        Set<ESLicenses> licenses = new HashSet<>(fileContents.size());
        for (String fileContent : fileContents) {
            licenses.add(LicenseUtils.readLicensesFromString(fileContent));
        }
        return licenses;
    }

    private static Options parse(String[] args) throws IOException {
        Set<String> licenseFilePaths = null;
        Set<String> licensesContents = new HashSet<>();
        Set<ESLicenses> licenses = null;
        String publicKeyPath = null;

        for (int i = 0; i < args.length; i++) {
            String command = args[i];
            switch (command) {
                case "--licensesFiles":
                    licenseFilePaths = new HashSet<>();
                    licenseFilePaths.addAll(Arrays.asList(args[++i].split(":")));
                    break;
                case "--licenses":
                    licensesContents.add(args[++i]);
                    break;
                case "--publicKeyPath":
                    publicKeyPath = args[++i];
                    break;
            }
        }
        if (licenseFilePaths == null && licensesContents.size() == 0) {
            throw new IllegalArgumentException("mandatory option '--licensesFiles' or '--licenses' is missing");
        } else if (licenseFilePaths != null) {
            licenses = asLicensesFromFiles(licenseFilePaths);
        } else if (licensesContents.size() > 0) {
            licenses = asLicensesFromStrings(licensesContents);
        } else {
            throw new IllegalArgumentException("no licenses could be extracted");
        }
        if (publicKeyPath == null) {
            throw new IllegalArgumentException("mandatory option '--publicKeyPath' is missing");
        }
        assert licenses != null;
        return new Options(licenses, publicKeyPath);
    }

    public static void main(String[] args) throws IOException {
        run(args, System.out);
    }

    public static void run(String[] args, OutputStream out) throws IOException {
        Options options = parse(args);

        // verify licenses
        ESLicenseManager licenseManager = new ESLicenseManager(options.licenses, options.publicKeyFilePath);
        licenseManager.verifyLicenses();

        // dump effective licences
        LicenseUtils.dumpLicenseAsJson(licenseManager.getEffectiveLicenses(), out);
    }

}
