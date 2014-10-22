/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.manager.ESLicenseManager;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class LicenseVerificationTool {

    static class Options {
        private final Set<ESLicense> licenses;
        private final String publicKeyFilePath;

        Options(Set<ESLicense> licenses, String publicKeyFilePath) {
            this.licenses = licenses;
            this.publicKeyFilePath = publicKeyFilePath;
        }
    }

    private static Options parse(String[] args) throws IOException {
        Set<ESLicense> licenses = new HashSet<>();
        String publicKeyPath = null;

        for (int i = 0; i < args.length; i++) {
            String command = args[i];
            switch (command) {
                case "--licensesFiles":
                    for (String filePath : args[++i].split(":")) {
                        File file = new File(filePath);
                        if (file.exists()) {
                            licenses.addAll(ESLicenses.fromSource(Files.readAllBytes(Paths.get(file.getAbsolutePath()))));
                        } else {
                            throw new IllegalArgumentException(file.getAbsolutePath() + " does not exist!");
                        }
                    }
                    break;
                case "--licenses":
                    licenses.addAll(ESLicenses.fromSource(args[++i]));
                    break;
                case "--publicKeyPath":
                    publicKeyPath = args[++i];
                    break;
            }
        }
        if (licenses.size() == 0) {
            throw new IllegalArgumentException("mandatory option '--licensesFiles' or '--licenses' is missing");
        }
        if (publicKeyPath == null) {
            throw new IllegalArgumentException("mandatory option '--publicKeyPath' is missing");
        }
        return new Options(licenses, publicKeyPath);
    }

    public static void main(String[] args) throws IOException {
        run(args, System.out);
    }

    public static void run(String[] args, OutputStream out) throws IOException {
        Options options = parse(args);

        // verify licenses
        FileBasedESLicenseProvider licenseProvider = new FileBasedESLicenseProvider(options.licenses);
        ESLicenseManager licenseManager = new ESLicenseManager();
        licenseManager.verifyLicenses(licenseProvider.getEffectiveLicenses());

        // dump effective licences
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, out);
        ESLicenses.toXContent(licenseProvider.getEffectiveLicenses().values(), builder);
        builder.flush();
    }

}
