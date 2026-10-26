/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Project;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The {@code @token@} values that are replaced (via {@code ReplaceTokens}) when copying packaging files into the
 * archive and package distributions. This includes both scripts like {@code bin/elasticsearch} and
 * {@code bin/elasticsearch-plugin} that a user might run and also scripts like {@code postinst} which are run as
 * part of the installation.
 *
 * <dl>
 *  <dt>project.name</dt>
 *  <dd>The name of the project. It's sprinkled throughout the scripts.</dd>
 *  <dt>project.version</dt>
 *  <dd>The version of the project. It's mostly used to find the exact jar name.</dd>
 *  <dt>path.conf</dt>
 *  <dd>The default directory from which to load configuration. This is used in the packaging scripts, but in that
 *    context it is always /etc/elasticsearch. It's also used in bin/elasticsearch-plugin, where it is
 *    /etc/elasticsearch for the os packages but $ES_HOME/config otherwise.</dd>
 *  <dt>path.env</dt>
 *  <dd>The env file sourced before bin/elasticsearch to set environment variables. Think
 *    /etc/defaults/elasticsearch. Only defined for the os packages.</dd>
 *  <dt>scripts.footer</dt>
 *  <dd>Footer appended to control scripts embedded in the distribution that is (almost) entirely there for
 *    cosmetic reasons.</dd>
 * </dl>
 */
public final class TokenExpansions {

    static final String DEFAULT_LICENSE_FILE = "ELASTIC-LICENSE-2.0.txt";
    static final String TEST_DISTRIBUTION_LICENSE_FILE = "AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt";

    private static final String PACKAGING_PATH_DATA = "path.data: /var/lib/elasticsearch";
    private static final String PACKAGING_PATH_LOGS = "path.logs: /var/log/elasticsearch";

    private TokenExpansions() {}

    /**
     * The name of the license file in the repository's {@code licenses} directory that a distribution ships.
     */
    public static String licenseFileName(boolean testDistribution) {
        return testDistribution ? TEST_DISTRIBUTION_LICENSE_FILE : DEFAULT_LICENSE_FILE;
    }

    /**
     * Computes the token expansions for the given distribution type of the given project, reading the license text
     * from the repository's {@code licenses} directory.
     *
     * <p>This is a thin adapter for build scripts; see {@link #forDistribution} for the actual rules. The license file
     * is read through {@link org.gradle.api.provider.ProviderFactory#fileContents} so that the configuration cache
     * tracks it as an input and is invalidated when the license text changes.
     *
     * @param distributionType the identifier of a {@link DistributionType}, e.g. {@code deb}
     */
    public static Map<String, String> forProject(Project project, String distributionType, boolean testDistribution) {
        var licenseFile = project.getLayout().getSettingsDirectory().file("licenses/" + licenseFileName(testDistribution));
        String licenseText = project.getProviders().fileContents(licenseFile).getAsText().getOrNull();
        if (licenseText == null) {
            throw new IllegalStateException("License file [" + licenseFile.getAsFile() + "] does not exist");
        }
        return forDistribution(
            DistributionType.fromId(distributionType),
            project.getName(),
            String.valueOf(project.getVersion()),
            VersionProperties.getElasticsearchVersion(),
            licenseText
        );
    }

    /**
     * Computes the token expansions for the given distribution type.
     *
     * @param distributionType the distribution type to compute the expansions for
     * @param projectName the name used for {@code @project.name@} and in the script footer
     * @param projectVersion the version used for {@code @project.version@} and in the script footer
     * @param elasticsearchVersion the version {@code @project.minor.version@} is derived from
     * @param licenseText the license text shipped by the distribution; only expanded for deb packages
     */
    public static Map<String, String> forDistribution(
        DistributionType distributionType,
        String projectName,
        String projectVersion,
        Version elasticsearchVersion,
        String licenseText
    ) {
        boolean isPackage = distributionType.isPackage();
        String footer = "# Built for " + projectName + "-" + projectVersion + " (" + distributionType.id() + ")";

        Map<String, String> expansions = new LinkedHashMap<>();
        expansions.put("project.name", projectName);
        expansions.put("project.version", projectVersion);
        expansions.put("project.minor.version", elasticsearchVersion.getMajor() + "." + elasticsearchVersion.getMinor());
        expansions.put("path.conf", isPackage ? "/etc/elasticsearch" : "\"$ES_HOME\"/config");
        expansions.put("path.data", isPackage ? PACKAGING_PATH_DATA : "#path.data: /path/to/data");
        // There is no env file for tar or zip, and @path.env@ is only referenced by the package scripts, so archives do
        // not get the token at all.
        switch (distributionType) {
            case DEB -> expansions.put("path.env", PackageMetadata.Type.DEB.envFile());
            case RPM -> expansions.put("path.env", PackageMetadata.Type.RPM.envFile());
            case TAR, ZIP -> {
            }
        }
        expansions.put("source.path.env", switch (distributionType) {
            case DEB -> "source " + PackageMetadata.Type.DEB.envFile();
            case RPM -> "source " + PackageMetadata.Type.RPM.envFile();
            case TAR, ZIP -> "if [ -z \"$ES_PATH_CONF\" ]; then ES_PATH_CONF=\"$ES_HOME\"/config; fi";
        });
        expansions.put("path.logs", isPackage ? PACKAGING_PATH_LOGS : "#path.logs: /path/to/logs");
        // Debian needs exit 0 on these scripts so we add it here and preserve the pretty footer.
        expansions.put("scripts.footer", distributionType == DistributionType.DEB ? "exit 0\n" + footer : footer);
        expansions.put("es.distribution.type", distributionType.id());
        if (distributionType == DistributionType.DEB) {
            expansions.put("license.name", "Elastic-License");
            // license text needs to be indented with a single space
            expansions.put("license.text", " " + licenseText.replace("\n", "\n "));
        }
        return Collections.unmodifiableMap(expansions);
    }
}
