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
import org.gradle.api.UncheckedIOException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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
 *    /etc/defaults/elasticsearch.</dd>
 *  <dt>scripts.footer</dt>
 *  <dd>Footer appended to control scripts embedded in the distribution that is (almost) entirely there for
 *    cosmetic reasons.</dd>
 * </dl>
 */
public final class TokenExpansions {

    /**
     * The distribution types token expansions can be computed for.
     */
    public static final Set<String> DISTRIBUTION_TYPES = Set.of("tar", "zip", "deb", "rpm");

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
     * <p>This is a thin adapter for build scripts; see {@link #forDistribution} for the actual rules.
     */
    public static Map<String, String> forProject(Project project, String distributionType, boolean testDistribution) {
        var licenseFile = project.getLayout()
            .getSettingsDirectory()
            .file("licenses/" + licenseFileName(testDistribution))
            .getAsFile()
            .toPath();
        String licenseText;
        try {
            licenseText = Files.readString(licenseFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read license file [" + licenseFile + "]", e);
        }
        return forDistribution(
            distributionType,
            project.getName(),
            String.valueOf(project.getVersion()),
            VersionProperties.getElasticsearchVersion(),
            licenseText
        );
    }

    /**
     * Computes the token expansions for the given distribution type.
     *
     * @param distributionType one of {@link #DISTRIBUTION_TYPES}
     * @param projectName the name used for {@code @project.name@} and in the script footer
     * @param projectVersion the version used for {@code @project.version@} and in the script footer
     * @param elasticsearchVersion the version {@code @project.minor.version@} is derived from
     * @param licenseText the license text shipped by the distribution; only expanded for deb packages
     */
    public static Map<String, String> forDistribution(
        String distributionType,
        String projectName,
        String projectVersion,
        Version elasticsearchVersion,
        String licenseText
    ) {
        if (DISTRIBUTION_TYPES.contains(distributionType) == false) {
            throw new IllegalArgumentException(
                "Unknown distribution type [" + distributionType + "], expected one of " + DISTRIBUTION_TYPES
            );
        }
        boolean isPackage = distributionType.equals("deb") || distributionType.equals("rpm");
        String footer = "# Built for " + projectName + "-" + projectVersion + " (" + distributionType + ")";

        Map<String, String> expansions = new LinkedHashMap<>();
        expansions.put("project.name", projectName);
        expansions.put("project.version", projectVersion);
        expansions.put("project.minor.version", elasticsearchVersion.getMajor() + "." + elasticsearchVersion.getMinor());
        expansions.put("path.conf", isPackage ? "/etc/elasticsearch" : "\"$ES_HOME\"/config");
        expansions.put("path.data", isPackage ? PACKAGING_PATH_DATA : "#path.data: /path/to/data");
        expansions.put("path.env", switch (distributionType) {
            case "deb" -> "/etc/default/elasticsearch";
            case "rpm" -> "/etc/sysconfig/elasticsearch";
            // There isn't one of these files for tar or zip. Only package scripts reference @path.env@, so this value
            // is currently never emitted (note the stray `done` where `fi` would be expected).
            default -> "if [ -z \"$ES_PATH_CONF\" ]; then ES_PATH_CONF=\"$ES_HOME\"/config; done";
        });
        expansions.put("source.path.env", switch (distributionType) {
            case "deb" -> "source /etc/default/elasticsearch";
            case "rpm" -> "source /etc/sysconfig/elasticsearch";
            default -> "if [ -z \"$ES_PATH_CONF\" ]; then ES_PATH_CONF=\"$ES_HOME\"/config; fi";
        });
        expansions.put("path.logs", isPackage ? PACKAGING_PATH_LOGS : "#path.logs: /path/to/logs");
        // Debian needs exit 0 on these scripts so we add it here and preserve the pretty footer.
        expansions.put("scripts.footer", distributionType.equals("deb") ? "exit 0\n" + footer : footer);
        expansions.put("es.distribution.type", distributionType);
        if (distributionType.equals("deb")) {
            expansions.put("license.name", "Elastic-License");
            // license text needs to be indented with a single space
            expansions.put("license.text", " " + licenseText.replace("\n", "\n "));
        }
        return Collections.unmodifiableMap(expansions);
    }
}
