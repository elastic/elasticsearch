/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.elasticsearch.gradle.transform.UnzipTransform;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.RepositoryHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;

import java.util.Arrays;

public class JdkDownloadPlugin implements Plugin<Project> {

    public static final String VENDOR_ADOPTIUM = "adoptium";
    public static final String VENDOR_OPENJDK = "openjdk";
    public static final String VENDOR_ZULU = "zulu";

    private static final String REPO_NAME_PREFIX = "jdk_repo_";
    private static final String EXTENSION_NAME = "jdks";
    public static final String JDK_TRIMMED_PREFIX = "(jdk-?\\d.*)|(zulu-?\\d.+).jdk";
    public static final String ZULU_LINUX_AARCH_PATTERN = "zulu.*linux_aarch64";

    @Override
    public void apply(Project project) {
        Attribute<Boolean> jdkAttribute = Attribute.of("jdk", Boolean.class);
        project.getDependencies().getAttributesSchema().attribute(jdkAttribute);
        project.getDependencies().getArtifactTypes().maybeCreate(ArtifactTypeDefinition.ZIP_TYPE);
        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.parameters(parameters -> parameters.setTrimmedPrefixPattern(JDK_TRIMMED_PREFIX));
        });

        ArtifactTypeDefinition tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, tarArtifactTypeDefinition.getName())
                .attribute(jdkAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.parameters(parameters -> {
                parameters.setTrimmedPrefixPattern(JDK_TRIMMED_PREFIX);
                parameters.setKeepStructureFor(Arrays.asList(ZULU_LINUX_AARCH_PATTERN));
            });
        });

        NamedDomainObjectContainer<Jdk> jdksContainer = project.container(Jdk.class, name -> {
            Configuration configuration = project.getConfigurations().create("jdk_" + name);
            configuration.setCanBeConsumed(false);
            configuration.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
            configuration.getAttributes().attribute(jdkAttribute, true);
            Jdk jdk = new Jdk(name, configuration, project.getObjects());
            configuration.defaultDependencies(dependencies -> {
                jdk.finalizeValues();
                setupRepository(project, jdk);
                dependencies.add(project.getDependencies().create(dependencyNotation(jdk)));
            });
            return jdk;
        });
        project.getExtensions().add(EXTENSION_NAME, jdksContainer);
    }

    private void setupRepository(Project project, Jdk jdk) {
        RepositoryHandler repositories = project.getRepositories();

        /*
         * Define the appropriate repository for the given JDK vendor and version
         *
         * For Oracle/OpenJDK/Adoptium we define a repository per-version.
         */
        String repoName = REPO_NAME_PREFIX + jdk.getVendor() + "_" + jdk.getVersion();
        String repoUrl;
        String artifactPattern;

        if (jdk.getVendor().equals(VENDOR_ADOPTIUM)) {
            repoUrl = "https://api.adoptium.net/v3/binary/version/";
            if (jdk.getMajor().equals("8")) {
                // legacy pattern for JDK 8
                artifactPattern = "jdk"
                    + jdk.getBaseVersion()
                    + "-"
                    + jdk.getBuild()
                    + "/[module]/[classifier]/jdk/hotspot/normal/adoptium";
            } else {
                // current pattern since JDK 9
                artifactPattern = "jdk-"
                    + jdk.getBaseVersion()
                    + "+"
                    + jdk.getBuild()
                    + "/[module]/[classifier]/jdk/hotspot/normal/adoptium";
            }
        } else if (jdk.getVendor().equals(VENDOR_OPENJDK)) {
            repoUrl = "https://download.oracle.com";
            if (jdk.getHash() != null) {
                // current pattern since 12.0.1
                artifactPattern = "java/GA/jdk"
                    + jdk.getBaseVersion()
                    + "/"
                    + jdk.getHash()
                    + "/"
                    + jdk.getBuild()
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
            } else {
                // simpler legacy pattern from JDK 9 to JDK 12 that we are advocating to Oracle to bring back
                artifactPattern = "java/GA/jdk"
                    + jdk.getMajor()
                    + "/"
                    + jdk.getBuild()
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
            }
        } else if (jdk.getVendor().equals(VENDOR_ZULU)) {
            repoUrl = "https://cdn.azul.com";
            if (jdk.getMajor().equals("8") && isJdkOnMacOsPlatform(jdk) && jdk.getArchitecture().equals("aarch64")) {
                artifactPattern = "zulu/bin/zulu"
                    + jdk.getDistributionVersion()
                    + "-ca-jdk"
                    + jdk.getBaseVersion().replace("u", ".0.")
                    + "-[module]x_[classifier].[ext]";
            } else {
                throw new GradleException("JDK vendor zulu is supported only for JDK8 on MacOS with Apple Silicon.");
            }
        } else {
            throw new GradleException("Unknown JDK vendor [" + jdk.getVendor() + "]");
        }

        // Define the repository if we haven't already
        if (repositories.findByName(repoName) == null) {
            repositories.ivy(repo -> {
                repo.setName(repoName);
                repo.setUrl(repoUrl);
                repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
                repo.patternLayout(layout -> layout.artifact(artifactPattern));
                repo.content(repositoryContentDescriptor -> repositoryContentDescriptor.includeGroup(groupName(jdk)));
            });
        }
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<Jdk> getContainer(Project project) {
        return (NamedDomainObjectContainer<Jdk>) project.getExtensions().getByName(EXTENSION_NAME);
    }

    private static String dependencyNotation(Jdk jdk) {
        String platformDep = isJdkOnMacOsPlatform(jdk) ? (jdk.getVendor().equals(VENDOR_ADOPTIUM) ? "mac" : "macos") : jdk.getPlatform();
        String extension = jdk.getPlatform().equals("windows") ? "zip" : "tar.gz";

        return groupName(jdk) + ":" + platformDep + ":" + jdk.getBaseVersion() + ":" + jdk.getArchitecture() + "@" + extension;
    }

    private static boolean isJdkOnMacOsPlatform(Jdk jdk) {
        return jdk.getPlatform().equals("darwin") || jdk.getPlatform().equals("mac");
    }

    private static String groupName(Jdk jdk) {
        return jdk.getVendor() + "_" + jdk.getMajor();
    }

}
