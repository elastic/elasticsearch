/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.precommit.LicenseAnalyzer.LicenseInfo;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

/**
 * A task to check licenses for dependencies.
 * <p>
 * There are two parts to the check:
 * <ul>
 *   <li>LICENSE, NOTICE and SOURCES files</li>
 *   <li>SHA checksums for each dependency jar</li>
 * </ul>
 * <p>
 * The directory to find the license and sha files in defaults to the dir @{code licenses}
 * in the project directory for this task. You can override this directory:
 * <pre>
 *   dependencyLicenses {
 *     licensesDir = getProject().file("mybetterlicensedir")
 *   }
 * </pre>
 * <p>
 * The jar files to check default to the dependencies from the default configuration. You
 * can override this, for example, to only check compile dependencies:
 * <pre>
 *   dependencyLicenses {
 *     dependencies = getProject().configurations.compile
 *   }
 * </pre>
 * <p>
 * Every jar must have a {@code .sha1} file in the licenses dir. These can be managed
 * automatically using the {@code updateShas} helper task that is created along
 * with this task. It will add {@code .sha1} files for new jars that are in dependencies
 * and remove old {@code .sha1} files that are no longer needed.
 * <p>
 * Every jar must also have a LICENSE and NOTICE file. However, multiple jars can share
 * LICENSE and NOTICE files by mapping a pattern to the same name.
 * <pre>
 *   dependencyLicenses {
 *     mapping from: &#47;lucene-.*&#47;, to: "lucene"
 *   }
 * </pre>
 * Dependencies using licenses with stricter distribution requirements (such as LGPL)
 * require a SOURCES file as well. The file should include a URL to a source distribution
 * for the dependency. This artifact will be redistributed by us with the release to
 * comply with the license terms.
 */
public class DependencyLicensesTask extends DefaultTask {

    private final Pattern regex = Pattern.compile("-v?\\d+.*");

    private final Logger logger = Logging.getLogger(getClass());

    private static final String SHA_EXTENSION = ".sha1";

    // TODO: we should be able to default this to eg compile deps, but we need to move the licenses
    // check from distribution to core (ie this should only be run on java projects)
    /**
     * A collection of jar files that should be checked.
     */
    private FileCollection dependencies;

    /**
     * The directory to find the license and sha files in.
     */
    private final DirectoryProperty licensesDir;

    /**
     * A map of patterns to prefix, used to find the LICENSE and NOTICE file.
     */
    private Map<String, String> mappings = new LinkedHashMap<>();

    /**
     * Names of dependencies whose shas should not exist.
     */
    private Set<String> ignoreShas = new HashSet<>();

    /**
     *  Names of files that should be ignored by the check
     */
    private LinkedHashSet<String> ignoreFiles = new LinkedHashSet<>();
    private ProjectLayout projectLayout;

    /**
     * Add a mapping from a regex pattern for the jar name, to a prefix to find
     * the LICENSE and NOTICE file for that jar.
     */
    public void mapping(Map<String, String> props) {
        String from = props.get("from");
        if (from == null) {
            throw new InvalidUserDataException("Missing \"from\" setting for license name mapping");
        }
        String to = props.get("to");
        if (to == null) {
            throw new InvalidUserDataException("Missing \"to\" setting for license name mapping");
        }
        if (props.size() > 2) {
            throw new InvalidUserDataException("Unknown properties for mapping on dependencyLicenses: " + props.keySet());
        }
        mappings.put(from, to);
    }

    @Inject
    public DependencyLicensesTask(ObjectFactory objects, ProjectLayout projectLayout) {
        this.projectLayout = projectLayout;
        licensesDir = objects.directoryProperty().convention(projectLayout.getProjectDirectory().dir("licenses"));
    }

    @InputFiles
    public FileCollection getDependencies() {
        return dependencies;
    }

    public void setDependencies(FileCollection dependencies) {
        this.dependencies = dependencies;
    }

    @Optional
    @InputDirectory
    public File getLicensesDir() {
        File asFile = licensesDir.get().getAsFile();
        if (asFile.exists()) {
            return asFile;
        }

        return null;
    }

    public void setLicensesDir(File licensesDir) {
        this.licensesDir.set(licensesDir);
    }

    /**
     * Add a rule which will skip SHA checking for the given dependency name. This should be used for
     * locally build dependencies, which cause the sha to change constantly.
     */
    public void ignoreSha(String dep) {
        ignoreShas.add(dep);
    }

    /**
     * Add a file that should be ignored by the check. This should be used for additional license files not tied to jar dependency
     */
    public void ignoreFile(String file) {
        ignoreFiles.add(file);
    }

    @TaskAction
    public void checkDependencies() {
        if (dependencies == null) {
            throw new GradleException("No dependencies variable defined.");
        }
        File licensesDirAsFile = licensesDir.get().getAsFile();
        if (dependencies.isEmpty()) {
            if (licensesDirAsFile.exists()) {
                throw new GradleException("Licenses dir " + licensesDirAsFile + " exists, but there are no dependencies");
            }
            return; // no dependencies to check
        } else if (licensesDirAsFile.exists() == false) {
            String deps = "";
            for (File file : dependencies) {
                deps += file.getName() + "\n";
            }
            throw new GradleException("Licences dir " + licensesDirAsFile + " does not exist, but there are dependencies: " + deps);
        }

        Map<String, Boolean> licenses = new HashMap<>();
        Map<String, Boolean> notices = new HashMap<>();
        Map<String, Boolean> sources = new HashMap<>();
        for (File file : licensesDirAsFile.listFiles()) {
            String name = file.getName();
            if (name.endsWith("-LICENSE") || name.endsWith("-LICENSE.txt")) {
                // TODO: why do we support suffix of LICENSE *and* LICENSE.txt??
                licenses.put(name, false);
            } else if (name.contains("-NOTICE") || name.contains("-NOTICE.txt")) {
                notices.put(name, false);
            } else if (name.contains("-SOURCES") || name.contains("-SOURCES.txt")) {
                sources.put(name, false);
            }
        }

        licenses.keySet().removeAll(ignoreFiles);
        notices.keySet().removeAll(ignoreFiles);
        sources.keySet().removeAll(ignoreFiles);

        checkDependencies(licenses, notices, sources);

        licenses.forEach((item, exists) -> failIfAnyMissing(item, exists, "license"));

        notices.forEach((item, exists) -> failIfAnyMissing(item, exists, "notice"));

        sources.forEach((item, exists) -> failIfAnyMissing(item, exists, "sources"));
    }

    // This is just a marker output folder to allow this task being up-to-date.
    // The check logic is exception driven so a failed tasks will not be defined
    // by this output but when successful we can safely mark the task as up-to-date.
    @OutputDirectory
    public Provider<Directory> getOutputMarker() {
        return projectLayout.getBuildDirectory().dir("dependencyLicense");
    }

    private void failIfAnyMissing(String item, Boolean exists, String type) {
        if (exists == false) {
            throw new GradleException("Unused " + type + " " + item);
        }
    }

    private void checkDependencies(Map<String, Boolean> licenses, Map<String, Boolean> notices, Map<String, Boolean> sources) {
        for (File dependency : dependencies) {
            String jarName = dependency.getName();
            String depName = regex.matcher(jarName).replaceFirst("");
            String dependencyName = getDependencyName(mappings, depName);
            logger.info("mapped dependency name {} to {} for license/notice check", depName, dependencyName);
            checkFile(dependencyName, jarName, licenses, "LICENSE");
            checkFile(dependencyName, jarName, notices, "NOTICE");

            File licenseFile = new File(licensesDir.get().getAsFile(), getFileName(dependencyName, licenses, "LICENSE"));
            LicenseInfo licenseInfo = LicenseAnalyzer.licenseType(licenseFile);
            if (licenseInfo.sourceRedistributionRequired()) {
                checkFile(dependencyName, jarName, sources, "SOURCES");
            }
        }
    }

    public static String getDependencyName(Map<String, String> mappings, String dependencyName) {
        // order is the same for keys and values iteration since we use a linked hashmap
        List<String> mapped = new ArrayList<>(mappings.values());
        Pattern mappingsPattern = Pattern.compile("(" + String.join(")|(", mappings.keySet()) + ")");
        Matcher match = mappingsPattern.matcher(dependencyName);
        if (match.matches()) {
            int i = 0;
            while (i < match.groupCount() && match.group(i + 1) == null) {
                ++i;
            }
            return mapped.get(i);
        }
        return dependencyName;
    }

    private void checkFile(String name, String jarName, Map<String, Boolean> counters, String type) {
        String fileName = getFileName(name, counters, type);

        if (counters.containsKey(fileName) == false) {
            throw new GradleException("Missing " + type + " for " + jarName + ", expected in " + fileName);
        }

        counters.put(fileName, true);
    }

    private String getFileName(String name, Map<String, ?> counters, String type) {
        String fileName = name + "-" + type;

        if (counters.containsKey(fileName) == false) {
            // try the other suffix...TODO: get rid of this, just support ending in .txt
            return fileName + ".txt";
        }

        return fileName;
    }

    @Input
    @Optional
    public LinkedHashSet<String> getIgnoreFiles() {
        return new LinkedHashSet<>(ignoreFiles);
    }

    @Input
    public LinkedHashMap<String, String> getMappings() {
        return new LinkedHashMap<>(mappings);
    }

}
