/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.precommit;

import org.apache.commons.codec.binary.Hex;
import org.elasticsearch.gradle.precommit.LicenseAnalyzer.LicenseInfo;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private File licensesDir = new File(getProject().getProjectDir(), "licenses");

    /**
     * A map of patterns to prefix, used to find the LICENSE and NOTICE file.
     */
    private Map<String, String> mappings = new LinkedHashMap<>();

    /**
     * Names of dependencies whose shas should not exist.
     */
    private Set<String> ignoreShas = new HashSet<>();

    /**
     * Add a mapping from a regex pattern for the jar name, to a prefix to find
     * the LICENSE and NOTICE file for that jar.
     */
    public void mapping(Map<String, String> props) {
        String from = props.remove("from");
        if (from == null) {
            throw new InvalidUserDataException("Missing \"from\" setting for license name mapping");
        }
        String to = props.remove("to");
        if (to == null) {
            throw new InvalidUserDataException("Missing \"to\" setting for license name mapping");
        }
        if (props.isEmpty() == false) {
            throw new InvalidUserDataException("Unknown properties for mapping on dependencyLicenses: " + props.keySet());
        }
        mappings.put(from, to);
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
        if (licensesDir.exists()) {
            return licensesDir;
        }

        return null;
    }

    public void setLicensesDir(File licensesDir) {
        this.licensesDir = licensesDir;
    }

    /**
     * Add a rule which will skip SHA checking for the given dependency name. This should be used for
     * locally build dependencies, which cause the sha to change constantly.
     */
    public void ignoreSha(String dep) {
        ignoreShas.add(dep);
    }

    @TaskAction
    public void checkDependencies() throws IOException, NoSuchAlgorithmException {
        if (dependencies == null) {
            throw new GradleException("No dependencies variable defined.");
        }

        if (dependencies.isEmpty()) {
            if (licensesDir.exists()) {
                throw new GradleException("Licenses dir " + licensesDir + " exists, but there are no dependencies");
            }
            return; // no dependencies to check
        } else if (licensesDir.exists() == false) {
            String deps = "";
            for (File file : dependencies) {
                deps += file.getName() + "\n";
            }
            throw new GradleException("Licences dir " + licensesDir + " does not exist, but there are dependencies: " + deps);
        }

        Map<String, Boolean> licenses = new HashMap<>();
        Map<String, Boolean> notices = new HashMap<>();
        Map<String, Boolean> sources = new HashMap<>();
        Set<File> shaFiles = new HashSet<>();

        for (File file : licensesDir.listFiles()) {
            String name = file.getName();
            if (name.endsWith(SHA_EXTENSION)) {
                shaFiles.add(file);
            } else if (name.endsWith("-LICENSE") || name.endsWith("-LICENSE.txt")) {
                // TODO: why do we support suffix of LICENSE *and* LICENSE.txt??
                licenses.put(name, false);
            } else if (name.contains("-NOTICE") || name.contains("-NOTICE.txt")) {
                notices.put(name, false);
            } else if (name.contains("-SOURCES") || name.contains("-SOURCES.txt")) {
                sources.put(name, false);
            }
        }

        checkDependencies(licenses, notices, sources, shaFiles);

        licenses.forEach((item, exists) -> failIfAnyMissing(item, exists, "license"));

        notices.forEach((item, exists) -> failIfAnyMissing(item, exists, "notice"));

        sources.forEach((item, exists) -> failIfAnyMissing(item, exists, "sources"));

        if (shaFiles.isEmpty() == false) {
            throw new GradleException("Unused sha files found: \n" + joinFilenames(shaFiles));
        }

    }

    // This is just a marker output folder to allow this task being up-to-date.
    // The check logic is exception driven so a failed tasks will not be defined
    // by this output but when successful we can safely mark the task as up-to-date.
    @OutputDirectory
    public File getOutputMarker() {
        return new File(getProject().getBuildDir(), "dependencyLicense");
    }

    private void failIfAnyMissing(String item, Boolean exists, String type) {
        if (exists == false) {
            throw new GradleException("Unused " + type + " " + item);
        }
    }

    private void checkDependencies(
        Map<String, Boolean> licenses,
        Map<String, Boolean> notices,
        Map<String, Boolean> sources,
        Set<File> shaFiles
    ) throws NoSuchAlgorithmException, IOException {
        for (File dependency : dependencies) {
            String jarName = dependency.getName();
            String depName = regex.matcher(jarName).replaceFirst("");

            validateSha(shaFiles, dependency, jarName, depName);

            String dependencyName = getDependencyName(mappings, depName);
            logger.info("mapped dependency name {} to {} for license/notice check", depName, dependencyName);
            checkFile(dependencyName, jarName, licenses, "LICENSE");
            checkFile(dependencyName, jarName, notices, "NOTICE");

            File licenseFile = new File(licensesDir, getFileName(dependencyName, licenses, "LICENSE"));
            LicenseInfo licenseInfo = LicenseAnalyzer.licenseType(licenseFile);
            if (licenseInfo.isSourceRedistributionRequired()) {
                checkFile(dependencyName, jarName, sources, "SOURCES");
            }
        }
    }

    private void validateSha(Set<File> shaFiles, File dependency, String jarName, String depName) throws NoSuchAlgorithmException,
        IOException {
        if (ignoreShas.contains(depName)) {
            // local deps should not have sha files!
            if (getShaFile(jarName).exists()) {
                throw new GradleException("SHA file " + getShaFile(jarName) + " exists for ignored dependency " + depName);
            }
        } else {
            logger.info("Checking sha for {}", jarName);
            checkSha(dependency, jarName, shaFiles);
        }
    }

    private String joinFilenames(Set<File> shaFiles) {
        List<String> names = shaFiles.stream().map(File::getName).collect(Collectors.toList());
        return String.join("\n", names);
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

    private void checkSha(File jar, String jarName, Set<File> shaFiles) throws NoSuchAlgorithmException, IOException {
        File shaFile = getShaFile(jarName);
        if (shaFile.exists() == false) {
            throw new GradleException("Missing SHA for " + jarName + ". Run \"gradle updateSHAs\" to create them");
        }

        // TODO: shouldn't have to trim, sha files should not have trailing newline
        byte[] fileBytes = Files.readAllBytes(shaFile.toPath());
        String expectedSha = new String(fileBytes, StandardCharsets.UTF_8).trim();

        String sha = getSha1(jar);

        if (expectedSha.equals(sha) == false) {
            final String exceptionMessage = String.format(
                Locale.ROOT,
                "SHA has changed! Expected %s for %s but got %s."
                    + "\nThis usually indicates a corrupt dependency cache or artifacts changed upstream."
                    + "\nEither wipe your cache, fix the upstream artifact, or delete %s and run updateShas",
                expectedSha,
                jarName,
                sha,
                shaFile
            );

            throw new GradleException(exceptionMessage);
        }
        shaFiles.remove(shaFile);
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
    public LinkedHashMap<String, String> getMappings() {
        return new LinkedHashMap<>(mappings);
    }

    File getShaFile(String jarName) {
        return new File(licensesDir, jarName + SHA_EXTENSION);
    }

    @Internal
    Set<File> getShaFiles() {
        File[] array = licensesDir.listFiles();
        if (array == null) {
            throw new GradleException("\"" + licensesDir.getPath() + "\" isn't a valid directory");
        }

        return Arrays.stream(array).filter(file -> file.getName().endsWith(SHA_EXTENSION)).collect(Collectors.toSet());
    }

    String getSha1(File file) throws IOException, NoSuchAlgorithmException {
        byte[] bytes = Files.readAllBytes(file.toPath());

        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        char[] encoded = Hex.encodeHex(digest.digest(bytes));
        return String.copyValueOf(encoded);
    }

}
