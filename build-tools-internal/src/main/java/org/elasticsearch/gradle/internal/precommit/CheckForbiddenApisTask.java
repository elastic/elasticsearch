/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import de.thetaphi.forbiddenapis.Checker;
import de.thetaphi.forbiddenapis.Constants;
import de.thetaphi.forbiddenapis.ForbiddenApiException;
import de.thetaphi.forbiddenapis.Logger;
import de.thetaphi.forbiddenapis.ParseException;
import groovy.lang.Closure;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Transformer;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.FileTreeElement;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.VerificationTask;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.RetentionPolicy;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import javax.inject.Inject;

import static de.thetaphi.forbiddenapis.Checker.Option.*;

@CacheableTask
public abstract class CheckForbiddenApisTask extends DefaultTask implements PatternFilterable, VerificationTask, Constants {

    public static final Set<String> BUNDLED_SIGNATURE_DEFAULTS = Set.of("jdk-unsafe", "jdk-non-portable", "jdk-system-out");

    private static final String NL = System.getProperty("line.separator", "\n");
    private final PatternSet patternSet = new PatternSet().include("**/*.class");
    private FileCollection classesDirs;
    private FileCollection classpath;
    private String targetCompatibility;

    private FileCollection signaturesFiles;

    private final ObjectFactory objectFactory;
    private ProjectLayout projectLayout;

    private Set<URL> signaturesURLs = new LinkedHashSet<>();

    private List<String> signatures = new ArrayList<>();

    private File resourcesDir;

    @Input
    @Optional
    abstract SetProperty<String> getBundledSignatures();

    /**
     * List of a custom Java annotations (full class names) that are used in the checked
     * code to suppress errors. Those annotations must have at least
     * {@link RetentionPolicy#CLASS}. They can be applied to classes, their methods,
     * or fields. By default, {@code @de.thetaphi.forbiddenapis.SuppressForbidden}
     * can always be used, but needs the {@code forbidden-apis.jar} file in classpath
     * of compiled project, which may not be wanted.
     * Instead of a full class name, a glob pattern may be used (e.g.,
     * {@code **.SuppressForbidden}).
     */
    @Input
    @Optional
    abstract SetProperty<String> getSuppressAnnotations();

    public boolean failOnUnsupportedJava = false, failOnMissingClasses = true, failOnUnresolvableSignatures = true, ignoreFailures = false,
        ignoreSignaturesOfMissingClasses = false, disableClassloadingCache = true;

    @Inject
    public CheckForbiddenApisTask(ObjectFactory factory, ProjectLayout projectLayout) {
        signaturesFiles = factory.fileCollection();
        this.objectFactory = factory;
        this.projectLayout = projectLayout;
    }

    /**
     * Directories with the class files to check.
     * Defaults to current sourseSet's output directory (Gradle 3) or output directories (Gradle 4.0+).
     */
    @Internal
    public FileCollection getClassesDirs() {
        return classesDirs;
    }

    /** @see #getClassesDirs() */
    public void setClassesDirs(FileCollection classesDirs) {
        Objects.requireNonNull(classesDirs, "classesDirs");
        this.classesDirs = classesDirs;
    }

    /** Returns the pattern set to match against class files in {@link #getClassesDirs()}. */
    @Internal
    public PatternSet getPatternSet() {
        return patternSet;
    }

    /** @see #getPatternSet() */
    public void setPatternSet(PatternSet patternSet) {
        patternSet.copyFrom(patternSet);
    }

    /**
     * A {@link FileCollection} used to configure the classpath.
     * Defaults to current sourseSet's compile classpath.
     */
    @CompileClasspath
    public FileCollection getClasspath() {
        return classpath;
    }

    /** @see #getClasspath */
    public void setClasspath(FileCollection classpath) {
        Objects.requireNonNull(classpath, "classpath");
        this.classpath = classpath;
    }

    /**
     * A {@link FileCollection} containing all files, which contain signatures and comments for forbidden API calls.
     * The signatures are resolved against {@link #getClasspath()}.
     */
    @InputFiles
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getSignaturesFiles() {
        return signaturesFiles;
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getResourcesDir() {
        return resourcesDir;
    }

    public void setResourcesDir(File resourcesDir) {
        this.resourcesDir = resourcesDir;
    }

    /** @see #getSignaturesFiles */
    public void setSignaturesFiles(FileCollection signaturesFiles) {
        this.signaturesFiles = signaturesFiles;
    }

    public void modifyBundledSignatures(Transformer<Set<String>, Set<String>> transformer) {
        getBundledSignatures().set(transformer.transform(getBundledSignatures().get()));
    }

    public void replaceSignatureFiles(String... signatureFiles) {
        List<File> resources = new ArrayList<>(signatureFiles.length);
        for (Object name : signatureFiles) {
            resources.add(new File(resourcesDir, "forbidden/" + name + ".txt"));
        }
        setSignaturesFiles(objectFactory.fileCollection().from(resources));
    }

    public void addSignatureFiles(String... signatureFiles) {
        List<File> resources = new ArrayList<>(signatureFiles.length);
        for (Object name : signatureFiles) {
            resources.add(new File(resourcesDir, "forbidden/" + name + ".txt"));
        }
        setSignaturesFiles(objectFactory.fileCollection().from(getSignaturesFiles()).from(resources));

    }

    /**
     * A list of references to URLs, which contain signatures and comments for forbidden API calls.
     * The signatures are resolved against {@link #getClasspath()}.
     * <p>
     * This property is useful to refer to resources in plugin classpath, e.g., using
     * {@link Class#getResource(String)}. It is not useful for general gradle builds. Especially,
     * don't use it to refer to resources on foreign servers!
     */
    @Input
    @Optional
    public Set<URL> getSignaturesURLs() {
        return signaturesURLs;
    }

    /** @see #getSignaturesURLs */
    public void setSignaturesURLs(Set<URL> signaturesURLs) {
        this.signaturesURLs = signaturesURLs;
    }

    /**
     * Gives multiple API signatures that are joined with newlines and
     * parsed like a single {@link #getSignaturesFiles()}.
     * The signatures are resolved against {@link #getClasspath()}.
     */
    @Input
    @Optional
    public List<String> getSignatures() {
        return signatures;
    }

    /** @see #getSignatures */
    public void setSignatures(List<String> signatures) {
        this.signatures = signatures;
    }

    /**
     * Specifies <a href="bundled-signatures.html">built-in signatures</a> files (e.g., deprecated APIs for specific Java versions,
     * unsafe method calls using default locale, default charset,...)
     */

    /**
     * Fail the build, if the bundled ASM library cannot read the class file format
     * of the runtime library or the runtime library cannot be discovered.
     * Defaults to {@code false}.
     */
    @Input
    public boolean getFailOnUnsupportedJava() {
        return failOnUnsupportedJava;
    }

    /** @see #getFailOnUnsupportedJava */
    public void setFailOnUnsupportedJava(boolean failOnUnsupportedJava) {
        this.failOnUnsupportedJava = failOnUnsupportedJava;
    }

    /**
     * Fail the build, if a class referenced in the scanned code is missing. This requires
     * that you pass the whole classpath including all dependencies to this task
     * (Gradle does this by default).
     * Defaults to {@code true}.
     */
    @Input
    public boolean getFailOnMissingClasses() {
        return failOnMissingClasses;
    }

    /** @see #getFailOnMissingClasses */
    public void setFailOnMissingClasses(boolean failOnMissingClasses) {
        this.failOnMissingClasses = failOnMissingClasses;
    }

    /**
     * Fail the build if a signature is not resolving. If this parameter is set to
     * to false, then such signatures are ignored. Defaults to {@code true}.
     * <p>When disabling this setting, the task still prints a warning to inform the user about
     * broken signatures. This cannot be disabled. There is a second setting
     * {@link #getIgnoreSignaturesOfMissingClasses()} that can be used to silently ignore
     * signatures that refer to methods or field in classes that are not on classpath,
     * e.g. This is useful in multi-module Gradle builds where a common set of signatures is used,
     * that are not part of every sub-modules dependencies.
     * @see #getIgnoreSignaturesOfMissingClasses()
     * @deprecated Use {@link #getIgnoreSignaturesOfMissingClasses()} instead.
     */
    @Input
    @Deprecated
    public boolean getFailOnUnresolvableSignatures() {
        return failOnUnresolvableSignatures;
    }

    /** @see #getFailOnUnresolvableSignatures */
    @Deprecated
    public void setFailOnUnresolvableSignatures(boolean failOnUnresolvableSignatures) {
        this.failOnUnresolvableSignatures = failOnUnresolvableSignatures;
    }

    /**
     * If a class is missing while parsing signatures files, all methods and fields from this
     * class are silently ignored. This is useful in multi-module Gradle
     * projects where only some modules have the dependency to which the signature file(s) apply.
     * This settings prints no warning at all, so verify the signatures at least once with
     * full dependencies.
     * Defaults to {@code false}.
     * @since 3.0
     */
    @Input
    public boolean getIgnoreSignaturesOfMissingClasses() {
        return ignoreSignaturesOfMissingClasses;
    }

    /** @see #getFailOnUnresolvableSignatures */
    public void setIgnoreSignaturesOfMissingClasses(boolean ignoreSignaturesOfMissingClasses) {
        this.ignoreSignaturesOfMissingClasses = ignoreSignaturesOfMissingClasses;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This setting is to conform with {@link VerificationTask} interface.
     * Other ForbiddenApis implementations use another name: {@code failOnViolation}
     * Default is {@code false}.
     */
    @Override
    @Input
    public boolean getIgnoreFailures() {
        return ignoreFailures;
    }

    @Override
    public void setIgnoreFailures(boolean ignoreFailures) {
        this.ignoreFailures = ignoreFailures;
    }

    /**
     * Disable the internal JVM classloading cache when getting bytecode from
     * the classpath. This setting slows down checks, but <em>may</em> work around
     * issues with other plugin, that do not close their class loaders.
     * If you get {@code FileNotFoundException}s related to non-existent JAR entries
     * you can try to work around using this setting.
     * <p>
     * The default is {@code false}, unless the plugin detects that your build is
     * running in the <em>Gradle Daemon</em> (which has this problem), setting the
     * default to {@code true} as a consequence.
     * @since 2.2
     */
    @Input
    public boolean getDisableClassloadingCache() {
        return disableClassloadingCache;
    }

    /** @see #getDisableClassloadingCache */
    public void setDisableClassloadingCache(boolean disableClassloadingCache) {
        this.disableClassloadingCache = disableClassloadingCache;
    }

    /**
     * The default compiler target version used to expand references to bundled JDK signatures.
     * E.g., if you use "jdk-deprecated", it will expand to this version.
     * This setting should be identical to the target version used in the compiler task.
     * Defaults to {@code project.targetCompatibility}.
     */
    @Input
    @Optional
    public String getTargetCompatibility() {
        return targetCompatibility;
    }

    /** @see #getTargetCompatibility */
    public void setTargetCompatibility(String targetCompatibility) {
        this.targetCompatibility = targetCompatibility;
    }

    // PatternFilterable implementation:

    /**
     * {@inheritDoc}
     * <p>
     * Set of patterns matching all class files to be parsed from the classesDirectory.
     * Can be changed to e.g. exclude several files (using excludes).
     * The default is a single include with pattern '**&#47;*.class'
     */
    @Override
    @Internal
    public Set<String> getIncludes() {
        return getPatternSet().getIncludes();
    }

    @Override
    public CheckForbiddenApisTask setIncludes(Iterable<String> includes) {
        getPatternSet().setIncludes(includes);
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Set of patterns matching class files to be excluded from checking.
     */
    @Override
    @Internal
    public Set<String> getExcludes() {
        return getPatternSet().getExcludes();
    }

    @Override
    public CheckForbiddenApisTask setExcludes(Iterable<String> excludes) {
        getPatternSet().setExcludes(excludes);
        return this;
    }

    @Override
    public CheckForbiddenApisTask exclude(String... arg0) {
        getPatternSet().exclude(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask exclude(Iterable<String> arg0) {
        getPatternSet().exclude(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask exclude(Spec<FileTreeElement> arg0) {
        getPatternSet().exclude(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask exclude(@SuppressWarnings("rawtypes") Closure arg0) {
        getPatternSet().exclude(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask include(String... arg0) {
        getPatternSet().include(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask include(Iterable<String> arg0) {
        getPatternSet().include(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask include(Spec<FileTreeElement> arg0) {
        getPatternSet().include(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask include(@SuppressWarnings("rawtypes") Closure arg0) {
        getPatternSet().include(arg0);
        return this;
    }

    /** Returns the classes to check. */
    @InputFiles
    @SkipWhenEmpty
    @IgnoreEmptyDirectories
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileTree getClassFiles() {
        return getClassesDirs().getAsFileTree().matching(getPatternSet());
    }

    /** Executes the forbidden apis task. */
    @TaskAction
    public void checkForbidden() throws ForbiddenApiException, IOException {
        final FileCollection classesDirs = getClassesDirs();
        final FileCollection classpath = getClasspath();
        if (classesDirs == null || classpath == null) {
            throw new InvalidUserDataException("Missing 'classesDirs' or 'classpath' property.");
        }

        final Logger log = new Logger() {
            @Override
            public void error(String msg) {
                getLogger().error(msg);
            }

            @Override
            public void warn(String msg) {
                getLogger().warn(msg);
            }

            @Override
            public void info(String msg) {
                getLogger().info(msg);
            }

            @Override
            public void debug(String msg) {
                getLogger().debug(msg);
            }
        };

        final Set<File> cpElements = new LinkedHashSet<>();
        cpElements.addAll(classpath.getFiles());
        cpElements.addAll(classesDirs.getFiles());
        final URL[] urls = new URL[cpElements.size()];
        final StringBuilder humanClasspath = new StringBuilder();
        try {
            int i = 0;
            for (final File cpElement : cpElements) {
                urls[i++] = cpElement.toURI().toURL();
                if (humanClasspath.length() > 0) {
                    humanClasspath.append(File.pathSeparatorChar);
                }
                humanClasspath.append(cpElement);
            }
            assert i == urls.length;
        } catch (MalformedURLException mfue) {
            throw new InvalidUserDataException("Failed to build classpath URLs.", mfue);
        }
        log.debug("Classpath: " + humanClasspath);

        URLClassLoader urlLoader = null;
        final ClassLoader loader = (urls.length > 0)
            ? (urlLoader = URLClassLoader.newInstance(urls, ClassLoader.getSystemClassLoader()))
            : ClassLoader.getSystemClassLoader();

        try {
            final EnumSet<Checker.Option> options = EnumSet.noneOf(Checker.Option.class);
            if (getFailOnMissingClasses()) options.add(FAIL_ON_MISSING_CLASSES);
            if (!getIgnoreFailures()) options.add(FAIL_ON_VIOLATION);
            if (getFailOnUnresolvableSignatures()) {
                options.add(FAIL_ON_UNRESOLVABLE_SIGNATURES);
            } else {
                log.warn(DEPRECATED_WARN_FAIL_ON_UNRESOLVABLE_SIGNATURES);
            }
            if (getIgnoreSignaturesOfMissingClasses()) options.add(IGNORE_SIGNATURES_OF_MISSING_CLASSES);
            if (getDisableClassloadingCache()) options.add(DISABLE_CLASSLOADING_CACHE);
            final Checker checker = new Checker(log, loader, options);

            if (!checker.isSupportedJDK) {
                final String msg = String.format(
                    Locale.ENGLISH,
                    "Your Java runtime (%s %s) is not supported by the forbiddenapis plugin. Please run the checks with a supported JDK!",
                    System.getProperty("java.runtime.name"),
                    System.getProperty("java.runtime.version")
                );
                if (getFailOnUnsupportedJava()) {
                    throw new GradleException(msg);
                } else {
                    log.warn(msg);
                    return;
                }
            }

            final Set<String> suppressAnnotations = getSuppressAnnotations().get();
            for (String a : suppressAnnotations) {
                checker.addSuppressAnnotation(a);
            }

            try {
                final Set<String> bundledSignatures = getBundledSignatures().get();
                if (bundledSignatures.isEmpty() == false) {
                    final String bundledSigsJavaVersion = getTargetCompatibility();
                    if (bundledSigsJavaVersion == null) {
                        log.warn(
                            "The 'targetCompatibility' project or task property is missing. "
                                + "Trying to read bundled JDK signatures without compiler target. "
                                + "You have to explicitly specify the version in the resource name."
                        );
                    }
                    for (String bs : bundledSignatures) {
                        checker.addBundledSignatures(bs, bundledSigsJavaVersion);
                    }
                }

                final FileCollection signaturesFiles = getSignaturesFiles();
                if (signaturesFiles != null) for (final File f : signaturesFiles) {
                    checker.parseSignaturesFile(f);
                }
                final Set<URL> signaturesURLs = getSignaturesURLs();
                if (signaturesURLs != null) for (final URL url : signaturesURLs) {
                    checker.parseSignaturesFile(url);
                }
                final List<String> signatures = getSignatures();
                if (signatures != null && !signatures.isEmpty()) {
                    final StringBuilder sb = new StringBuilder();
                    for (String line : signatures) {
                        sb.append(line).append(NL);
                    }
                    checker.parseSignaturesString(sb.toString());
                }
            } catch (IOException ioe) {
                throw new GradleException("IO problem while reading files with API signatures.", ioe);
            } catch (ParseException pe) {
                throw new InvalidUserDataException("Parsing signatures failed: " + pe.getMessage(), pe);
            }

            if (checker.hasNoSignatures()) {
                if (checker.noSignaturesFilesParsed()) {
                    throw new InvalidUserDataException(
                        "No signatures were added to task; use properties 'signatures', 'bundledSignatures', 'signaturesURLs', and/or 'signaturesFiles' to define those!"
                    );
                } else {
                    log.info("Skipping execution because no API signatures are available.");
                    return;
                }
            }

            try {
                checker.addClassesToCheck(getClassFiles());
            } catch (IOException ioe) {
                throw new GradleException("Failed to load one of the given class files.", ioe);
            }

            checker.run();
        } finally {
            // Close the classloader to free resources:
            try {
                if (urlLoader != null) urlLoader.close();
            } catch (IOException ioe) {
                log.warn("Cannot close classloader: ".concat(ioe.toString()));
            }
        }
        writeMarker();
    }

    @OutputFile
    public File getSuccessMarker() {
        return new File(projectLayout.getBuildDirectory().getAsFile().get(), "markers/" + this.getName());
    }

    private void writeMarker() throws IOException {
        Files.write(getSuccessMarker().toPath(), new byte[] {}, StandardOpenOption.CREATE);
    }

}
